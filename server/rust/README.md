# `roomer` – Rust Server Implementation

[![Crates.io Version](https://img.shields.io/crates/v/roomer.svg)](https://crates.io/crates/roomer)
[![Rust Version](https://img.shields.io/badge/Rust-1.88%2B-DEA584?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Rust Edition](https://img.shields.io/badge/Edition-2024-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

Rust implementation of the Roomer WebSocket framework with Axum 0.8, Tokio, lock-striped `DashMap` concurrency, 64-byte L1/L2 cache line false sharing elimination, true wire-level unicast routing, auto-expiring SET presence, token-bucket control-plane rate limiting, configurable backpressure, zero-copy `bytes::Bytes` 12-byte framing, and Redis cluster scaling.

> 📖 **For Wire Protocol specifications and Client API documentation, see the [Root README](../../README.md).**

---

## 📦 Scope & Architecture

The `roomer` Rust crate provides an asynchronous, zero-cost WebSocket hub, connection lifecycles, and horizontal cluster adapters for Axum applications.

```text
               +---------------------------------------------------+
               |            Axum 0.8 WebSocket Handshake           |
               +-------------------------+-------------------------+
                                         |
               +-------------------------v-------------------------+
               |               Hub Coordinator                     |
               |  - DashMap<String, Arc<Conn>> (Connection Shards) |
               |  - DashMap<String, Arc<Room>> (Room Shards)       |
               |  - #[repr(align(64))] False Sharing Elimination   |
               |  - Token-Bucket Control Rate Limiter              |
               +-------------------+-------------------+-----------+
                                   |                   |
                     +-------------v----+        +-----v-------------+
                     | Split WS Reader  |        | Split WS Writer   |
                     | (Zero-Copy Bytes)|        | (Tokio mpsc queue)|
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |        Pluggable Distributed Adapter (Redis)      |
               |  - Auto-Expiring SET Presence (SADD, SREM)        |
               |  - True Wire-Level Unicast (SUBSCRIBE prefix:node)|
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
```

- **Lock-Striped DashMap Concurrency**: Sharded concurrency for high-throughput reads and concurrent room membership updates.
- **False Sharing Elimination**: Atomic metrics and counter structs are tagged with `#[repr(align(64))]`, aligning them to 64-byte L1/L2 CPU cache lines and preventing cross-core cache invalidations.
- **12-Byte Binary Wire Framing**: Packets begin with a 2-byte `[1B Version][1B Flags]` header followed by right-sized Big-Endian length prefixes (Room/Event `uint16`, Dst/Src `uint8`, Payload `uint32`).
- **Token-Bucket Control Protection**: Rate limits `join` and `leave` control-plane actions on `Conn` to protect Redis and server memory from command storms.
- **Auto-Expiring SET Presence**: Redis plain SETs (`SADD`, `SREM`, `SMEMBERS`) with key expiration on `add_presence` eliminate periodic heartbeat touching and automatically prune abandoned rooms on server crash.
- **True Wire-Level Unicast**: Direct node messages route via dedicated `SUBSCRIBE prefix:node:<nodeID>` channels, preventing bystander cluster nodes from receiving direct traffic over the wire.
- **Early Size Guarding**: Max message size validation executes before buffering full frames, preventing malicious memory allocation attacks.
- **Configurable Backpressure**: Choose between `DropSlowClient` (default memory protection), `DropOldest` (circular queue eviction), and `DropNewest`.

---

## 🚀 Installation

Add to `Cargo.toml`:

```toml
[dependencies]
roomer = { version = "1.1.5", features = ["redis-adapter"] }
tokio = { version = "1.43", features = ["full"] }
axum = { version = "0.8.9", features = ["ws"] }
bytes = "1.10"
```

---

## 🧠 Quick Start

```rust
use axum::{routing::get, Router};
use bytes::Bytes;
use roomer::{ws_handler, AppState, BackpressureStrategy, Hub, Message, ServerConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hub = Hub::new();

    // 1. Register custom event handlers
    let hub_chat = hub.clone();
    hub.register_handler(
        "chat",
        Arc::new(move |conn, msg| {
            let hub = hub_chat.clone();
            Box::pin(async move {
                // Broadcast to room members except sender
                hub.broadcast_room(Some(&conn.id), msg);
                Ok(())
            })
        }),
    )?;

    // 2. Configure server state, backpressure, and rate limits
    let state = AppState::new(hub.clone()).with_config(
        ServerConfig::default()
            .with_channel_capacity(8192)
            .with_max_message_size(16 * 1024 * 1024)
            .with_backpressure(BackpressureStrategy::DropSlowClient)
            .with_control_rate_limit(10.0, 20),
    );

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    println!("Server running on http://localhost:8080");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            let _ = hub.shutdown().await;
        })
        .await?;

    Ok(())
}
```

---

## 🌐 Distributed Clustering (Redis Adapter)

The Redis clustering adapter provides **loopback suppression**, **auto-expiring SET presence**, and **wire-level isolated unicast routing**:

```rust
use roomer::{Hub, InMemoryMetrics, RedisAdapter};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hub = Hub::new();
    let metrics = Arc::new(InMemoryMetrics::new());

    // Connect to Redis with auto-expiring SET presence and unicast routing
    let adapter = RedisAdapter::builder("redis://127.0.0.1:6379")
        .prefix("roomer:demo:")
        .publish_timeout(Duration::from_secs(3))
        .presence_ttl(Duration::from_secs(180)) // Prune abandoned presence sets after 3 mins
        .build()?;

    hub.configure(Arc::new(adapter), metrics).await;
    Ok(())
}
```

---

## 📚 API Reference

### Server Configuration (`ServerConfig`)

| Builder Method | Default | Description |
|---|---|---|
| `.with_backpressure(strategy)` | `DropSlowClient` | Buffer saturation strategy: `DropSlowClient`, `DropOldest`, or `DropNewest`. |
| `.with_channel_capacity(cap)` | `8192` | Buffered outbound message queue capacity per connection. |
| `.with_control_rate_limit(rate, burst)` | `10.0, 20` | Token-bucket refill rate (tokens/sec) and burst capacity for join/leave events. |
| `.with_max_message_size(bytes)` | `16 MB` | Maximum allowed WebSocket frame size in bytes. |
| `.with_ping_interval(duration)` | `54s` | Heartbeat ping frame transmission interval. |
| `.with_pong_timeout(duration)` | `60s` | Maximum time allowed before terminating a client due to missing pong. |

### Core Hub Methods

| Method | Description |
|---|---|
| `Hub::new() -> Arc<Hub>` | Creates a new shared Hub coordinator instance. |
| `hub.configure(adapter, metrics)` | Attaches cluster adapter and telemetry metrics collector. |
| `hub.register_handler(event, handler)` | Registers an asynchronous custom message handler. |
| `hub.broadcast_room(exclude_id, msg)` | Broadcasts message to room members and cluster adapter. |
| `hub.get_cluster_presence(room) -> Vec<String>` | Fetches all member IDs in a room across the cluster using SMEMBERS. |
| `hub.send_direct_to_cluster(msg)` | Dispatches a direct message via isolated node unicast. |
| `hub.shutdown()` | Broadcasts `1001 Going Away` close frames to all connections and drains adapters. |

### `Conn` Methods & Fields

| Method / Field | Description |
|---|---|
| `conn.id` | Unique UUID v4 connection identifier string. |
| `conn.claims` | Map of authenticated claims extracted during handshake. |
| `conn.allow_control_event() -> bool` | Evaluates token-bucket rate limiter for control operations. |
| `conn.send_to_room(hub, room, event, payload)` | Broadcasts to room members **except sender** (local + cluster). |
| `conn.send_to_client(hub, dst_id, event, payload)` | Sends direct message to a client ID via isolated node unicast. |
| `conn.try_send(bytes) -> bool` | Non-blocking frame transmission with configured backpressure policy. |
| `conn.try_send_close(code, reason) -> bool` | Sends a WebSocket close frame. |
| `conn.is_in_room(room) -> bool` | Checks if connection is currently in a room. |

---

## 🧪 Testing & Benchmarks

```bash
# Run all unit tests, integration tests, and hub tests
cargo test --all-targets --features redis-adapter

# Run randomized property-based fuzz testing
cargo test --test proptest_message

# Run Criterion micro-benchmarks
cargo bench --features redis-adapter
```
