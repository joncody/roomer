# `roomer` – Rust Server Implementation

[![Crates.io Version](https://img.shields.io/crates/v/roomer.svg)](https://crates.io/crates/roomer)
[![Rust Version](https://img.shields.io/badge/Rust-1.88%2B-DEA584?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Rust Edition](https://img.shields.io/badge/Edition-2024-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

Rust implementation of the Roomer WebSocket framework with Axum 0.8, Tokio, lock-striped `DashMap` concurrency, zero-copy `bytes::Bytes` framing, and Redis cluster scaling.

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
               +-------------------+-------------------+-----------+
                                   |                   |
                     +-------------v----+        +-----v-------------+
                     | Split WS Reader  |        | Split WS Writer   |
                     | (Zero-Copy Bytes)|        | (Tokio mpsc queue)|
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |        Pluggable Distributed Adapter (Redis)      |
               |  - Presence Sets (SADD/SREM/SMEMBERS)             |
               |  - Targeted Unicast Routing (PublishDirect)       |
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
```

- **Zero-Copy Memory Model**: Packet payloads are held in `bytes::Bytes`. Broadcasting to 1,000 clients clones atomic pointer references with **zero byte copying**.
- **Lock-Striped DashMap Concurrency**: Sharded concurrency for high-throughput reads and concurrent room membership updates.
- **Configurable Backpressure**: Choose between `DropSlowClient` (default), `DropOldest`, and `DropNewest`.

---

## 🚀 Installation

Add to `Cargo.toml`:

```toml
[dependencies]
roomer = { version = "0.1.0", features = ["redis-adapter"] }
tokio = { version = "1.43", features = ["full"] }
axum = { version = "0.8.9", features = ["ws"] }
bytes = "1.10"
```

---

## 🧠 Quick Start

```rust
use axum::{routing::get, Router};
use bytes::Bytes;
use roomer::{ws_handler, AppState, Hub, Message, ServerConfig};
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

    // 2. Configure server state and channel capacities
    let state = AppState::new(hub.clone()).with_config(
        ServerConfig::default()
            .with_channel_capacity(2048)
            .with_max_message_size(16 * 1024 * 1024),
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

```rust
use roomer::{Hub, InMemoryMetrics, RedisAdapter};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hub = Hub::new();
    let metrics = Arc::new(InMemoryMetrics::new());

    // Connect to Redis with automatic presence synchronization and unicast routing
    let adapter = RedisAdapter::builder("redis://127.0.0.1:6379")
        .prefix("roomer:demo:")
        .publish_timeout(Duration::from_secs(3))
        .build()?;

    hub.configure(Arc::new(adapter), metrics).await;
    Ok(())
}
```

---

## 📚 API Reference

### Core Hub Methods
| Method | Description |
|---|---|
| `Hub::new() -> Arc<Hub>` | Creates a new shared Hub instance. |
| `hub.configure(adapter, metrics)` | Attaches cluster adapter and telemetry metrics collector. |
| `hub.register_handler(event, handler)`| Registers an asynchronous custom message handler. |
| `hub.broadcast_room(exclude_id, msg)` | Broadcasts message to room members and cluster adapter. |
| `hub.get_cluster_presence(room) -> Vec<String>` | Fetches all member IDs in a room across the cluster. |
| `hub.send_direct_to_cluster(msg)` | Dispatches a direct message via $O(1)$ node unicast. |
| `hub.shutdown()` | Broadcasts `1001 Going Away` close frames to all connections and drains adapters. |

### `Conn` Methods & Fields
| Method / Field | Description |
|---|---|
| `conn.id` | Unique UUID v4 connection identifier string. |
| `conn.claims` | Map of authenticated claims extracted during handshake. |
| `conn.send_to_room(hub, room, event, payload)` | Broadcasts to room members **except sender**. |
| `conn.send_to_client(hub, dst_id, event, payload)`| Sends direct message to a client ID (local or cluster unicast). |
| `conn.try_send(bytes) -> bool` | Non-blocking frame transmission with backpressure policy. |
| `conn.try_send_close(code, reason) -> bool` | Sends a WebSocket close frame. |

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
