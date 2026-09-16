# `roomer` – Rust Server Implementation

[![Crates.io Version](https://img.shields.io/crates/v/roomer.svg)](https://crates.io/crates/roomer)
[![Rust Version](https://img.shields.io/badge/Rust-1.88%2B-DEA584?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Rust Edition](https://img.shields.io/badge/Edition-2024-000000?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

Rust implementation of the Roomer WebSocket framework with Axum 0.8, Tokio, lock-striped `DashMap` concurrency, 64-byte L1/L2 cache line false sharing elimination, true wire-level unicast routing, auto-expiring SET presence, token-bucket control-plane rate limiting, configurable backpressure, explicit RFC 6455 close frame control, zero-copy `bytes::Bytes` 12-byte framing, and Redis cluster scaling.

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

---

## 🏁 Running the Standalone Demo & Test Server

The crate includes an example server binary (`examples/server.rs`) serving the HTML/JS demo frontend, the automated browser test runner, and the WebSocket endpoint.

### Run in Standalone In-Memory Mode
```bash
# Run from repository root:
cargo run --manifest-path server/rust/Cargo.toml --example server

# Or run from inside server/rust:
cargo run --example server
```

### Run with Redis Cluster Adapter
```bash
# Set REDIS_URL and enable the redis-adapter feature:
REDIS_URL=redis://127.0.0.1:6379 cargo run --example server --features redis-adapter
```

Once running, navigate to:
* **Interactive Chat Demo:** [http://localhost:8080/](http://localhost:8080/)
* **Automated Browser Test Suite:** [http://localhost:8080/tests/](http://localhost:8080/tests/)
* **WebSocket Endpoint:** `ws://localhost:8080/ws`

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

## 📚 API Reference

### Core Hub Methods

| Method | Description |
|---|---|
| `Hub::new() -> Arc<Hub>` | Creates a new shared Hub coordinator instance. |
| `hub.configure(adapter, metrics)` | Attaches cluster adapter and telemetry metrics collector. |
| `hub.register_handler(event, handler)` | Registers an asynchronous custom message handler. |
| `hub.broadcast_room(exclude_id, msg)` | Broadcasts message to room members and cluster adapter. |
| `hub.get_cluster_presence(room) -> Vec<String>` | Fetches all member IDs in a room across the cluster using SMEMBERS. |
| `hub.send_direct_to_cluster(msg)` | Dispatches a direct message via isolated node unicast. |
| `hub.disconnect(conn_id, code, reason) -> bool` | Dispatches an RFC 6455 close frame to a connection and initiates teardown. |
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
| `conn.try_send_close(code, reason) -> bool` | Enqueues a WebSocket close frame ensuring only a single close message is sent. |
| `conn.close_with(code, reason) -> bool` | Sends a close frame and initiates immediate connection teardown. |
| `conn.is_in_room(room) -> bool` | Checks if connection is currently in a room. |

---

## 🧪 Testing & Benchmarks

```bash
# Run all unit tests, integration tests, and hub tests
cargo test --all-targets --features redis-adapter
```
