#![deny(unsafe_code)]
#![warn(missing_docs)]
#![doc(html_root_url = "https://docs.rs/roomer/1.0.0")]

//! # `roomer` – High-Performance Room-Based WebSocket Framework
//!
//! `roomer` is an enterprise-grade WebSocket framework written in Rust (server)
//! and JavaScript/TypeScript (client). It provides:
//!
//! - **Room Management:** Atomic dynamic room creation, subscription, and cleanup.
//! - **Binary Protocol:** High-throughput, zero-copy packet framing.
//! - **Horizontal Clustering:** Pluggable multi-node scaling (e.g. Redis pub/sub with loopback suppression).
//! - **Strict Concurrency Safety:** Sharded lock-striped concurrency via `DashMap`.
//! - **Observability:** Metric observation hooks and Tokio `tracing` diagnostics.
//!
//! ## Quick Example
//!
//! ```rust
//! use roomer::{Hub, Message};
//! use bytes::Bytes;
//! use std::sync::Arc;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let hub = Hub::new();
//!
//! // Register custom event handler
//! hub.register_handler("ping", Arc::new(|conn, _msg| {
//!     Box::pin(async move {
//!         let reply = Message::new("util", "pong", "", &conn.id, Bytes::new());
//!         conn.try_send(reply.encode());
//!         Ok(())
//!     })
//! }))?;
//! # Ok(())
//! # }
//! ```

/// Multi-node distributed adapters (Local in-memory, Redis Pub/Sub).
pub mod adapter;
/// Authentication and claim extraction helpers for WebSocket handshakes.
pub mod auth;
/// WebSocket connection representation and thread-safe send handles.
pub mod conn;
/// Strongly-typed error definitions for framing, handlers, auth, and adapters.
pub mod error;
/// Axum WebSocket upgrade routing and read/write task lifecycle loops.
pub mod handler;
/// Central hub for room sharding, routing, and message dispatch.
pub mod hub;
/// Zero-copy binary message framing serialization and deserialization.
pub mod message;
/// Real-time metrics collection and observability hooks.
pub mod metrics;
/// Lock-striped concurrent room member management.
pub mod room;

pub use adapter::{Adapter, DynAdapter, LocalAdapter};
pub use auth::{BearerAuth, QueryAuth, extract_bearer_token, extract_query_param};
pub use conn::{Conn, OutboundMessage};
pub use error::{AdapterError, AuthError, FrameError, HandlerError, RoomerError};
pub use handler::{AppState, AuthorizeFn, ServerConfig, ws_handler};
pub use hub::{Hub, MessageHandler, RESERVED_EVENTS};
pub use message::Message;
pub use metrics::{DynMetrics, InMemoryMetrics, Metrics, NopMetrics};
pub use room::Room;

#[cfg(feature = "redis-adapter")]
pub use adapter::redis::{RedisAdapter, RedisAdapterBuilder};
