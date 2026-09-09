use crate::hub::Hub;
use crate::message::Message;
use crate::metrics::DynMetrics;
use bytes::Bytes;
use dashmap::DashSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

/// Backpressure policy when outbound connection queue is saturated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressureStrategy {
    /// Closes connection and cleans up state to protect server memory (default).
    #[default]
    DropSlowClient,
    /// Evicts oldest queued frame in buffer to make room for new message.
    DropOldest,
    /// Discards the incoming message while keeping connection and queue intact.
    DropNewest,
}

/// Outbound WebSocket frame variant dispatched to connection writer tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundMessage {
    /// Standard binary frame payload.
    Binary(Bytes),
    /// WebSocket close frame with status code and reason.
    Close(u16, String),
}

/// Represents an individual active WebSocket connection.
pub struct Conn {
    /// Globally unique connection identifier (UUID v4).
    pub id: String,
    /// Authenticated claims extracted during the WebSocket handshake.
    pub claims: HashMap<String, String>,
    /// Outbound message sender channel.
    pub send_tx: mpsc::Sender<OutboundMessage>,
    /// Lock-striped set of active room names joined by this connection.
    pub rooms: DashSet<String>,
    /// Observability metrics reference.
    pub metrics: DynMetrics,
    /// Buffer saturation policy.
    pub backpressure: BackpressureStrategy,
    last_touch_ms: AtomicU64,
}

impl Conn {
    /// Constructs a new `Conn` wrapped in an `Arc`.
    #[must_use]
    pub fn new(
        id: String,
        claims: HashMap<String, String>,
        send_tx: mpsc::Sender<OutboundMessage>,
        metrics: DynMetrics,
    ) -> Arc<Self> {
        Self::with_backpressure(
            id,
            claims,
            send_tx,
            metrics,
            BackpressureStrategy::default(),
        )
    }

    /// Constructs a new `Conn` with an explicit backpressure strategy.
    #[must_use]
    pub fn with_backpressure(
        id: String,
        claims: HashMap<String, String>,
        send_tx: mpsc::Sender<OutboundMessage>,
        metrics: DynMetrics,
        backpressure: BackpressureStrategy,
    ) -> Arc<Self> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Arc::new(Self {
            id,
            claims,
            send_tx,
            rooms: DashSet::new(),
            metrics,
            backpressure,
            last_touch_ms: AtomicU64::new(now_ms),
        })
    }

    /// Non-blocking send of a binary payload according to configured backpressure.
    pub fn try_send(&self, data: Bytes) -> bool {
        let size = data.len();
        match self.send_tx.try_send(OutboundMessage::Binary(data)) {
            Ok(()) => {
                self.metrics.on_message_sent(size);
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.on_message_dropped();
                match self.backpressure {
                    BackpressureStrategy::DropSlowClient => {
                        let _ = self
                            .send_tx
                            .try_send(OutboundMessage::Close(1008, "Slow client dropped".into()));
                        false
                    }
                    BackpressureStrategy::DropOldest | BackpressureStrategy::DropNewest => false,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Sends a WebSocket close frame to the connection.
    pub fn try_send_close(&self, code: u16, reason: impl Into<String>) -> bool {
        self.send_tx
            .try_send(OutboundMessage::Close(code, reason.into()))
            .is_ok()
    }

    /// Broadcasts a message to all members in a given room except this connection.
    pub fn send_to_room(&self, hub: &Hub, room_name: &str, event: &str, payload: impl Into<Bytes>) {
        let msg = Message::new(room_name, event, "", &self.id, payload.into());
        hub.broadcast_room(Some(&self.id), msg);
    }

    /// Sends a targeted direct message to a specific client ID via local delivery or cluster unicast.
    pub fn send_to_client(&self, hub: &Hub, dst_id: &str, event: &str, payload: impl Into<Bytes>) {
        let msg = Message::new("root", event, dst_id, &self.id, payload.into());
        if let Some(dst) = hub.get_conn(dst_id) {
            dst.try_send(msg.encode());
        } else {
            hub.send_direct_to_cluster(msg);
        }
    }

    /// Touches cluster presence timestamp for all joined rooms if the configured interval has elapsed.
    pub fn touch_presence(&self, hub: &Hub, interval: Duration) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let interval_ms = interval.as_millis() as u64;

        let prev = self.last_touch_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(prev) < interval_ms {
            return;
        }

        if self
            .last_touch_ms
            .compare_exchange(prev, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let rooms = self.joined_rooms();
            if !rooms.is_empty() {
                hub.touch_presence(&self.id, rooms);
            }
        }
    }

    /// Tracks membership in a room.
    pub fn track_room(&self, room: &str) {
        self.rooms.insert(room.to_string());
    }

    /// Untracks membership from a room.
    pub fn untrack_room(&self, room: &str) {
        self.rooms.remove(room);
    }

    /// Checks if this connection is actively tracked in a room.
    #[must_use]
    pub fn is_in_room(&self, room: &str) -> bool {
        self.rooms.contains(room)
    }

    /// Returns a copy of all room names this connection is currently in.
    #[must_use]
    pub fn joined_rooms(&self) -> Vec<String> {
        self.rooms.iter().map(|r| r.key().clone()).collect()
    }
}
