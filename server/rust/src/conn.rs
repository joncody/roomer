use crate::hub::Hub;
use crate::message::Message;
use crate::metrics::DynMetrics;
use bytes::Bytes;
use dashmap::DashSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::{Notify, mpsc};

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

/// Internal state tracking token-bucket rate limiter for control operations.
#[derive(Debug)]
struct ControlLimiter {
    tokens: f64,
    last_refill: Instant,
    rate: f64,
    burst: f64,
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
    /// Asynchronous notification channel signaling immediate connection teardown.
    pub abort_notify: Arc<Notify>,
    is_aborted: AtomicBool,
    close_sent: AtomicBool,
    control_limiter: Mutex<ControlLimiter>,
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
        Self::with_rate_limit(
            id,
            claims,
            send_tx,
            metrics,
            BackpressureStrategy::default(),
            10.0,
            20.0,
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
        Self::with_rate_limit(id, claims, send_tx, metrics, backpressure, 10.0, 20.0)
    }

    /// Constructs a new `Conn` with custom backpressure and token-bucket rate limiter settings.
    #[must_use]
    pub fn with_rate_limit(
        id: String,
        claims: HashMap<String, String>,
        send_tx: mpsc::Sender<OutboundMessage>,
        metrics: DynMetrics,
        backpressure: BackpressureStrategy,
        rate: f64,
        burst: f64,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            claims,
            send_tx,
            rooms: DashSet::new(),
            metrics,
            backpressure,
            abort_notify: Arc::new(Notify::new()),
            is_aborted: AtomicBool::new(false),
            close_sent: AtomicBool::new(false),
            control_limiter: Mutex::new(ControlLimiter {
                tokens: burst,
                last_refill: Instant::now(),
                rate: if rate > 0.0 { rate } else { 10.0 },
                burst: if burst > 0.0 { burst } else { 20.0 },
            }),
        })
    }

    /// Evaluates token-bucket rate limiter for control-plane requests (join/leave).
    #[must_use]
    pub fn allow_control_event(&self) -> bool {
        if let Ok(mut limiter) = self.control_limiter.lock() {
            let now = Instant::now();
            let elapsed = now.duration_since(limiter.last_refill).as_secs_f64();
            limiter.last_refill = now;
            limiter.tokens = (limiter.tokens + elapsed * limiter.rate).min(limiter.burst);

            if limiter.tokens >= 1.0 {
                limiter.tokens -= 1.0;
                return true;
            }
        }
        false
    }

    /// Non-blocking send of a binary payload according to configured backpressure.
    pub fn try_send(&self, data: Bytes) -> bool {
        if self.is_aborted.load(Ordering::Relaxed) {
            return false;
        }

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
                        // Immediately signal connection teardown with RFC 1008 policy violation
                        if !self.is_aborted.swap(true, Ordering::SeqCst) {
                            self.try_send_close(1008, "Slow client buffer overflow");
                            self.abort_notify.notify_one();
                        }
                        false
                    }
                    BackpressureStrategy::DropOldest | BackpressureStrategy::DropNewest => false,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.is_aborted.store(true, Ordering::Relaxed);
                false
            }
        }
    }

    /// Sends a WebSocket close frame to the connection ensuring only a single close message is dispatched.
    pub fn try_send_close(&self, code: u16, reason: impl Into<String>) -> bool {
        if self.close_sent.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.send_tx
            .try_send(OutboundMessage::Close(code, reason.into()))
            .is_ok()
    }

    /// Sends a WebSocket close frame with status code and reason, then triggers connection termination.
    pub fn close_with(&self, code: u16, reason: impl Into<String>) -> bool {
        let sent = self.try_send_close(code, reason);
        if !sent && !self.is_aborted.swap(true, Ordering::SeqCst) {
            self.abort_notify.notify_one();
        }
        sent
    }

    /// Returns `true` if a close frame has already been scheduled or sent.
    #[must_use]
    pub fn is_close_sent(&self) -> bool {
        self.close_sent.load(Ordering::Relaxed)
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
