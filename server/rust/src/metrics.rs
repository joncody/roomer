use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Trait defining telemetry and observability hooks.
pub trait Metrics: Send + Sync + 'static {
    /// Invoked when a new client WebSocket connects.
    fn on_connect(&self) {}
    /// Invoked when a client WebSocket disconnects.
    fn on_disconnect(&self) {}
    /// Invoked when a binary frame is sent to a connection.
    fn on_message_sent(&self, _bytes: usize) {}
    /// Invoked when a binary frame is received from a connection.
    fn on_message_received(&self, _bytes: usize) {}
    /// Invoked when a message is dropped due to backpressure.
    fn on_message_dropped(&self) {}
    /// Invoked when a new room is instantiated.
    fn on_room_created(&self, _room: &str) {}
    /// Invoked when an empty room is cleaned up.
    fn on_room_deleted(&self, _room: &str) {}
    /// Invoked when a message is published to the cluster adapter.
    fn on_cluster_publish(&self, _bytes: usize) {}
    /// Invoked when a message is received from the cluster adapter.
    fn on_cluster_received(&self, _bytes: usize) {}
    /// Invoked when a cluster message is dropped.
    fn on_cluster_dropped(&self) {}
}

/// No-op default metrics implementation.
#[derive(Default, Clone, Debug)]
pub struct NopMetrics;
impl Metrics for NopMetrics {}

/// Dynamic trait object type alias for metrics.
pub type DynMetrics = Arc<dyn Metrics>;

/// Atomic in-memory metrics counter implementation for diagnostics and tests.
#[derive(Default, Debug)]
pub struct InMemoryMetrics {
    active_connections: AtomicUsize,
    total_connections: AtomicU64,
    active_rooms: AtomicUsize,
    total_rooms: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    messages_dropped: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    cluster_published: AtomicU64,
    cluster_received: AtomicU64,
    cluster_bytes_published: AtomicU64,
    cluster_bytes_received: AtomicU64,
}

impl InMemoryMetrics {
    /// Creates a new `InMemoryMetrics` instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current number of active WebSocket connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Cumulative count of all connected clients.
    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    /// Current number of active rooms.
    pub fn active_rooms(&self) -> usize {
        self.active_rooms.load(Ordering::Relaxed)
    }

    /// Cumulative count of all created rooms.
    pub fn total_rooms(&self) -> u64 {
        self.total_rooms.load(Ordering::Relaxed)
    }

    /// Cumulative number of messages sent to clients.
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }

    /// Cumulative number of messages received from clients.
    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    /// Cumulative number of messages dropped due to slow clients.
    pub fn messages_dropped(&self) -> u64 {
        self.messages_dropped.load(Ordering::Relaxed)
    }

    /// Cumulative outbound bytes sent.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    /// Cumulative inbound bytes received.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received.load(Ordering::Relaxed)
    }

    /// Cumulative messages published to cluster.
    pub fn cluster_published(&self) -> u64 {
        self.cluster_published.load(Ordering::Relaxed)
    }

    /// Cumulative messages received from cluster.
    pub fn cluster_received(&self) -> u64 {
        self.cluster_received.load(Ordering::Relaxed)
    }
}

impl Metrics for InMemoryMetrics {
    fn on_connect(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    fn on_disconnect(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn on_message_sent(&self, bytes: usize) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_message_received(&self, bytes: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_message_dropped(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
    }

    fn on_room_created(&self, _room: &str) {
        self.active_rooms.fetch_add(1, Ordering::Relaxed);
        self.total_rooms.fetch_add(1, Ordering::Relaxed);
    }

    fn on_room_deleted(&self, _room: &str) {
        self.active_rooms.fetch_sub(1, Ordering::Relaxed);
    }

    fn on_cluster_publish(&self, bytes: usize) {
        self.cluster_published.fetch_add(1, Ordering::Relaxed);
        self.cluster_bytes_published
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_cluster_received(&self, bytes: usize) {
        self.cluster_received.fetch_add(1, Ordering::Relaxed);
        self.cluster_bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}
