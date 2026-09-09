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

/// Cache-line aligned container for connection counters (eliminates false sharing).
#[repr(align(64))]
#[derive(Default, Debug)]
struct ConnMetrics {
    active: AtomicUsize,
    total: AtomicU64,
}

/// Cache-line aligned container for room counters (eliminates false sharing).
#[repr(align(64))]
#[derive(Default, Debug)]
struct RoomMetrics {
    active: AtomicUsize,
    total: AtomicU64,
}

/// Cache-line aligned container for outbound traffic counters (eliminates false sharing).
#[repr(align(64))]
#[derive(Default, Debug)]
struct OutboundMetrics {
    messages: AtomicU64,
    bytes: AtomicU64,
    dropped: AtomicU64,
}

/// Cache-line aligned container for inbound traffic counters (eliminates false sharing).
#[repr(align(64))]
#[derive(Default, Debug)]
struct InboundMetrics {
    messages: AtomicU64,
    bytes: AtomicU64,
}

/// Cache-line aligned container for cluster transit counters (eliminates false sharing).
#[repr(align(64))]
#[derive(Default, Debug)]
struct ClusterMetrics {
    published: AtomicU64,
    bytes_published: AtomicU64,
    received: AtomicU64,
    bytes_received: AtomicU64,
    dropped: AtomicU64,
}

/// Atomic in-memory metrics counter implementation with 64-byte CPU cache line isolation.
#[derive(Default, Debug)]
pub struct InMemoryMetrics {
    conns: ConnMetrics,
    rooms: RoomMetrics,
    outbound: OutboundMetrics,
    inbound: InboundMetrics,
    cluster: ClusterMetrics,
}

impl InMemoryMetrics {
    /// Creates a new `InMemoryMetrics` instance.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Current number of active WebSocket connections.
    pub fn active_connections(&self) -> usize {
        self.conns.active.load(Ordering::Relaxed)
    }

    /// Cumulative count of all connected clients.
    pub fn total_connections(&self) -> u64 {
        self.conns.total.load(Ordering::Relaxed)
    }

    /// Current number of active rooms.
    pub fn active_rooms(&self) -> usize {
        self.rooms.active.load(Ordering::Relaxed)
    }

    /// Cumulative count of all created rooms.
    pub fn total_rooms(&self) -> u64 {
        self.rooms.total.load(Ordering::Relaxed)
    }

    /// Cumulative number of messages sent to clients.
    pub fn messages_sent(&self) -> u64 {
        self.outbound.messages.load(Ordering::Relaxed)
    }

    /// Cumulative number of messages received from clients.
    pub fn messages_received(&self) -> u64 {
        self.inbound.messages.load(Ordering::Relaxed)
    }

    /// Cumulative number of messages dropped due to slow clients.
    pub fn messages_dropped(&self) -> u64 {
        self.outbound.dropped.load(Ordering::Relaxed)
    }

    /// Cumulative outbound bytes sent.
    pub fn bytes_sent(&self) -> u64 {
        self.outbound.bytes.load(Ordering::Relaxed)
    }

    /// Cumulative inbound bytes received.
    pub fn bytes_received(&self) -> u64 {
        self.inbound.bytes.load(Ordering::Relaxed)
    }

    /// Cumulative messages published to cluster.
    pub fn cluster_published(&self) -> u64 {
        self.cluster.published.load(Ordering::Relaxed)
    }

    /// Cumulative messages received from cluster.
    pub fn cluster_received(&self) -> u64 {
        self.cluster.received.load(Ordering::Relaxed)
    }
}

impl Metrics for InMemoryMetrics {
    fn on_connect(&self) {
        self.conns.active.fetch_add(1, Ordering::Relaxed);
        self.conns.total.fetch_add(1, Ordering::Relaxed);
    }

    fn on_disconnect(&self) {
        self.conns.active.fetch_sub(1, Ordering::Relaxed);
    }

    fn on_message_sent(&self, bytes: usize) {
        self.outbound.messages.fetch_add(1, Ordering::Relaxed);
        self.outbound
            .bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_message_received(&self, bytes: usize) {
        self.inbound.messages.fetch_add(1, Ordering::Relaxed);
        self.inbound
            .bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_message_dropped(&self) {
        self.outbound.dropped.fetch_add(1, Ordering::Relaxed);
    }

    fn on_room_created(&self, _room: &str) {
        self.rooms.active.fetch_add(1, Ordering::Relaxed);
        self.rooms.total.fetch_add(1, Ordering::Relaxed);
    }

    fn on_room_deleted(&self, _room: &str) {
        self.rooms.active.fetch_sub(1, Ordering::Relaxed);
    }

    fn on_cluster_publish(&self, bytes: usize) {
        self.cluster.published.fetch_add(1, Ordering::Relaxed);
        self.cluster
            .bytes_published
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_cluster_received(&self, bytes: usize) {
        self.cluster.received.fetch_add(1, Ordering::Relaxed);
        self.cluster
            .bytes_received
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn on_cluster_dropped(&self) {
        self.cluster.dropped.fetch_add(1, Ordering::Relaxed);
    }
}
