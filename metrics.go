package roomer

import (
	"sync/atomic"
)

// Metrics provides observation hooks for telemetry collectors (e.g. Prometheus, OpenTelemetry).
type Metrics interface {
	OnConnect()
	OnDisconnect()
	OnMessageSent(bytes int)
	OnMessageReceived(bytes int)
	OnMessageDropped()
	OnRoomCreated(room string)
	OnRoomDeleted(room string)
	OnClusterPublish(bytes int)
	OnClusterReceived(bytes int)
	OnClusterDropped()
}

// NopMetrics is the default no-op metrics implementation.
type NopMetrics struct{}

func (NopMetrics) OnConnect()                  {}
func (NopMetrics) OnDisconnect()               {}
func (NopMetrics) OnMessageSent(bytes int)     {}
func (NopMetrics) OnMessageReceived(bytes int) {}
func (NopMetrics) OnMessageDropped()           {}
func (NopMetrics) OnRoomCreated(room string)   {}
func (NopMetrics) OnRoomDeleted(room string)   {}
func (NopMetrics) OnClusterPublish(bytes int)  {}
func (NopMetrics) OnClusterReceived(bytes int) {}
func (NopMetrics) OnClusterDropped()           {}

// InMemoryMetrics tracks atomic counters for testing, debugging, and stats dashboards.
type InMemoryMetrics struct {
	activeConnections int64
	totalConnections  int64
	activeRooms       int64
	totalRooms        int64
	messagesSent      int64
	messagesReceived  int64
	messagesDropped   int64
	bytesSent         int64
	bytesReceived     int64
	clusterPublished  int64
	clusterReceived   int64
	clusterDropped    int64
}

// NewInMemoryMetrics initializes an in-memory atomic metrics tracker.
func NewInMemoryMetrics() *InMemoryMetrics {
	return &InMemoryMetrics{}
}

func (m *InMemoryMetrics) OnConnect() {
	atomic.AddInt64(&m.activeConnections, 1)
	atomic.AddInt64(&m.totalConnections, 1)
}

func (m *InMemoryMetrics) OnDisconnect() {
	atomic.AddInt64(&m.activeConnections, -1)
}

func (m *InMemoryMetrics) OnMessageSent(bytes int) {
	atomic.AddInt64(&m.messagesSent, 1)
	atomic.AddInt64(&m.bytesSent, int64(bytes))
}

func (m *InMemoryMetrics) OnMessageReceived(bytes int) {
	atomic.AddInt64(&m.messagesReceived, 1)
	atomic.AddInt64(&m.bytesReceived, int64(bytes))
}

func (m *InMemoryMetrics) OnMessageDropped() {
	atomic.AddInt64(&m.messagesDropped, 1)
}

func (m *InMemoryMetrics) OnRoomCreated(room string) {
	atomic.AddInt64(&m.activeRooms, 1)
	atomic.AddInt64(&m.totalRooms, 1)
}

func (m *InMemoryMetrics) OnRoomDeleted(room string) {
	atomic.AddInt64(&m.activeRooms, -1)
}

func (m *InMemoryMetrics) OnClusterPublish(bytes int) {
	atomic.AddInt64(&m.clusterPublished, 1)
}

func (m *InMemoryMetrics) OnClusterReceived(bytes int) {
	atomic.AddInt64(&m.clusterReceived, 1)
}

func (m *InMemoryMetrics) OnClusterDropped() {
	atomic.AddInt64(&m.clusterDropped, 1)
}

// Stats Accessors
func (m *InMemoryMetrics) ActiveConnections() int64 { return atomic.LoadInt64(&m.activeConnections) }
func (m *InMemoryMetrics) TotalConnections() int64  { return atomic.LoadInt64(&m.totalConnections) }
func (m *InMemoryMetrics) ActiveRooms() int64       { return atomic.LoadInt64(&m.activeRooms) }
func (m *InMemoryMetrics) TotalRooms() int64        { return atomic.LoadInt64(&m.totalRooms) }
func (m *InMemoryMetrics) MessagesSent() int64      { return atomic.LoadInt64(&m.messagesSent) }
func (m *InMemoryMetrics) MessagesReceived() int64  { return atomic.LoadInt64(&m.messagesReceived) }
func (m *InMemoryMetrics) MessagesDropped() int64   { return atomic.LoadInt64(&m.messagesDropped) }
func (m *InMemoryMetrics) BytesSent() int64         { return atomic.LoadInt64(&m.bytesSent) }
func (m *InMemoryMetrics) BytesReceived() int64     { return atomic.LoadInt64(&m.bytesReceived) }
