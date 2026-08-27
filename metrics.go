package roomer

// Metrics provides observation hooks for telemetry collectors (e.g. Prometheus, OpenTelemetry).
type Metrics interface {
	OnConnect()
	OnDisconnect()
	OnMessageSent(bytes int)
	OnMessageReceived(bytes int)
	OnMessageDropped()
	OnRoomCreated(room string)
	OnRoomDeleted(room string)
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
