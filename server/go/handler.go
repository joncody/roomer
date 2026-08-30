package roomer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ReservedEvents contains internal protocol event names that cannot be registered as custom handlers.
var ReservedEvents = []string{
	"join",
	"leave",
	"join_ack",
	"leave_ack",
	"new_member",
	"member_left",
	"open",
	"close",
}

// BackpressureStrategy specifies how the server handles outbound buffer saturation.
type BackpressureStrategy int

const (
	// DropSlowClient drops the message and initiates asynchronous connection teardown (default).
	DropSlowClient BackpressureStrategy = iota
	// DropOldest evicts the oldest queued message in the channel buffer to accommodate new frames.
	DropOldest
	// DropNewest discards the incoming frame while preserving the client connection and buffered messages.
	DropNewest
)

// Authorize is a function that extracts authenticated claims from an HTTP request.
type Authorize func(*http.Request) (map[string]string, error)

// MessageHandler processes a custom event message from a connection.
type MessageHandler func(c *Conn, msg *Message) error

// Config holds configuration options for WebSocket connections and upgrader.
type Config struct {
	Hub             *Hub
	Authorize       Authorize
	MaxMessageSize  int64
	WriteWait       time.Duration
	PongWait        time.Duration
	PingPeriod      time.Duration
	ChannelCapacity int
	ReadBufferSize  int
	WriteBufferSize int
	CheckOrigin     func(r *http.Request) bool
	Logger          *slog.Logger
	Metrics         Metrics
	Adapter         Adapter
	Backpressure    BackpressureStrategy
}

// Option sets a configuration option for roomer WebSocket handling.
type Option func(*Config)

// DefaultConfig returns a Config with production-grade default settings.
func DefaultConfig() Config {
	return Config{
		Hub:             defaultHub,
		MaxMessageSize:  16 * 1024 * 1024,
		WriteWait:       10 * time.Second,
		PongWait:        60 * time.Second,
		PingPeriod:      54 * time.Second,
		ChannelCapacity: 2048,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     nil, // gorilla/websocket enforces same-origin by default
		Logger:          slog.Default(),
		Metrics:         NopMetrics{},
		Adapter:         newLocalAdapter(),
		Backpressure:    DropSlowClient,
	}
}

// WithHub sets a specific custom Hub coordinator instance.
func WithHub(h *Hub) Option {
	return func(c *Config) {
		if h != nil {
			c.Hub = h
		}
	}
}

// WithAuthorize sets the authorization function.
func WithAuthorize(auth Authorize) Option {
	return func(c *Config) {
		c.Authorize = auth
	}
}

// WithMaxMessageSize sets the maximum allowed message size from a client in bytes.
func WithMaxMessageSize(size int64) Option {
	return func(c *Config) {
		if size > 0 {
			c.MaxMessageSize = size
		}
	}
}

// WithWriteWait sets the time allowed to write a message to the client.
func WithWriteWait(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.WriteWait = d
		}
	}
}

// WithPongWait sets the maximum time to wait for next pong from client.
func WithPongWait(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.PongWait = d
			c.PingPeriod = d * 9 / 10
		}
	}
}

// WithPingPeriod sets the interval for sending ping control frames.
func WithPingPeriod(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.PingPeriod = d
		}
	}
}

// WithChannelCapacity sets the buffered outbound channel capacity for each connection.
func WithChannelCapacity(capacity int) Option {
	return func(c *Config) {
		if capacity > 0 {
			c.ChannelCapacity = capacity
		}
	}
}

// WithCheckOrigin sets the HTTP CheckOrigin policy for WebSocket upgrade handshakes.
func WithCheckOrigin(check func(r *http.Request) bool) Option {
	return func(c *Config) {
		if check != nil {
			c.CheckOrigin = check
		}
	}
}

// WithBufferSizes configures the read and write buffer sizes for WebSocket upgrading.
func WithBufferSizes(readSize, writeSize int) Option {
	return func(c *Config) {
		if readSize > 0 {
			c.ReadBufferSize = readSize
		}
		if writeSize > 0 {
			c.WriteBufferSize = writeSize
		}
	}
}

// WithLogger sets the structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) {
		if logger != nil {
			c.Logger = logger
		}
	}
}

// WithMetrics sets the telemetry metrics provider.
func WithMetrics(metrics Metrics) Option {
	return func(c *Config) {
		if metrics != nil {
			c.Metrics = metrics
		}
	}
}

// WithAdapter sets the multi-node distributed clustering adapter.
func WithAdapter(adapter Adapter) Option {
	return func(c *Config) {
		if adapter != nil {
			c.Adapter = adapter
		}
	}
}

// WithBackpressureStrategy sets the buffer saturation backpressure policy.
func WithBackpressureStrategy(strategy BackpressureStrategy) Option {
	return func(c *Config) {
		c.Backpressure = strategy
	}
}

var (
	messageHandlersMu sync.RWMutex
	messageHandlers   = make(map[string]MessageHandler)
)

// RegisterHandler registers a custom event handler for a given event name.
// Rejects reserved event names and duplicate registrations.
func RegisterHandler(event string, handler MessageHandler) error {
	if event == "" {
		return fmt.Errorf("event name cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	for _, reserved := range ReservedEvents {
		if event == reserved {
			return fmt.Errorf("cannot register handler for reserved event %q", event)
		}
	}

	messageHandlersMu.Lock()
	defer messageHandlersMu.Unlock()
	if _, exists := messageHandlers[event]; exists {
		return fmt.Errorf("handler for event %q already registered", event)
	}
	messageHandlers[event] = handler
	return nil
}

// getHandler returns the registered handler for an event, if any.
func getHandler(event string) MessageHandler {
	messageHandlersMu.RLock()
	defer messageHandlersMu.RUnlock()
	return messageHandlers[event]
}

// ExtractBearerToken extracts standard Bearer token credentials from the HTTP Authorization header.
func ExtractBearerToken(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing Authorization header")
	}
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return "", fmt.Errorf("invalid Authorization header format")
	}
	return strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer ")), nil
}

// Shutdown gracefully terminates all connections on DefaultHub and releases adapter resources.
func Shutdown(ctx context.Context) error {
	return defaultHub.Shutdown(ctx)
}

// SocketHandler returns an HTTP handler that upgrades to WebSocket with basic authorization.
func SocketHandler(authFn Authorize) http.HandlerFunc {
	return SocketHandlerWithOptions(WithAuthorize(authFn))
}

// SocketHandlerWithOptions returns an HTTP handler configured with functional options.
func SocketHandlerWithOptions(opts ...Option) http.HandlerFunc {
	cfg := DefaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	hub := cfg.Hub
	if hub == nil {
		hub = defaultHub
	}
	hub.Configure(cfg.Adapter, cfg.Metrics, cfg.Logger)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var claims map[string]string
		if cfg.Authorize != nil {
			var err error
			claims, err = cfg.Authorize(r)
			if err != nil {
				if cfg.Logger != nil {
					cfg.Logger.Warn("Unauthorized WebSocket upgrade handshake", "err", err)
				}
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		c := newConnection(w, r, claims, cfg, hub)
		if c == nil {
			return
		}

		hub.addConn(c)
		hub.joinRoom("root", c)

		// Send join_ack for default "root" room using cluster-wide presence snapshot
		members := []byte("[]")
		snap := hub.getClusterPresence("root")
		if snapJSON, err := json.Marshal(snap); err == nil {
			members = snapJSON
		}
		ack := NewMessage("root", "join_ack", "", c.ID, members).Bytes()
		if !c.TrySend(ack) {
			c.cleanup()
			return
		}

		go c.writePump()
		go c.readPump()
	}
}
