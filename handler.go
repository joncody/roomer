package roomer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Authorize is a function that extracts authenticated claims from an HTTP request.
type Authorize func(*http.Request) (map[string]string, error)

// MessageHandler processes a custom event message from a connection.
type MessageHandler func(c *Conn, msg *Message) error

// Config holds configuration options for WebSocket connections and upgrader.
type Config struct {
	Authorize       Authorize
	MaxMessageSize  int64
	WriteWait       time.Duration
	PongWait        time.Duration
	PingPeriod      time.Duration
	ReadBufferSize  int
	WriteBufferSize int
	CheckOrigin     func(r *http.Request) bool
	Logger          *slog.Logger
	Metrics         Metrics
	Adapter         Adapter
}

// Option sets a configuration option for roomer WebSocket handling.
type Option func(*Config)

// DefaultConfig returns a Config with sensible default settings.
func DefaultConfig() Config {
	return Config{
		MaxMessageSize:  16 * 1024 * 1024,
		WriteWait:       10 * time.Second,
		PongWait:        60 * time.Second,
		PingPeriod:      54 * time.Second,
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     func(r *http.Request) bool { return true },
		Logger:          slog.Default(),
		Metrics:         NopMetrics{},
		Adapter:         newLocalAdapter(),
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

var (
	messageHandlersMu sync.RWMutex
	messageHandlers   = make(map[string]MessageHandler)
)

// RegisterHandler registers a custom event handler for a given event name.
func RegisterHandler(event string, handler MessageHandler) error {
	if event == "" {
		return fmt.Errorf("event name cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
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

// Shutdown gracefully terminates all connections and releases hub/adapter resources within deadline context.
func Shutdown(ctx context.Context) error {
	return hub.Shutdown(ctx)
}

// SocketHandler returns an HTTP handler that upgrades to WebSocket and manages the connection lifecycle.
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

	hub.configure(cfg.Adapter, cfg.Metrics, cfg.Logger)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var claims map[string]string
		if cfg.Authorize != nil {
			var err error
			claims, err = cfg.Authorize(r)
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		c := newConnection(w, r, claims, cfg)
		if c == nil {
			return
		}
		hub.addConn(c)
		hub.joinRoom("root", c)

		// Send join_ack for "root" room
		members := []byte("[]")
		if room, ok := hub.getRoom("root"); ok {
			if snap, err := json.Marshal(room.snapshot()); err == nil {
				members = snap
			}
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
