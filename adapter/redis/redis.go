package redis

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/joncody/roomer"
	goredis "github.com/redis/go-redis/v9"
)

// Ensure Adapter implements roomer.Adapter at compile time.
var _ roomer.Adapter = (*Adapter)(nil)

// Adapter implements roomer.Adapter using Redis Pub/Sub with loopback suppression.
type Adapter struct {
	client         goredis.UniversalClient
	nodeID         string
	nodeIDBytes    []byte
	prefix         string
	logger         *slog.Logger
	publishTimeout time.Duration
	ownsClient     bool

	subMu     sync.Mutex
	pubsub    *goredis.PubSub
	subCtx    context.Context
	subCancel context.CancelFunc
	subWg     sync.WaitGroup
	closeOnce sync.Once
	isClosed  bool
}

// Option configures the Redis adapter.
type Option func(*Adapter)

// WithPrefix sets a custom channel prefix in Redis (default: "roomer:").
func WithPrefix(prefix string) Option {
	return func(a *Adapter) {
		if prefix != "" {
			if !strings.HasSuffix(prefix, ":") {
				prefix += ":"
			}
			a.prefix = prefix
		}
	}
}

// WithNodeID sets an explicit cluster node ID (default: auto-generated UUID).
func WithNodeID(nodeID string) Option {
	return func(a *Adapter) {
		if nodeID != "" {
			a.nodeID = nodeID
			a.nodeIDBytes = []byte(nodeID)
		}
	}
}

// WithLogger sets the structured logger.
func WithLogger(logger *slog.Logger) Option {
	return func(a *Adapter) {
		if logger != nil {
			a.logger = logger
		}
	}
}

// WithPublishTimeout sets the timeout for outbound PUBLISH operations.
func WithPublishTimeout(d time.Duration) Option {
	return func(a *Adapter) {
		if d > 0 {
			a.publishTimeout = d
		}
	}
}

// New creates a new Redis clustering adapter from any go-redis UniversalClient
// (*goredis.Client, *goredis.ClusterClient, or *goredis.Ring).
func New(client goredis.UniversalClient, opts ...Option) (*Adapter, error) {
	if client == nil {
		return nil, errors.New("redis client cannot be nil")
	}

	nodeUUID := uuid.NewString()
	ctx, cancel := context.WithCancel(context.Background())

	a := &Adapter{
		client:         client,
		nodeID:         nodeUUID,
		nodeIDBytes:    []byte(nodeUUID),
		prefix:         "roomer:",
		logger:         slog.Default(),
		publishTimeout: 5 * time.Second,
		subCtx:         ctx,
		subCancel:      cancel,
		ownsClient:     false,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(a)
		}
	}

	return a, nil
}

// NewFromURL connects to Redis using a connection string (e.g. "redis://localhost:6379/0").
func NewFromURL(redisURL string, opts ...Option) (*Adapter, error) {
	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}

	client := goredis.NewClient(opt)
	adapter, err := New(client, opts...)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	adapter.ownsClient = true
	return adapter, nil
}

// NodeID returns the unique instance identifier for this cluster node.
func (a *Adapter) NodeID() string {
	return a.nodeID
}

// Publish broadcasts an enveloped binary message to the Redis channel for the room.
func (a *Adapter) Publish(ctx context.Context, room string, msg *roomer.Message) error {
	if msg == nil {
		return errors.New("cannot publish nil message")
	}

	channel := a.prefix + room
	rawMsg := msg.Bytes()

	// Binary Envelope: [4-byte NodeID length][NodeID bytes][Raw Message bytes]
	envelope := make([]byte, 4+len(a.nodeIDBytes)+len(rawMsg))
	binary.BigEndian.PutUint32(envelope[0:4], uint32(len(a.nodeIDBytes)))
	copy(envelope[4:], a.nodeIDBytes)
	copy(envelope[4+len(a.nodeIDBytes):], rawMsg)

	pubCtx := ctx
	if pubCtx == nil {
		pubCtx = context.Background()
	}
	if _, hasDeadline := pubCtx.Deadline(); !hasDeadline && a.publishTimeout > 0 {
		var cancel context.CancelFunc
		pubCtx, cancel = context.WithTimeout(pubCtx, a.publishTimeout)
		defer cancel()
	}

	return a.client.Publish(pubCtx, channel, envelope).Err()
}

// Subscribe listens to all room channels matching the prefix (e.g. "roomer:*")
// and dispatches incoming messages to local room connections.
func (a *Adapter) Subscribe(handler func(room string, msg *roomer.Message)) error {
	if handler == nil {
		return errors.New("subscriber handler cannot be nil")
	}

	a.subMu.Lock()
	defer a.subMu.Unlock()

	if a.isClosed {
		return errors.New("adapter is closed")
	}
	if a.pubsub != nil {
		return errors.New("subscribe can only be called once")
	}

	pattern := a.prefix + "*"
	pubsub := a.client.PSubscribe(a.subCtx, pattern)

	// Wait for subscription confirmation with a 5s timeout
	initCtx, cancel := context.WithTimeout(a.subCtx, 5*time.Second)
	defer cancel()

	if _, err := pubsub.Receive(initCtx); err != nil {
		_ = pubsub.Close()
		return fmt.Errorf("failed to register redis psubscribe: %w", err)
	}

	a.pubsub = pubsub
	a.subWg.Add(1)

	go a.listenLoop(pubsub, handler)
	return nil
}

func (a *Adapter) listenLoop(pubsub *goredis.PubSub, handler func(room string, msg *roomer.Message)) {
	defer a.subWg.Done()
	ch := pubsub.Channel()

	for {
		select {
		case <-a.subCtx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			a.handleIncoming(m, handler)
		}
	}
}

func (a *Adapter) handleIncoming(m *goredis.Message, handler func(room string, msg *roomer.Message)) {
	payload := []byte(m.Payload)
	if len(payload) < 4 {
		return
	}

	nodeIDLen := int(binary.BigEndian.Uint32(payload[0:4]))
	if nodeIDLen <= 0 || len(payload) < 4+nodeIDLen {
		return
	}

	senderNodeID := string(payload[4 : 4+nodeIDLen])

	// Loopback Prevention: Drop messages originating from our own node instance
	if senderNodeID == a.nodeID {
		return
	}

	// Decode the actual roomer packet
	packet := roomer.BytesToMessage(payload[4+nodeIDLen:])
	if packet == nil {
		if a.logger != nil {
			a.logger.Warn("Redis adapter received malformed message", "channel", m.Channel)
		}
		return
	}

	roomName := strings.TrimPrefix(m.Channel, a.prefix)
	handler(roomName, packet)
}

// Close unsubscribes from channels and releases resources cleanly.
func (a *Adapter) Close() error {
	var err error
	a.closeOnce.Do(func() {
		a.subMu.Lock()
		a.isClosed = true
		if a.subCancel != nil {
			a.subCancel()
		}
		if a.pubsub != nil {
			_ = a.pubsub.Close()
		}
		a.subMu.Unlock()

		// Wait for subscriber loop to finish
		a.subWg.Wait()

		if a.ownsClient && a.client != nil {
			err = a.client.Close()
		}
	})
	return err
}
