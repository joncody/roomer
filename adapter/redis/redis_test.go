package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/joncody/roomer-go"
	goredis "github.com/redis/go-redis/v9"
)

// Mock Redis UniversalClient for standalone unit testing without a live Redis server.
type mockClient struct {
	goredis.UniversalClient
	mu               sync.Mutex
	publishedChannel string
	publishedPayload []byte
}

func (m *mockClient) Publish(ctx context.Context, channel string, message interface{}) *goredis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedChannel = channel
	if b, ok := message.([]byte); ok {
		m.publishedPayload = append([]byte(nil), b...)
	}
	cmd := goredis.NewIntCmd(ctx)
	cmd.SetVal(1)
	return cmd
}

func (m *mockClient) Close() error {
	return nil
}

func TestAdapter_InterfaceCompliance(t *testing.T) {
	var _ roomer.Adapter = (*Adapter)(nil)
}

func TestAdapter_OptionsAndPrefix(t *testing.T) {
	mock := &mockClient{}

	// Test prefix normalization (adds trailing colon)
	a, err := New(mock,
		WithPrefix("custom_prefix"),
		WithNodeID("node_123"),
		WithPublishTimeout(2*time.Second),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}
	defer a.Close()

	if a.prefix != "custom_prefix:" {
		t.Errorf("expected prefix 'custom_prefix:', got %q", a.prefix)
	}
	if a.NodeID() != "node_123" {
		t.Errorf("expected nodeID 'node_123', got %q", a.NodeID())
	}
	if a.publishTimeout != 2*time.Second {
		t.Errorf("expected timeout 2s, got %v", a.publishTimeout)
	}
}

func TestAdapter_Publish(t *testing.T) {
	mock := &mockClient{}
	nodeID := "test_node_xyz"
	a, err := New(mock, WithNodeID(nodeID), WithPrefix("roomer:"))
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}
	defer a.Close()

	msg := roomer.NewMessage("lobby", "chat", "", "user_42", []byte("hello redis"))
	err = a.Publish(context.Background(), "lobby", msg)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if mock.publishedChannel != "roomer:lobby" {
		t.Errorf("expected channel 'roomer:lobby', got %q", mock.publishedChannel)
	}

	payload := mock.publishedPayload
	if len(payload) < 4 {
		t.Fatalf("payload too short: %d bytes", len(payload))
	}

	idLen := int(binary.BigEndian.Uint32(payload[0:4]))
	if idLen != len(nodeID) {
		t.Errorf("expected nodeID length %d, got %d", len(nodeID), idLen)
	}

	parsedNodeID := string(payload[4 : 4+idLen])
	if parsedNodeID != nodeID {
		t.Errorf("expected nodeID %q, got %q", nodeID, parsedNodeID)
	}

	parsedMsg := roomer.BytesToMessage(payload[4+idLen:])
	if parsedMsg == nil {
		t.Fatalf("failed to parse message payload from envelope")
	}
	if !bytes.Equal(parsedMsg.Payload, []byte("hello redis")) {
		t.Errorf("expected payload 'hello redis', got %s", string(parsedMsg.Payload))
	}
}

func TestAdapter_LoopbackSuppression(t *testing.T) {
	nodeSelf := "node_self_111"
	nodeRemote := "node_remote_222"

	a, err := New(&mockClient{}, WithNodeID(nodeSelf), WithPrefix("roomer:"))
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}
	defer a.Close()

	var received []*roomer.Message
	var receivedRooms []string
	handler := func(room string, msg *roomer.Message) {
		received = append(received, msg)
		receivedRooms = append(receivedRooms, room)
	}

	msg := roomer.NewMessage("lobby", "chat", "", "user_1", []byte("test message"))
	rawMsg := msg.Bytes()

	// 1. Packet from SELF -> MUST be suppressed (dropped)
	envSelf := make([]byte, 4+len(nodeSelf)+len(rawMsg))
	binary.BigEndian.PutUint32(envSelf[0:4], uint32(len(nodeSelf)))
	copy(envSelf[4:], []byte(nodeSelf))
	copy(envSelf[4+len(nodeSelf):], rawMsg)

	a.handleIncoming(&goredis.Message{
		Channel: "roomer:lobby",
		Payload: string(envSelf),
	}, handler)

	if len(received) != 0 {
		t.Errorf("expected 0 messages (self-packet should be dropped), got %d", len(received))
	}

	// 2. Packet from REMOTE node -> MUST be received & parsed
	envRemote := make([]byte, 4+len(nodeRemote)+len(rawMsg))
	binary.BigEndian.PutUint32(envRemote[0:4], uint32(len(nodeRemote)))
	copy(envRemote[4:], []byte(nodeRemote))
	copy(envRemote[4+len(nodeRemote):], rawMsg)

	a.handleIncoming(&goredis.Message{
		Channel: "roomer:lobby",
		Payload: string(envRemote),
	}, handler)

	if len(received) != 1 {
		t.Fatalf("expected 1 message from remote node, got %d", len(received))
	}
	if receivedRooms[0] != "lobby" {
		t.Errorf("expected room 'lobby', got %q", receivedRooms[0])
	}
	if !bytes.Equal(received[0].Payload, []byte("test message")) {
		t.Errorf("expected payload 'test message', got %s", string(received[0].Payload))
	}
}

func TestAdapter_MalformedMessages(t *testing.T) {
	a, _ := New(&mockClient{},
		WithNodeID("node_test"),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)
	defer a.Close()

	handler := func(room string, msg *roomer.Message) {
		t.Errorf("handler should not be called for malformed packets")
	}

	// Packet too short
	a.handleIncoming(&goredis.Message{Channel: "roomer:lobby", Payload: "abc"}, handler)

	// NodeID length header exceeds payload length
	invalidLen := []byte{0, 0, 0, 50, 'a', 'b', 'c'}
	a.handleIncoming(&goredis.Message{Channel: "roomer:lobby", Payload: string(invalidLen)}, handler)

	// Valid NodeID header, but invalid roomer message
	validHeaderBadMsg := make([]byte, 4+4+3)
	binary.BigEndian.PutUint32(validHeaderBadMsg[0:4], 4)
	copy(validHeaderBadMsg[4:8], "node")
	copy(validHeaderBadMsg[8:], []byte{1, 2, 3}) // corrupt message
	a.handleIncoming(&goredis.Message{Channel: "roomer:lobby", Payload: string(validHeaderBadMsg)}, handler)
}

func TestAdapter_CloseIdempotency(t *testing.T) {
	a, err := New(&mockClient{})
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	// Calling Close multiple times must be safe and not panic
	if err := a.Close(); err != nil {
		t.Errorf("first close failed: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Errorf("second close failed: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

func BenchmarkAdapter_Publish(b *testing.B) {
	mock := &mockClient{}
	a, _ := New(mock, WithNodeID("bench_node"))
	msg := roomer.NewMessage("lobby", "chat", "", "bench_src", []byte("benchmark payload bytes"))
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = a.Publish(ctx, "lobby", msg)
	}
}

func BenchmarkAdapter_HandleIncoming_Remote(b *testing.B) {
	a, _ := New(&mockClient{}, WithNodeID("self_node"))
	msg := roomer.NewMessage("lobby", "chat", "", "remote_src", []byte("benchmark payload bytes"))
	rawMsg := msg.Bytes()

	nodeRemote := "remote_node"
	envRemote := make([]byte, 4+len(nodeRemote)+len(rawMsg))
	binary.BigEndian.PutUint32(envRemote[0:4], uint32(len(nodeRemote)))
	copy(envRemote[4:], []byte(nodeRemote))
	copy(envRemote[4+len(nodeRemote):], rawMsg)

	rMsg := &goredis.Message{
		Channel: "roomer:lobby",
		Payload: string(envRemote),
	}

	handler := func(room string, msg *roomer.Message) {}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		a.handleIncoming(rMsg, handler)
	}
}

func BenchmarkAdapter_HandleIncoming_SelfSuppressed(b *testing.B) {
	selfNode := "self_node"
	a, _ := New(&mockClient{}, WithNodeID(selfNode))
	msg := roomer.NewMessage("lobby", "chat", "", "self_src", []byte("benchmark payload bytes"))
	rawMsg := msg.Bytes()

	envSelf := make([]byte, 4+len(selfNode)+len(rawMsg))
	binary.BigEndian.PutUint32(envSelf[0:4], uint32(len(selfNode)))
	copy(envSelf[4:], []byte(selfNode))
	copy(envSelf[4+len(selfNode):], rawMsg)

	rMsg := &goredis.Message{
		Channel: "roomer:lobby",
		Payload: string(envSelf),
	}

	handler := func(room string, msg *roomer.Message) {}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		a.handleIncoming(rMsg, handler)
	}
}
