package roomer

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// 1. Message Encoding & Framing Tests
// -----------------------------------------------------------------------------

func TestMessage_Roundtrip(t *testing.T) {
	original := NewMessage("lobby", "chat", "user_dst", "user_src", []byte("hello roomer!"))
	raw := original.Bytes()

	decoded := BytesToMessage(raw)
	if decoded == nil {
		t.Fatalf("expected message to decode successfully, got nil")
	}

	if decoded.Room != original.Room {
		t.Errorf("expected Room %q, got %q", original.Room, decoded.Room)
	}
	if decoded.Event != original.Event {
		t.Errorf("expected Event %q, got %q", original.Event, decoded.Event)
	}
	if decoded.Dst != original.Dst {
		t.Errorf("expected Dst %q, got %q", original.Dst, decoded.Dst)
	}
	if decoded.Src != original.Src {
		t.Errorf("expected Src %q, got %q", original.Src, decoded.Src)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("expected Payload %q, got %q", original.Payload, decoded.Payload)
	}
	if decoded.PayloadString() != "hello roomer!" {
		t.Errorf("expected PayloadString 'hello roomer!', got %q", decoded.PayloadString())
	}
}

func TestMessage_JSONHelpers(t *testing.T) {
	type userPayload struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	input := userPayload{Name: "Alice", Age: 30}
	msg, err := NewJSONMessage("lobby", "user_join", "", "system", input)
	if err != nil {
		t.Fatalf("failed to create JSON message: %v", err)
	}

	var output userPayload
	if err := msg.PayloadJSON(&output); err != nil {
		t.Fatalf("failed to parse JSON payload: %v", err)
	}

	if output != input {
		t.Errorf("expected output %+v, got %+v", input, output)
	}
}

func TestMessage_MalformedInput(t *testing.T) {
	if msg := BytesToMessage([]byte{1, 2, 3}); msg != nil {
		t.Errorf("expected nil for short payload, got %+v", msg)
	}

	corrupted := []byte{
		0, 0, 0, 255,
		'a', 'b', 'c', 'd',
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
		0, 0, 0, 0,
	}
	if msg := BytesToMessage(corrupted); msg != nil {
		t.Errorf("expected nil for length out of bounds, got %+v", msg)
	}
}

// -----------------------------------------------------------------------------
// 2. Handler Registration & Invariant Guard Tests
// -----------------------------------------------------------------------------

func TestRegisterHandler_ReservedAndDuplicateGuards(t *testing.T) {
	// 1. Reserved event registration must fail
	err := RegisterHandler("join", func(c *Conn, msg *Message) error { return nil })
	if err == nil {
		t.Errorf("expected error registering reserved event 'join', got nil")
	}

	// 2. Custom event succeeds
	err = RegisterHandler("custom_test_evt", func(c *Conn, msg *Message) error { return nil })
	if err != nil {
		t.Fatalf("expected custom event registration to succeed, got %v", err)
	}

	// 3. Duplicate event registration must fail
	err = RegisterHandler("custom_test_evt", func(c *Conn, msg *Message) error { return nil })
	if err == nil {
		t.Errorf("expected error registering duplicate event, got nil")
	}
}

// -----------------------------------------------------------------------------
// 3. Backpressure Policy Tests
// -----------------------------------------------------------------------------

func TestConn_BackpressureStrategies(t *testing.T) {
	// 1. DropOldest strategy
	cfgOldest := DefaultConfig()
	cfgOldest.Backpressure = DropOldest
	cfgOldest.ChannelCapacity = 2

	cOldest := &Conn{
		ID:     "conn_oldest",
		send:   make(chan []byte, 2),
		done:   make(chan struct{}),
		config: cfgOldest,
	}

	cOldest.TrySend([]byte("msg_1"))
	cOldest.TrySend([]byte("msg_2"))
	// Queue is full; sending msg_3 should evict msg_1
	cOldest.TrySend([]byte("msg_3"))

	firstOut := <-cOldest.send
	if string(firstOut) != "msg_2" {
		t.Errorf("expected msg_2 after oldest eviction, got %s", string(firstOut))
	}
	secondOut := <-cOldest.send
	if string(secondOut) != "msg_3" {
		t.Errorf("expected msg_3, got %s", string(secondOut))
	}

	// 2. DropNewest strategy
	cfgNewest := DefaultConfig()
	cfgNewest.Backpressure = DropNewest
	cfgNewest.ChannelCapacity = 2

	cNewest := &Conn{
		ID:     "conn_newest",
		send:   make(chan []byte, 2),
		done:   make(chan struct{}),
		config: cfgNewest,
	}

	cNewest.TrySend([]byte("msg_A"))
	cNewest.TrySend([]byte("msg_B"))
	// Queue is full; sending msg_C should be dropped while preserving msg_A and msg_B
	success := cNewest.TrySend([]byte("msg_C"))
	if success {
		t.Errorf("expected TrySend to return false for dropped newest message")
	}

	if string(<-cNewest.send) != "msg_A" {
		t.Errorf("expected msg_A to remain in queue")
	}
	if string(<-cNewest.send) != "msg_B" {
		t.Errorf("expected msg_B to remain in queue")
	}
}

// -----------------------------------------------------------------------------
// 4. Concurrency, Race Condition & Metrics Tests
// -----------------------------------------------------------------------------

func TestHub_ConcurrentShardedAccessAndMetrics(t *testing.T) {
	metrics := NewInMemoryMetrics()
	h := NewHub()
	h.Configure(newLocalAdapter(), metrics, nil)

	var wg sync.WaitGroup
	workers := 20
	connsPerWorker := 25

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < connsPerWorker; i++ {
				connID := fmt.Sprintf("conn_%d_%d", workerID, i)
				roomName := fmt.Sprintf("room_%d", (workerID+i)%5)

				c := &Conn{
					ID:     connID,
					hub:    h,
					send:   make(chan []byte, 256),
					done:   make(chan struct{}),
					rooms:  make(map[string]struct{}),
					config: DefaultConfig(),
				}

				// Consumer goroutine
				go func(conn *Conn) {
					for {
						select {
						case <-conn.done:
							return
						case <-conn.send:
						}
					}
				}(c)

				h.addConn(c)
				h.joinRoom(roomName, c)

				if _, ok := h.getConn(connID); !ok {
					t.Errorf("failed to retrieve conn %s", connID)
				}
				if _, ok := h.getRoom(roomName); !ok {
					t.Errorf("failed to retrieve room %s", roomName)
				}

				h.leaveRoom(roomName, c)
				h.removeConn(connID)
				c.cleanup()
			}
		}(w)
	}

	wg.Wait()

	if metrics.TotalConnections() != int64(workers*connsPerWorker) {
		t.Errorf("expected %d total connections, got %d", workers*connsPerWorker, metrics.TotalConnections())
	}
	if metrics.ActiveConnections() != 0 {
		t.Errorf("expected 0 active connections after cleanup, got %d", metrics.ActiveConnections())
	}
}

func TestHub_GracefulShutdown(t *testing.T) {
	h := NewHub()
	c := &Conn{
		ID:     "shutdown_test_conn",
		hub:    h,
		send:   make(chan []byte, 10),
		done:   make(chan struct{}),
		rooms:  make(map[string]struct{}),
		config: DefaultConfig(),
	}

	h.addConn(c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := h.Shutdown(ctx); err != nil {
		t.Fatalf("expected graceful shutdown without error, got %v", err)
	}

	select {
	case <-c.done:
	default:
		t.Errorf("expected connection to be cleaned up on shutdown")
	}
}

// -----------------------------------------------------------------------------
// 5. Benchmarks
// -----------------------------------------------------------------------------

func BenchmarkMessage_Bytes(b *testing.B) {
	msg := NewMessage("lobby", "chat", "client_12345", "client_67890", []byte(`{"text":"benchmark payload message"}`))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = msg.Bytes()
	}
}

func BenchmarkBytesToMessage(b *testing.B) {
	msg := NewMessage("lobby", "chat", "client_12345", "client_67890", []byte(`{"text":"benchmark payload message"}`))
	raw := msg.Bytes()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = BytesToMessage(raw)
	}
}

func BenchmarkRoom_Emit_1000Conns(b *testing.B) {
	h := NewHub()
	r := newRoom("bench_room", h)
	msg := NewMessage("bench_room", "chat", "", "sender", []byte("payload"))

	conns := make([]*Conn, 1000)
	for i := 0; i < 1000; i++ {
		c := &Conn{
			ID:     fmt.Sprintf("user_%d", i),
			hub:    h,
			send:   make(chan []byte, 2048),
			done:   make(chan struct{}),
			rooms:  make(map[string]struct{}),
			config: DefaultConfig(),
		}
		r.addMember(c)
		conns[i] = c

		go func(conn *Conn) {
			for {
				select {
				case <-conn.done:
					return
				case <-conn.send:
				}
			}
		}(c)
	}

	defer func() {
		for _, c := range conns {
			c.cleanup()
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r.emit(nil, msg)
	}
}
