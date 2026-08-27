package roomer

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// -----------------------------------------------------------------------------
// 1. Functional Unit Tests
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
}

func TestMessage_MalformedInput(t *testing.T) {
	// 1. Under minimum packet length (< 20 bytes)
	if msg := BytesToMessage([]byte{1, 2, 3}); msg != nil {
		t.Errorf("expected nil for short payload, got %+v", msg)
	}

	// 2. Corrupted length prefix exceeding slice boundary
	corrupted := []byte{
		0, 0, 0, 255, // RoomLength = 255 bytes, but total slice is only 20 bytes
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

func TestHub_ConcurrentShardedAccess(t *testing.T) {
	var wg sync.WaitGroup
	workers := 50
	connsPerWorker := 30

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < connsPerWorker; i++ {
				connID := fmt.Sprintf("conn_%d_%d", workerID, i)
				roomName := fmt.Sprintf("room_%d", (workerID+i)%10)

				c := &Conn{
					ID:    connID,
					send:  make(chan []byte, 256),
					done:  make(chan struct{}),
					rooms: make(map[string]struct{}),
				}

				// Start background drain to simulate active client reading frames
				go func(conn *Conn) {
					for {
						select {
						case <-conn.done:
							return
						case <-conn.send:
						}
					}
				}(c)

				// Concurrent Adds / Joins
				hub.addConn(c)
				hub.joinRoom(roomName, c)

				// Concurrent Reads
				if _, ok := hub.getConn(connID); !ok {
					t.Errorf("failed to retrieve conn %s", connID)
				}
				if _, ok := hub.getRoom(roomName); !ok {
					t.Errorf("failed to retrieve room %s", roomName)
				}

				// Concurrent Leaves / Removals
				hub.leaveRoom(roomName, c)
				hub.removeConn(connID)
				c.cleanup()
			}
		}(w)
	}

	wg.Wait()
}

// -----------------------------------------------------------------------------
// 2. Allocation & Performance Benchmarks
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

func BenchmarkRoom_Emit(b *testing.B) {
	r := newRoom("bench_room")
	msg := NewMessage("bench_room", "chat", "", "sender", []byte("payload"))

	for i := 0; i < 100; i++ {
		c := &Conn{
			ID:    fmt.Sprintf("user_%d", i),
			send:  make(chan []byte, 1024),
			done:  make(chan struct{}),
			rooms: make(map[string]struct{}),
		}
		r.addMember(c)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		r.emit(nil, msg)
	}
}
