package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

func encodePacket(room, event, dst, src string, payload []byte) []byte {
	total := 20 + len(room) + len(event) + len(dst) + len(src) + len(payload)
	buf := make([]byte, total)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(room)))
	offset += 4
	offset += copy(buf[offset:], room)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(event)))
	offset += 4
	offset += copy(buf[offset:], event)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(dst)))
	offset += 4
	offset += copy(buf[offset:], dst)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(src)))
	offset += 4
	offset += copy(buf[offset:], src)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(payload)))
	offset += 4
	copy(buf[offset:], payload)

	return buf
}

func main() {
	node1URL := flag.String("node1", "ws://localhost:8080/ws", "WebSocket URL for Node 1")
	node2URL := flag.String("node2", "ws://localhost:8081/ws", "WebSocket URL for Node 2")
	clientsPerNode := flag.Int("clients", 50, "Number of clients per node")
	messagesToSend := flag.Int("messages", 1000, "Number of broadcast messages to send")
	flag.Parse()

	log.Printf("Starting Cluster Load Test...")
	log.Printf("Connecting %d clients to Node 1 (%s)", *clientsPerNode, *node1URL)
	log.Printf("Connecting %d clients to Node 2 (%s)", *clientsPerNode, *node2URL)

	var totalReceived int64

	connectClients := func(url string, count int) []*websocket.Conn {
		conns := make([]*websocket.Conn, 0, count)
		for i := 0; i < count; i++ {
			c, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				log.Fatalf("Failed to connect client to %s: %v", url, err)
			}

			// Join room "bench_room"
			joinPacket := encodePacket("bench_room", "join", "", "", []byte(""))
			_ = c.WriteMessage(websocket.BinaryMessage, joinPacket)

			// Background reader loop
			go func(conn *websocket.Conn) {
				for {
					_, data, err := conn.ReadMessage()
					if err != nil {
						return
					}
					if len(data) > 0 {
						atomic.AddInt64(&totalReceived, 1)
					}
				}
			}(c)

			conns = append(conns, c)
		}
		return conns
	}

	node1Clients := connectClients(*node1URL, *clientsPerNode)
	node2Clients := connectClients(*node2URL, *clientsPerNode)
	time.Sleep(500 * time.Millisecond)

	sender := node1Clients[0]
	log.Printf("Broadcasting %d messages from Node 1 to room 'bench_room'...", *messagesToSend)

	start := time.Now()
	for i := 0; i < *messagesToSend; i++ {
		msg := encodePacket("bench_room", "chat", "", "", []byte(fmt.Sprintf("loadtest_payload_%d", i)))
		if err := sender.WriteMessage(websocket.BinaryMessage, msg); err != nil {
			log.Fatalf("Sender write error: %v", err)
		}
	}

	// Expected receives = (totalClients - 1 sender) * messagesToSend
	totalClients := (*clientsPerNode) * 2
	expectedReceives := int64((totalClients - 1) * (*messagesToSend))

	log.Printf("Waiting for %d expected receives across cluster...", expectedReceives)
	for {
		received := atomic.LoadInt64(&totalReceived)
		if received >= expectedReceives || time.Since(start) > 10*time.Second {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	elapsed := time.Since(start)
	finalReceived := atomic.LoadInt64(&totalReceived)

	for _, c := range append(node1Clients, node2Clients...) {
		c.Close()
	}

	log.Printf("--------------------------------------------------")
	log.Printf("CLUSTER LOAD TEST RESULTS:")
	log.Printf("Total Elapsed Time:   %v", elapsed)
	log.Printf("Total Receives:       %d / %d (%.2f%%)", finalReceived, expectedReceives, float64(finalReceived)/float64(expectedReceives)*100)
	log.Printf("Throughput:           %.2f messages delivered/sec", float64(finalReceived)/elapsed.Seconds())
	log.Printf("--------------------------------------------------")
}
