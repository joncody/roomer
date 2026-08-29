package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joncody/roomer-go"
)

func main() {
	node1URL := flag.String("node1", "ws://localhost:8080/ws", "WebSocket URL for Node 1")
	node2URL := flag.String("node2", "ws://localhost:8080/ws", "WebSocket URL for Node 2")
	clientsPerNode := flag.Int("clients", 50, "Number of clients per node")
	messagesToSend := flag.Int("messages", 1000, "Number of broadcast messages to send")
	delayMicros := flag.Int("delay", 0, "Delay in microseconds between sent messages (0 = unthrottled burst)")
	flag.Parse()

	log.Printf("Starting Cluster Load Test...")
	log.Printf("Connecting %d clients to Node 1 (%s)", *clientsPerNode, *node1URL)
	log.Printf("Connecting %d clients to Node 2 (%s)", *clientsPerNode, *node2URL)

	var chatMessagesReceived int64

	connectClients := func(url string, count int) []*websocket.Conn {
		conns := make([]*websocket.Conn, 0, count)
		for i := 0; i < count; i++ {
			c, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				log.Fatalf("Failed to connect client to %s: %v", url, err)
			}

			// Send binary join message to room "bench_room"
			joinMsg := roomer.NewMessage("bench_room", "join", "", "", nil)
			if err := c.WriteMessage(websocket.BinaryMessage, joinMsg.Bytes()); err != nil {
				log.Fatalf("Failed to send join: %v", err)
			}

			// Background reader loop: counts ONLY "chat" broadcasts
			go func(conn *websocket.Conn) {
				for {
					_, data, err := conn.ReadMessage()
					if err != nil {
						return
					}
					msg := roomer.BytesToMessage(data)
					if msg != nil && msg.Event == "chat" {
						atomic.AddInt64(&chatMessagesReceived, 1)
					}
				}
			}(c)

			conns = append(conns, c)
		}
		return conns
	}

	node1Clients := connectClients(*node1URL, *clientsPerNode)
	node2Clients := connectClients(*node2URL, *clientsPerNode)

	// Allow join handshakes to complete before starting measurement
	time.Sleep(500 * time.Millisecond)

	sender := node1Clients[0]
	log.Printf("Broadcasting %d chat messages from Node 1 to room 'bench_room'...", *messagesToSend)

	totalClients := (*clientsPerNode) * 2
	expectedReceives := int64((totalClients - 1) * (*messagesToSend))

	start := time.Now()
	for i := 0; i < *messagesToSend; i++ {
		msg := roomer.NewMessage("bench_room", "chat", "", "", []byte(fmt.Sprintf("loadtest_payload_%d", i)))
		if err := sender.WriteMessage(websocket.BinaryMessage, msg.Bytes()); err != nil {
			log.Fatalf("Sender write error: %v", err)
		}
		if *delayMicros > 0 {
			time.Sleep(time.Duration(*delayMicros) * time.Microsecond)
		}
	}

	log.Printf("Waiting for %d expected chat messages across cluster...", expectedReceives)
	for {
		received := atomic.LoadInt64(&chatMessagesReceived)
		if received >= expectedReceives || time.Since(start) > 10*time.Second {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	elapsed := time.Since(start)
	finalReceived := atomic.LoadInt64(&chatMessagesReceived)

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
