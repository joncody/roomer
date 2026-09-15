package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joncody/roomer/server/go"
)

func main() {
	node1URL := flag.String("node1", "ws://localhost:8080/ws", "WebSocket URL for Node 1")
	node2URL := flag.String("node2", "ws://localhost:8081/ws", "WebSocket URL for Node 2")
	nodesList := flag.String("nodes", "", "Comma-separated list of WebSocket node URLs (overrides -node1 and -node2)")
	roomFlag := flag.String("room", "", "Room name (default: unique timestamped room per run)")
	clientsPerNode := flag.Int("clients", 50, "Number of clients per node")
	messagesToSend := flag.Int("messages", 1000, "Number of broadcast messages to send")
	delayMicros := flag.Int("delay", 0, "Delay in microseconds between sent messages (0 = unthrottled burst)")
	flag.Parse()

	// 1. Resolve target nodes
	var targetNodes []string
	if *nodesList != "" {
		for _, u := range strings.Split(*nodesList, ",") {
			trimmed := strings.TrimSpace(u)
			if trimmed != "" {
				targetNodes = append(targetNodes, trimmed)
			}
		}
	} else {
		targetNodes = []string{*node1URL, *node2URL}
	}

	if len(targetNodes) < 2 {
		log.Fatalf("Cluster test requires at least 2 nodes; got %d", len(targetNodes))
	}

	// 2. Isolate test runs to unique rooms to prevent ghost socket collisions
	targetRoom := *roomFlag
	if targetRoom == "" {
		targetRoom = fmt.Sprintf("bench_room_%d", time.Now().UnixNano()%1000000)
	}

	log.Printf("Starting Cluster Load Test...")
	log.Printf("  Target Room: %s", targetRoom)
	for i, u := range targetNodes {
		log.Printf("  Node %d: %s (%d clients)", i+1, u, *clientsPerNode)
	}

	var chatMessagesReceived int64

	connectClients := func(url string, count int) []*websocket.Conn {
		conns := make([]*websocket.Conn, 0, count)
		for i := 0; i < count; i++ {
			c, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				log.Fatalf("Failed to connect client to %s: %v", url, err)
			}

			// Send 12-byte wire binary join message to isolated room
			joinMsg := roomer.NewMessage(targetRoom, "join", "", "", nil)
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

	var allConns []*websocket.Conn
	for _, u := range targetNodes {
		conns := connectClients(u, *clientsPerNode)
		allConns = append(allConns, conns...)
	}

	// Allow join handshakes and cluster presence propagation to complete
	time.Sleep(500 * time.Millisecond)

	sender := allConns[0]
	totalClients := len(allConns)
	expectedReceives := int64((totalClients - 1) * (*messagesToSend))

	log.Printf("Broadcasting %d chat messages from Node 1 to room '%s'...", *messagesToSend, targetRoom)
	log.Printf("Total connected clients: %d across %d cluster nodes", totalClients, len(targetNodes))

	start := time.Now()
	for i := 0; i < *messagesToSend; i++ {
		msg := roomer.NewMessage(targetRoom, "chat", "", "", []byte(fmt.Sprintf("loadtest_payload_%d", i)))
		if err := sender.WriteMessage(websocket.BinaryMessage, msg.Bytes()); err != nil {
			log.Fatalf("Sender write error: %v", err)
		}
		if *delayMicros > 0 {
			time.Sleep(time.Duration(*delayMicros) * time.Microsecond)
		}
	}

	log.Printf("Waiting for %d expected chat messages across cluster...", expectedReceives)
	deadline := time.Now().Add(10 * time.Second)
	for {
		received := atomic.LoadInt64(&chatMessagesReceived)
		if received >= expectedReceives || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	elapsed := time.Since(start)
	finalReceived := atomic.LoadInt64(&chatMessagesReceived)

	// Graceful Leave: notify server to prune room membership before severing TCP sockets
	for _, c := range allConns {
		leaveMsg := roomer.NewMessage(targetRoom, "leave", "", "", nil)
		_ = c.WriteMessage(websocket.BinaryMessage, leaveMsg.Bytes())
	}
	time.Sleep(50 * time.Millisecond)

	for _, c := range allConns {
		_ = c.Close()
	}

	log.Printf("--------------------------------------------------")
	log.Printf("CLUSTER LOAD TEST RESULTS:")
	log.Printf("Total Nodes:          %d", len(targetNodes))
	log.Printf("Total Clients:        %d", totalClients)
	log.Printf("Total Elapsed Time:   %v", elapsed)
	log.Printf("Total Receives:       %d / %d (%.2f%%)", finalReceived, expectedReceives, float64(finalReceived)/float64(expectedReceives)*100)
	log.Printf("Throughput:           %.2f messages delivered/sec", float64(finalReceived)/elapsed.Seconds())

	if finalReceived < expectedReceives {
		missing := expectedReceives - finalReceived
		log.Printf("NOTICE: %d messages dropped (clients were evicted by DropSlowClient backpressure)", missing)
	}
	log.Printf("--------------------------------------------------")
}
