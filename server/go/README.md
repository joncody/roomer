# `roomer-go` – Go Server Implementation

[![Go Reference](https://pkg.go.dev/badge/github.com/joncody/roomer/server/go.svg)](https://pkg.go.dev/github.com/joncody/roomer/server/go)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

Go implementation of the Roomer WebSocket framework with 32-shard FNV-1a lock-striped concurrency, 64-byte L1/L2 cache line false sharing elimination, pluggable Redis cluster presence and unicast routing, configurable backpressure, and zero-allocation binary framing.

> 📖 **For Wire Protocol specifications and Client API documentation, see the [Root README](../../README.md).**

---

## 📦 Scope & Architecture

The `server/go` package provides the backend coordinator (`Hub`), connection handles (`Conn`), room registries, and distributed cluster adapters for Go applications.

```text
               +---------------------------------------------------+
               |               HTTP Upgrader & Auth                |
               +-------------------------+-------------------------+
                                         |
               +-------------------------v-------------------------+
               |        Hub Coordinator (32-Shard Lock Striped)    |
               +-------------------+-------------------+-----------+
                                   |                   |
                     +-------------v----+        +-----v-------------+
                     | Connection Shards|        | Room Shards (1..32|
                     |  (64B Cache Pad) |        |  (64B Cache Pad)  |
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |          Pluggable Distributed Adapter            |
               |  - Presence Sets (ZSET Heartbeat Touch on Pong)   |
               |  - True Wire-Level Unicast (SUBSCRIBE prefix:node)|
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
```

- **32-Shard Lock Striping**: Both active connections and rooms are partitioned across 32 shards using FNV-1a hashing to eliminate CPU core mutex contention.
- **False Sharing Elimination**: Shards are padded with explicit 32-byte arrays to expand each 32-byte struct to exactly 64 bytes, aligning with L1/L2 CPU cache lines.
- **Configurable Backpressure**: Choose between `DropSlowClient` (default memory protection), `DropOldest` (circular queue eviction), and `DropNewest`.
- **True Wire-Level Unicast**: Direct node messages route via dedicated `SUBSCRIBE prefix:node:<nodeID>` channels, preventing bystander cluster nodes from receiving direct traffic over the wire.
- **Presence Heartbeat Touching**: WebSocket Pong frames refresh connection timestamps in Redis ZSET presence sets via batched pipelines every 54 seconds.
- **Zero-Allocation Binary Encoding**: Packets serialize directly into exact `make([]byte, totalLen)` pre-sized buffers with `binary.BigEndian` operations.
- **Zero Redis Memory Leaks**: Pure Pub/Sub routing keeps Redis completely stateless—no persistent stream radix trees, unread entry accumulation, or dead consumer groups.

---

## 🚀 Installation

```bash
go get github.com/joncody/roomer/server/go
go get github.com/redis/go-redis/v9 # Optional for multi-node clustering
```

---

## 🧠 Quick Start

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joncody/roomer/server/go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// 1. Register custom event handlers
	_ = roomer.RegisterHandler("chat", func(c *roomer.Conn, msg *roomer.Message) error {
		// Broadcast to all room members except sender
		c.SendToRoom(msg.Room, msg.Event, msg.Payload)
		return nil
	})

	// 2. Mount WebSocket handler with production options
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithLogger(logger),
		roomer.WithMaxMessageSize(8 * 1024 * 1024),
		roomer.WithChannelCapacity(2048),
		roomer.WithBackpressureStrategy(roomer.DropSlowClient),
		roomer.WithPresenceTouchInterval(54 * time.Second),
	))

	server := &http.Server{Addr: ":8080"}

	// 3. Graceful Shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = roomer.Shutdown(shutdownCtx)
		_ = server.Shutdown(shutdownCtx)
	}()

	_ = server.ListenAndServe()
}
```

---

## 🌐 Distributed Clustering (Redis Adapter)

The Redis clustering adapter provides **loopback suppression**, **cluster presence synchronization with heartbeat touches**, and **wire-level isolated unicast routing**:

```go
package main

import (
	"net/http"
	"time"

	"github.com/joncody/roomer/server/go"
	redisadapter "github.com/joncody/roomer/server/go/adapter/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	
	adapter, err := redisadapter.New(rdb,
		redisadapter.WithPrefix("roomer:demo:"),
		redisadapter.WithPresenceTTL(180 * time.Second), // Prune inactive presence entries after 3 mins
	)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithAdapter(adapter),
	))

	_ = http.ListenAndServe(":8080", nil)
}
```

---

## 📚 API Reference

### Functional Options (`SocketHandlerWithOptions`)

| Option | Default | Description |
|---|---|---|
| `WithLogger(logger)` | `slog.Default()` | Structured logger for diagnostics and connection events. |
| `WithMetrics(metrics)` | `NopMetrics{}` | Telemetry observer for connection counts, message rates, and dropped frames. |
| `WithAdapter(adapter)` | `localAdapter` | Distributed clustering provider (e.g. `redisadapter`). |
| `WithBackpressureStrategy(strategy)`| `DropSlowClient` | Buffer saturation strategy: `DropSlowClient`, `DropOldest`, or `DropNewest`. |
| `WithPresenceTouchInterval(duration)` | `54s` | Minimum interval between presence heartbeat score updates on Pong frames. |
| `WithAuthorize(authFn)` | `nil` | Authenticator extracting claims map during handshake. |
| `WithMaxMessageSize(bytes)` | `16 MB` | Maximum allowed WebSocket frame size in bytes. |
| `WithChannelCapacity(capacity)` | `2048` | Outbound message queue capacity per connection. |
| `WithWriteWait(duration)` | `10s` | Deadline duration for writing messages to client. |
| `WithPongWait(duration)` | `60s` | Maximum time allowed between heartbeat pongs. |

### `*Conn` Methods

| Method | Description |
|---|---|
| `c.ID` | Unique connection UUID string. |
| `c.Claims` | Map of authenticated claims extracted during handshake. |
| `c.SendToRoom(room, event, payload)` | Broadcasts message to room members **except sender** (local + cluster). |
| `c.SendToClient(dstID, event, payload)` | Sends direct message to client ID via isolated node unicast. |
| `c.TrySend(msgBytes) bool` | Non-blocking send to connection buffer; applies configured backpressure strategy. |
| `c.IsInRoom(room) bool` | Checks if connection is currently in a room. |

---

## 🧪 Testing & Benchmarks

```bash
# Run unit tests and race condition detector
go test -v -race ./...

# Run memory allocation and throughput benchmarks
go test -bench=. -benchmem ./...

# Run live Redis integration test (requires Redis on localhost:6379)
REDIS_ADDR=localhost:6379 go test -v -race ./adapter/redis/...

# Run multi-node cluster load test (requires 2 nodes running on 8080 & 8081)
go run cmd/loadtest/main.go -clients=200 -messages=2000
```
