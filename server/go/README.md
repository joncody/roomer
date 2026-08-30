# `roomer-go` – Go Server Implementation

[![Go Reference](https://pkg.go.dev/badge/github.com/joncody/roomer/server/go.svg)](https://pkg.go.dev/github.com/joncody/roomer/server/go)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

Go implementation of the Roomer WebSocket framework with 32-shard FNV-1a lock-striped concurrency, pluggable Redis cluster presence and unicast routing, configurable backpressure, and zero-allocation binary framing.

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
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |        Pluggable Distributed Adapter (Redis)      |
               |  - Presence Sets (SADD/SREM/SMEMBERS)             |
               |  - Targeted Unicast Routing (PublishDirect)       |
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
```

- **32-Shard Lock Striping**: Both active connections and rooms are partitioned across 32 shards using FNV-1a hashing to eliminate CPU core mutex contention.
- **Configurable Backpressure**: Choose between `DropSlowClient` (default memory protection), `DropOldest` (circular queue eviction), and `DropNewest`.
- **Zero-Allocation Binary Encoding**: Packets serialize directly into exact `make([]byte, totalLen)` pre-sized buffers with `binary.BigEndian` operations.

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

The Redis clustering adapter provides **loopback suppression**, **cluster presence synchronization**, and **$O(1)$ unicast direct routing**:

```go
package main

import (
	"net/http"

	"github.com/joncody/roomer/server/go"
	redisadapter "github.com/joncody/roomer/server/go/adapter/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	
	adapter, err := redisadapter.New(rdb,
		redisadapter.WithPrefix("roomer:demo:"),
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
| `c.SendToClient(dstID, event, payload)`| Sends direct message to client ID via $O(1)$ node unicast. |
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
