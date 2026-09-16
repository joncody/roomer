# `roomer-go` – Go Server Implementation

[![Go Reference](https://pkg.go.dev/badge/github.com/joncody/roomer/server/go.svg)](https://pkg.go.dev/github.com/joncody/roomer/server/go)
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

Go implementation of the Roomer WebSocket framework with 32-shard FNV-1a lock-striped concurrency, 64-byte L1/L2 cache line false sharing elimination, pluggable Redis cluster SET presence with key expiration, token-bucket control-plane rate limiting, configurable backpressure, explicit RFC 6455 close frame control, and zero-allocation 12-byte binary framing.

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
               |  - Auto-Expiring SET Presence (SADD, SREM)        |
               |  - True Wire-Level Unicast (SUBSCRIBE prefix:node)|
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
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
		roomer.WithControlRateLimit(10.0, 20),
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

## 📚 API Reference

### `*Conn` Methods

| Method | Description |
|---|---|
| `c.ID` | Unique connection UUID string. |
| `c.Claims` | Map of authenticated claims extracted during handshake. |
| `c.SendToRoom(room, event, payload)` | Broadcasts message to room members **except sender** (local + cluster). |
| `c.SendToClient(dstID, event, payload)` | Sends direct message to client ID via isolated node unicast. |
| `c.TrySend(msgBytes) bool` | Non-blocking send to connection buffer; applies configured backpressure strategy. |
| `c.CloseWith(code, reason)` | Sends an RFC 6455 WebSocket close frame with status code and reason, then cleans up. |
| `c.IsInRoom(room) bool` | Checks if connection is currently in a room. |

### `*Hub` Methods

| Method | Description |
|---|---|
| `hub.Disconnect(connID, code, reason) bool` | Terminates an active connection by ID with an explicit close code and reason. |
| `hub.Shutdown(ctx) error` | Broadcasts 1001 Going Away frames to all connections and closes cluster adapters. |

---

## 🧪 Testing & Benchmarks

```bash
# Run unit tests and race condition detector
go test -v -race ./...
```
