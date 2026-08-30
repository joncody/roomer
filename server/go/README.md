# `roomer-go` – Go Server Implementation

[![Go Reference](https://pkg.go.dev/badge/github.com/joncody/roomer/server/go.svg)](https://pkg.go.dev/github.com/joncody/roomer/server/go)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org/)

Go implementation of the Roomer WebSocket framework with 32-shard lock-striped concurrency, pluggable Redis cluster adapters, and zero-allocation framing.

> 📖 **For Wire Protocol & Client API documentation, see the [Root README](../../README.md).**

---

## 📦 Installation

```bash
go get github.com/joncody/roomer/server/go
go get github.com/redis/go-redis/v9 # Optional Redis adapter
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

	// Register custom event handler
	_ = roomer.RegisterHandler("ping", func(c *roomer.Conn, msg *roomer.Message) error {
		reply := roomer.NewTextMessage("util", "pong", "", c.ID, "pong")
		c.TrySend(reply.Bytes())
		return nil
	})

	// Mount WebSocket handler
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithLogger(logger),
		roomer.WithMaxMessageSize(8 * 1024 * 1024),
	))

	server := &http.Server{Addr: ":8080"}

	// Graceful shutdown
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

## 🌐 Multi-Node Clustering (Redis Adapter)

```go
import (
	"github.com/joncody/roomer/server/go"
	redisadapter "github.com/joncody/roomer/server/go/adapter/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	adapter, err := redisadapter.New(rdb, redisadapter.WithPrefix("roomer:prod:"))
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithAdapter(adapter),
	))
}
```

---

## 📚 Server API Reference

### Core Functions & Handlers
| Function | Description |
|---|---|
| `RegisterHandler(event, handler)` | Registers a custom message handler. Rejects reserved events and duplicates. |
| `Shutdown(ctx)` | Sends `1001 Going Away` close frames to all connections and drains adapters. |
| `SocketHandler(auth)` | Standard `http.HandlerFunc` with authorization. |
| `SocketHandlerWithOptions(opts...)` | Configures handler via functional options (`WithLogger`, `WithMetrics`, etc.). |
| `ExtractBearerToken(r)` | Extracts `Bearer <token>` from HTTP `Authorization` header. |

### `*Conn` Methods & Fields
| Method / Field | Description |
|---|---|
| `c.ID` | Unique connection UUID string. |
| `c.Claims` | Map of authenticated claims extracted during upgrade. |
| `c.SendToRoom(room, event, payload)` | Broadcasts to room members **except sender**. |
| `c.SendToClient(dstID, event, payload)` | Sends direct message to client ID (local or across cluster). |
| `c.TrySend(msg) bool` | Non-blocking send to self (triggers async cleanup if buffer is full). |
| `c.IsInRoom(room) bool` | Checks if connection is currently in a room. |
