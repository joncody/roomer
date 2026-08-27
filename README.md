# `roomer` – Room-Based WebSocket Framework

[![Go Reference](https://pkg.go.dev/badge/github.com/joncody/roomer.svg)](https://pkg.go.dev/github.com/joncody/roomer)
[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org/)
[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![WebSocket](https://img.shields.io/badge/WebSocket-Binary%20Framing-010101?style=flat&logo=socketdotio&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Formal Verification: TLA+](https://img.shields.io/badge/Formal%20Verification-TLA%2B-555555?style=flat)](./spec/roomer.tla)
[![Client Dependencies: 0](https://img.shields.io/badge/Client%20Deps-0-brightgreen.svg)]()
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

A lightweight, enterprise-grade WebSocket framework for real-time applications in Go (server) and JavaScript (client). Built around **rooms**, **binary framing**, and **explicit message routing**, `roomer` handles connection lifecycles, horizontal clustering, observability, and concurrency.

> 📦 **Zero client dependencies** • ⚡ **Zero-copy binary framing** • 🌐 **Multi-node clustering** • 🌲 **Formal TLA+ spec**

---

## ✅ Key Features

- 🏢 **Automatic Room Management**: Create, join, and leave rooms dynamically with automatic empty-room cleanup.
- ⚡ **Zero-Copy Binary Protocol**: Uses length-prefixed fields with direct slice decoding and single-allocation packet serialization for ultra-low latency.
- 🌐 **Pluggable Cluster Scaling (`Adapter`)**: Multi-node horizontal scaling support across Redis, NATS, or Kafka pub/sub clusters with a zero-dependency in-memory default.
- 🔁 **Built-in Loopback Suppression**: Distributed adapters filter node self-echoes to guarantee clients never receive duplicate messages.
- 📊 **Production Observability (`Metrics`)**: Telemetry hooks for Prometheus and OpenTelemetry instrumenting connections, room counts, byte throughput, and drop events.
- 🪵 **Structured Logging (`log/slog`)**: Integrated with Go 1.21+ structured logging with configurable handlers and log levels.
- 🛑 **Graceful Draining & Shutdown**: Package-level `Shutdown(ctx)` flushes queues, sends WebSocket `1001 Going Away` close frames, and cleans up active connections within deadline contexts.
- 🔒 **Lock-Striped Sharding**: Hub connections and rooms are partitioned across 32 lock-striped shards via FNV-1a hashing to eliminate CPU core contention.
- 🚀 **On-Demand Room Fanout**: Direct non-blocking broadcasting without per-room background goroutines or channel queue bottlenecks.
- 🧩 **Handler Registration**: Register custom per-event server logic with `RegisterHandler`.
- 🌐 **Single Root Connection**: Clients start in a `"root"` room and dynamically join and leave channels over a single WebSocket connection.
- 🧪 **Formal Verification**: Includes a TLA+ specification (`spec/roomer.tla`) proving state safety and room membership invariants.

---

## 📦 Installation

### Go Server
```bash
go get github.com/joncody/roomer
```
*Requires Go 1.21+.*

### Optional Redis Cluster Adapter
```bash
go get github.com/redis/go-redis/v9
```

### JavaScript Client
Include these standalone files from `src/` in your frontend:
- `src/roomer.js`
- `src/bytecursor.js` (zero-copy binary parsing)
- `src/emitter.js` (event subscription)

```js
import roomer from './roomer.js';
```

---

## 🧠 Quick Start

### 1. Production Go Server with Graceful Shutdown & Telemetry
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

	"github.com/joncody/roomer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Register custom event handler
	err := roomer.RegisterHandler("ping", func(c *roomer.Conn, msg *roomer.Message) error {
		reply := roomer.NewMessage("util", "pong", "", c.ID, nil)
		c.TrySend(reply.Bytes())
		return nil
	})
	if err != nil {
		logger.Error("Failed to register handler", "err", err)
		os.Exit(1)
	}

	// Mount WebSocket handler with production options
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithLogger(logger),
		roomer.WithMaxMessageSize(8 * 1024 * 1024), // 8MB limit
	))

	server := &http.Server{Addr: ":8080"}

	// Graceful shutdown listener
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		logger.Info("Shutting down server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = roomer.Shutdown(shutdownCtx)
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("Server listening on :8080")
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("Server error", "err", err)
	}
}
```

### 2. JavaScript Client
```js
import roomer from "./roomer.js";

const decoder = new TextDecoder("utf-8");
const root = roomer("ws://localhost:8080/ws");

root.on("open", function () {
    console.log("Connected! My ID: " + root.id());

    const lobby = root.join("lobby");

    lobby.on("open", function () {
        lobby.send("ping", new Uint8Array(0));
    });

    lobby.on("pong", function (payload, sender_id) {
        console.log("Received pong from " + sender_id);
    });

    lobby.on("new_member", function (id) {
        console.log("User joined: " + id);
    });

    lobby.on("member_left", function (id) {
        console.log("User left: " + id);
    });
});
```

---

## 🌐 Horizontal Scaling (Multi-Node Clustering)

By default, `roomer` operates in single-node mode with **zero external infrastructure dependencies**.

When scaling horizontally across multiple server instances behind a load balancer, import the built-in Redis adapter from `adapter/redis`:

```go
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/joncody/roomer"
	redisadapter "github.com/joncody/roomer/adapter/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Connect to Redis (supports standalone, cluster, or sentinel)
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 2. Initialize the cluster adapter (handles loopback suppression automatically)
	adapter, err := redisadapter.New(rdb,
		redisadapter.WithPrefix("roomer:prod:"),
		redisadapter.WithPublishTimeout(3 * time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to init redis adapter: %v", err)
	}

	// 3. Mount WebSocket handler with cluster scaling
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithAdapter(adapter),
	))

	http.ListenAndServe(":8080", nil)
}
```

### 🐳 Local Cluster Development with Docker

To test multi-node clustering locally across multiple server processes:

1. **Start Redis**:
   ```bash
   docker run -d --name roomer-redis -p 6379:6379 redis:alpine
   ```

2. **Start Node 1 (Port 8080)**:
   ```bash
   PORT=8080 REDIS_ADDR=localhost:6379 go run examples/main.go
   ```

3. **Start Node 2 (Port 8081)**:
   ```bash
   PORT=8081 REDIS_ADDR=localhost:6379 go run examples/main.go
   ```

4. Open `http://localhost:8080/` in one browser tab and `http://localhost:8081/` in another. Messages sent in room `"lobby"` will synchronize instantly across both nodes without message duplication.

### Writing a Custom Adapter
You can also implement the `roomer.Adapter` interface for NATS, Kafka, or Postgres:

```go
type Adapter interface {
    Publish(ctx context.Context, room string, msg *Message) error
    Subscribe(handler func(room string, msg *Message)) error
    Close() error
}
```

---

## 📚 Client API (`roomer.js`)

### Initialization
```js
const root = roomer("ws://...");
```
Returns the `"root"` room. All other rooms are created via `.join()`.

### Room Methods
| Method | Description |
|--------|-------------|
| `.clearListeners([exceptions])` | Removes registered event listeners except those listed in `exceptions`. |
| `.forceClose()` | Forcefully closes the room locally and clears state. |
| `.id()` | Returns your assigned client ID in this room. |
| `.join(name)` | Joins a room; returns a room client instance. |
| `.leave()` | Leaves the room and cleans up. |
| `.members()` | Returns a copy array of current member IDs. |
| `.open()` | `true` if the room connection is active. |
| `.send(event, payload, [dst])` | Sends a message (to room or direct to `dst`). |

---

## 📚 Server API (`roomer` Go package)

### Core Functions & Handlers
| Function | Description |
|--------|-------------|
| `RegisterHandler(event string, handler MessageHandler)` | Registers a custom message handler. Returns error if duplicate or invalid. |
| `Shutdown(ctx context.Context) error` | Sends WebSocket `1001 Going Away` close frames to all connections and drains adapters within context deadline. |
| `SocketHandler(auth Authorize)` | Returns an `http.HandlerFunc` with default configuration options. |
| `SocketHandlerWithOptions(opts ...Option)` | Returns an `http.HandlerFunc` configured via functional options. |

### Functional Configuration Options (`Option`)
| Option | Description |
|--------|-------------|
| `WithAdapter(adapter Adapter)` | Configures a multi-node distributed cluster adapter (e.g., Redis, NATS). |
| `WithMetrics(metrics Metrics)` | Hooks into telemetry metrics providers (e.g., Prometheus, OpenTelemetry). |
| `WithLogger(logger *slog.Logger)` | Configures standard structured `slog` logger instance. |
| `WithAuthorize(auth Authorize)` | Sets authorization function for HTTP upgrade requests. |
| `WithMaxMessageSize(size int64)` | Sets maximum allowed inbound frame size in bytes. |
| `WithWriteWait(d time.Duration)` | Sets write deadline for outbound frames. |
| `WithPongWait(d time.Duration)` | Sets deadline for reading client pong responses. |
| `WithPingPeriod(d time.Duration)` | Sets frequency for sending server ping frames. |
| `WithCheckOrigin(check func(*http.Request) bool)` | Configures HTTP WebSocket handshake origin check. |
| `WithBufferSizes(readSize, writeSize int)` | Sets upgrader read and write buffer sizes. |

### `*Conn` Methods & Fields
| Method / Field | Description |
|--------|-------------|
| `c.SendToRoom(room, event string, payload []byte)` | Broadcasts to all room members **except sender**. |
| `c.SendToClient(dstID, event string, payload []byte)` | Sends direct message to another client (uses `"root"` room internally). |
| `c.TrySend(msg []byte) bool` | **Sends a message to self** (e.g., acks, replies). Non-blocking; drops and tears down slow or closed clients asynchronously. |
| `c.ID` | Unique connection UUID string (read-only field). |
| `c.Claims` | Map of auth claims (e.g., from JWT / HTTP request). |

---

## 📐 Message Protocol (Binary)

Each message is a sequence of **length-prefixed** fields (big-endian `uint32`):

1. Room name (`string`)
2. Event name (`string`)
3. Destination ID (`string`, empty = broadcast)
4. Source ID (`string`)
5. Payload (`[]byte`)

Example:  
`[5][lobby][4][chat][0][][36][abc...][11][Hello room!]`

---

## 🛡️ Concurrency & Safety

- **Lock-Striped Sharding**: Hub state is partitioned into 32 distinct shards to distribute read/write locks across CPU cores under heavy concurrent traffic.
- **On-Demand Fanout**: Broadcasts iterate through members directly under reader locks without channel queuing overhead or dedicated per-room goroutines.
- **Loopback Suppression**: Multi-node adapters tag packets with a binary node envelope to prevent publishers from receiving self-echoes.
- **Deadlock-Free Asynchronous Teardown**: Slow client disconnects triggered during `TrySend` execute asynchronously, guaranteeing lock ordering integrity during broadcast loops.
- **Zero-Copy Byte Decoding**: Slices and strings are decoded directly from raw buffers using bounds-checked offset math without memory reallocation.

---

## 📄 License

See [LICENSE](./LICENSE)
