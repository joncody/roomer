# `roomer` – Room-Based WebSocket Framework

[![Go Reference](https://pkg.go.dev/badge/github.com/joncody/roomer.svg)](https://pkg.go.dev/github.com/joncody/roomer)
[![Go Version](https://img.shields.io/badge/Go-1.20+-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org/)
[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![WebSocket](https://img.shields.io/badge/WebSocket-Binary%20Framing-010101?style=flat&logo=socketdotio&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Formal Verification: TLA+](https://img.shields.io/badge/Formal%20Verification-TLA%2B-555555?style=flat)](./spec/roomer.tla)
[![Client Dependencies: 0](https://img.shields.io/badge/Client%20Deps-0-brightgreen.svg)]()
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

A lightweight, high-performance WebSocket framework for real-time applications in Go (server) and JavaScript (client). Built around **rooms**, **binary framing**, and **explicit message routing**, `roomer` handles connection lifecycle, room membership, and concurrency so you don’t have to.

> 📦 **Zero client dependencies** • ⚡ **Zero-copy binary framing** • 🔒 **Lock-striped hub** • 🌲 **Formal TLA+ spec**

---

## ✅ Key Features

- 🏢 **Automatic Room Management**: Create, join, and leave rooms dynamically with automatic empty-room cleanup.
- ⚡ **Zero-Copy Binary Protocol**: Uses length-prefixed fields with direct slice decoding and single-allocation packet serialization for ultra-low latency.
- 📨 **Flexible Messaging**:
  - Broadcast to rooms (excluding sender)
  - Send direct messages to peers
  - Send private messages to self via `TrySend`
- 🔒 **Lock-Striped Sharding**: Hub connections and rooms are partitioned across 32 lock-striped shards via FNV-1a hashing to eliminate CPU core contention.
- 🚀 **On-Demand Room Fanout**: Direct non-blocking broadcasting without per-room background goroutines or channel queue bottlenecks.
- 🧩 **Handler Registration**: Register custom per-event server logic with `RegisterHandler`.
- 🌐 **Single Root Connection**: Clients start in a `"root"` room and dynamically join and leave channels over a single WebSocket connection.
- ⚙️ **Configurable Settings**: Custom limits for max message size, timeout deadlines, and WebSocket buffer sizes.
- 🧪 **Formal Verification**: Includes a TLA+ specification (`spec/roomer.tla`) proving state safety and room membership invariants.

---

## 📦 Installation

### Go Server
```bash
go get github.com/joncody/roomer
```
*Note: The server relies on `github.com/gorilla/websocket` and `github.com/google/uuid`.*

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

### 1. Go Server
```go
package main

import (
	"log"
	"net/http"
	"github.com/joncody/roomer"
)

func main() {
	// Register custom event handler
	err := roomer.RegisterHandler("ping", func(c *roomer.Conn, msg *roomer.Message) error {
		// Respond directly to the sender
		reply := roomer.NewMessage("util", "pong", "", c.ID, nil)
		c.TrySend(reply.Bytes())
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Mount WebSocket handler with custom configuration
	http.HandleFunc("/ws", roomer.SocketHandlerWithOptions(
		roomer.WithMaxMessageSize(8 * 1024 * 1024), // 8MB limit
	))
	http.ListenAndServe(":8080", nil)
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
| `SocketHandler(auth Authorize)` | Returns an `http.HandlerFunc` with default configuration options. |
| `SocketHandlerWithOptions(opts ...Option)` | Returns an `http.HandlerFunc` configured via functional options. |

### Functional Configuration Options (`Option`)
| Option | Description |
|--------|-------------|
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
| `c.TrySend(msg []byte) bool` | **Sends a message to self** (e.g., acks, replies). Non-blocking; returns `false` and tears down slow or closed clients asynchronously. |
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
- **Deadlock-Free Asynchronous Teardown**: Slow client disconnects triggered during `TrySend` execute asynchronously, guaranteeing lock ordering integrity during broadcast loops.
- **Zero-Copy Byte Decoding**: Slices and strings are decoded directly from raw buffers using bounds-checked offset math without memory reallocation.

---

## 📄 License

See [LICENSE](./LICENSE)
