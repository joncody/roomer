# `roomer` – Room-Based WebSocket Framework

A lightweight, high-performance WebSocket framework for real-time applications in Go (server) and JavaScript (client). Built around **rooms**, **binary framing**, and **explicit message routing**, `roomer` handles connection lifecycle, room membership, and concurrency so you don’t have to.

> 📦 **Zero client dependencies** • ⚡ **Binary packet framing** • 🌲 **Formal TLA+ spec**

---

## ✅ Key Features

- 🏢 **Automatic Room Management**: Create, join, and leave rooms on demand.
- ⚡ **Efficient Binary Protocol**: Uses length-prefixed fields for compact, fast message encoding.
- 📨 **Flexible Messaging**:
  - Broadcast to rooms (excluding sender)
  - Send direct messages to peers
  - Send private messages to self via `TrySend`
- 🔒 **Concurrency-Safe**: Thread-safe rooms and hub using Go’s `sync` primitives.
- 🧩 **Handler Registration**: Register per-event logic on the server with `RegisterHandler`.
- 🌐 **Single Root Connection**: Clients start in a `"root"` room and dynamically join others.
- ⚙️ **Configurable Settings**: Custom limits for max message size, timeout deadlines, and WebSocket options.
- 🧪 **Formal Verification**: Includes TLA+ specification (`spec/roomer.tla`) proving protocol safety.

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
- `src/bytecursor.js` (binary parsing)
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
| `c.TrySend(msg []byte) bool` | **Sends a message to self** (e.g., acks, replies). Non-blocking; returns `false` if client is slow or closed. |
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
`[4][lobby][4][chat][0][][36][abc...][11][Hello room!]`

---

## 🛡️ Concurrency & Safety

- All room operations are **goroutine-safe**.
- Non-blocking sends: `TrySend` and internal messaging never block.
- Per-connection room membership tracking guarantees clean unregisters upon disconnect.
- Defensive binary parsing verifies frame bounds before advancing buffers.

---

## 📄 License

See [LICENSE](./LICENSE)
