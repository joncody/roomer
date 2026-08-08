
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
- 🌐 **Zero JS Dependencies**: Pure vanilla JavaScript for browser environments.
- 🧪 **Formal Verification**: Includes TLA+ specification (`spec/roomer.tla`) proving protocol safety.

---

## 📦 Installation

### Go Server
```bash
go get github.com/joncody/roomer
```
*Note: The server utilizes `github.com/gorilla/websocket` for WebSocket handling and `github.com/google/uuid` for connection identifiers.*

### JavaScript Client
Include these standalone files from `src/` in your frontend (no package manager or bundler required):
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

	http.HandleFunc("/ws", roomer.SocketHandler(nil))
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

### Events
Use `.on(event, handler)` to listen:
- `"open"` — room joined successfully
- `"close"` — room left or connection closed
- `"new_member"` — `(memberId)` when someone joins
- `"member_left"` — `(memberId)` when someone leaves
- Custom events (e.g., `"chat"`) — `(payload, senderId)`

> ⚠️ Reserved event names (`join`, `leave`, `join_ack`, `leave_ack`, `open`, `close`, `new_member`, `member_left`) cannot be used for custom messages.

---

## 📚 Server API (`roomer` Go package)

### Core Functions
| Function | Description |
|--------|-------------|
| `RegisterHandler(event string, handler func(*Conn, *Message) error)` | Registers a custom message handler. Returns error if duplicate or invalid. |
| `SocketHandler(auth Authorize)` | Returns an `http.HandlerFunc`. Optional `auth` function extracts claims from request. |

### `*Conn` Methods & Fields
| Method / Field | Description |
|--------|-------------|
| `c.SendToRoom(room, event string, payload []byte)` | Broadcasts to all room members **except sender**. |
| `c.SendToClient(dstID, event string, payload []byte)` | Sends direct message to another client (uses `"root"` room internally). |
| `c.TrySend(msg []byte) bool` | **Sends a message to self** (e.g., acks, replies). Non-blocking; returns `false` if client is slow or closed. |
| `c.ID` | Unique connection UUID string (read-only field). |
| `c.Claims` | Map of auth claims (e.g., from JWT / HTTP request). |

### Message Utilities
| Function | Description |
|--------|-------------|
| `NewMessage(room, event, dst, src string, payload []byte) *Message` | Builds a message struct with computed length fields. |
| `BytesToMessage([]byte) *Message` | Decodes binary message bytes into a `*Message` struct. |

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

> Clients send/receive **binary WebSocket frames** (`arraybuffer`).

---

## 🛡️ Concurrency & Safety

- All room operations are **goroutine-safe**.
- Connections use buffered channels + ping/pong to prevent hangs.
- **Non-blocking sends**: `TrySend` and internal messaging never block.
- Rooms auto-clean when empty.
- Malformed or oversized messages are dropped.

---

## 🧪 Testing & Formal Verification

This library includes a zero-dependency, comprehensive browser-based verification suite for the client, along with a formal TLA+ specification for the server protocol.

To run the client test suite:

1. Start the Go example server (`go run examples/main.go`).
2. Open `tests/index.html` in your browser (e.g., `http://localhost:8080/tests/index.html`).
3. View results visually on the page or open Developer Tools (`F12` -> **Console**).

To run the TLA+ model checker:
```bash
tlc spec/roomer.tla
```

---

## 📄 License

See [LICENSE](./LICENSE)
