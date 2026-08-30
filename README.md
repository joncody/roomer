# Roomer

[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-3178C6?style=flat&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![WebSocket](https://img.shields.io/badge/WebSocket-Binary%20Framing-010101?style=flat&logo=socketdotio&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Formal Verification: TLA+](https://img.shields.io/badge/Formal%20Verification-TLA%2B-555555?style=flat)](./spec/roomer.tla)
[![Client Dependencies: 0](https://img.shields.io/badge/Client%20Deps-0-brightgreen.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Roomer is a high-throughput, room-based WebSocket framework with zero client dependencies, zero-copy binary framing, multi-node horizontal clustering (Redis Pub/Sub), and formally verified state invariants (TLA+).

## Repository Layout

- **`client/`**: Zero-dependency JavaScript / TypeScript client (`roomer.js`, `bytecursor.js`, `emitter.js`).
- **`server/go/`**: Go server implementation (Go 1.21+, lock-striped sharding).
- **`server/rust/`**: Rust server implementation (Axum 0.8+, Tokio, lock-striped DashMap).
- **`spec/`**: Formal TLA+ specification (`roomer.tla`) proving state safety and room membership invariants.
- **`examples/`**: Unified frontend interactive demo.
- **`tests/`**: Unified browser test suite.

---

## 🏛️ Architecture & Wire Protocol

### 1. Zero-Copy Binary Framing
Every packet is serialized into 5 big-endian, length-prefixed fields (20-byte header overhead):

```
+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+-------------------+
| 4B room_len   | room (UTF-8)  | 4B event_len  | event (UTF-8) | 4B dst_len    | dst (UTF-8)   | 4B src_len    | src (UTF-8)   | 4B payload_len| payload (binary)  |
+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+---------------+-------------------+
```

#### Example Frame (39 bytes total)
```
Field         Value                   Length Prefix
---------------------------------------------------
room          "lobby"                 00 00 00 05 (5B)
event         "chat"                  00 00 00 04 (4B)
dst           "" (broadcast)          00 00 00 00 (0B)
src           "user-123"              00 00 00 08 (8B)
payload       "Hello"                 00 00 00 05 (5B)
```

### 2. Multi-Node Clustering with Loopback Suppression
When scaling across multiple instances, nodes publish to Redis channels prefixed by room name. Messages are wrapped in a binary node envelope (`[4B node_id_len][node_id][packet]`). The originating node filters out its own messages:

```mermaid
sequenceDiagram
    autonumber
    actor ClientA as Client A (Node 1)
    participant Node1 as Roomer Node 1
    participant Redis as Redis Pub/Sub
    participant Node2 as Roomer Node 2
    actor ClientB as Client B (Node 2)

    ClientA->>Node1: Binary Message (room: lobby, event: chat)
    Note over Node1: Fanout to local connections in lobby
    Node1->>Redis: PUBLISH roomer:lobby [Envelope: Node1_UUID + Packet]
    Redis-->>Node1: Message received (Self-Echo)
    Note over Node1: 🚫 Drop self-echo
    Redis-->>Node2: Message received
    Note over Node2: ✅ Decode & Fanout
    Node2->>ClientB: Binary Frame delivered
```

### 3. Room & Connection Lifecycle
```mermaid
stateDiagram-v2
    direction TB
    [*] --> RootConnection: 1. WebSocket Upgrade (Auto-joins "root")
    RootConnection --> InScopedRoom: 2. root.join("lobby") / join_ack
    InScopedRoom --> InScopedRoom: Broadcasts & Presence (new_member / member_left)
    InScopedRoom --> RootConnection: 3. lobby.leave() / leave_ack
    RootConnection --> [*]: 4. Socket Close / Shutdown (1001)
```

---

## 📚 Client API (`client/roomer.js`)

### Initialization
```typescript
import roomer from "./client/roomer.js";

const root = roomer("ws://localhost:8080/ws", {
    reconnect: true,     // Default: true
    initial_delay: 500,  // Default: 500ms
    max_delay: 5000      // Default: 5000ms
});
```

### `Room` Methods
| Method | Returns | Description |
|---|---|---|
| `.id()` | `string` | Assigned connection UUID string. |
| `.open()` | `boolean` | `true` if room membership is active. |
| `.members()` | `string[]` | Shallow copy array of active member IDs in this room. |
| `.join(name)` | `Room` | Joins a room channel over the same connection. |
| `.leave()` | `Room` | Leaves the room and notifies the server. |
| `.send(event, payload?, dst?)` | `Room` | Sends message packet (broadcast or direct to `dst`). |
| `.clearListeners([exceptions])` | `Room` | Removes event listeners except those listed in `exceptions`. |
| `.forceClose(is_disconnect?)` | `Room` | Closes room locally and emits `"close"`. |
| `.purge()` *(root only)* | `Room` | Leaves all non-root rooms simultaneously. |
| `.rooms()` *(root only)* | `Record<string, Room>` | Map of active room instances. |

### Reserved Protocol Events
The following event names cannot be sent directly via `.send()`:
`"join"`, `"leave"`, `"join_ack"`, `"leave_ack"`, `"new_member"`, `"member_left"`, `"open"`, `"close"`.

---

## 🚀 Server Implementations

- [Go Server Documentation](./server/go/README.md)
- [Rust Server Documentation](./server/rust/README.md)
- [TLA+ Formal Specification](./spec/roomer.tla)
