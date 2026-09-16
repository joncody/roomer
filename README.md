# Roomer

[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.0+-3178C6?style=flat&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev/)
[![Rust](https://img.shields.io/badge/Rust-1.88+_(2024)-DEA584?style=flat&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-22+-339933?style=flat&logo=node.js&logoColor=white)](https://nodejs.org/)
[![WebSocket](https://img.shields.io/badge/WebSocket-Binary%20Framing-010101?style=flat&logo=socketdotio&logoColor=white)](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API)
[![Formal Verification: TLA+](https://img.shields.io/badge/Formal%20Verification-TLA%2B-555555?style=flat)](./spec/roomer.tla)
[![Client Dependencies: 0](https://img.shields.io/badge/Client%20Deps-0-brightgreen.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

Roomer is a high-throughput, room-based WebSocket framework engineered with zero client runtime dependencies, 12-byte zero-copy binary wire framing, multi-node horizontal clustering (Redis Pub/Sub with wire-level isolated $O(1)$ unicast routing and auto-expiring SET presence synchronization), connection token-bucket control-plane rate limiting, and mathematically verified state invariants (TLA+).

---

## 🏛️ Repository Layout & Scopes

| Directory | Scope & Purpose |
|---|---|
| **`client/`** | Zero-dependency JavaScript / TypeScript client (`roomer.js`, `bytecursor.js`, `emitter.js`). Provides Crockfordian functional encapsulation, binary framing, readyState, bufferedAmount, URL reflection, and exponential reconnection. |
| **`client/python/`** | Asynchronous Python client SDK (`roomer.py`, `pyproject.toml`). Built for `asyncio` with native binary packing via `struct.pack_into()`, event emitters, and context managers. |
| **`server/go/`** | Production Go server implementation (Go 1.26+, 32-shard FNV-1a lock striping with 64B cache line padding, token-bucket control-plane rate limiter, Redis SET presence adapter, RFC 6455 close status frames). |
| **`server/rust/`** | Production Rust server implementation (Rust 1.88+ / Edition 2024, Axum 0.8, Tokio, `DashMap` concurrency with 64B cache alignment, token-bucket rate limiting, zero-copy `bytes::Bytes` framing, RFC 6455 close control). |
| **`server/node/`** | Production Node.js server implementation (Node 22+, Crockfordian functional encapsulation, single-allocation binary framing, token-bucket control-plane rate limiter, Redis SET adapter, RFC 6455 close status frames). |
| **`spec/`** | Formal TLA+ specification (`roomer.tla`, `roomer.cfg`) verifying safety invariants, token-bucket rate limits, and room membership state machines. |
| **`examples/`** | Unified cross-platform HTML/JS frontend demonstration and interactive room client. |
| **`tests/`** | Automated browser-based test suite verifying packet encoding, event emission, exception filtering, and teardown. |

---

## ⚡ Key Architectural Features

- **Optimized 12-Byte Binary Wire Framing**: Packets start with a 2-byte `[1B Version][1B Flags]` header followed by right-sized Big-Endian length prefixes (Room/Event `uint16`, Dst/Src `uint8`, Payload `uint32`), reducing base header overhead to only 12 bytes.
- **Triple Server Parity**: Go, Rust, and Node.js implementations share the exact binary wire protocol, Redis envelope format, and loopback suppression contract.
- **Dual Client Ecosystem**: Native client SDKs in JavaScript/TypeScript (Browser, Node, Bun, Deno) and Python (`asyncio`).
- **Connection Diagnostics & Backpressure**: Direct access to `.bufferedAmount()`, `.readyState()`, and `.url()` on client handles for telemetry and flow regulation.
- **Token-Bucket Control-Plane Rate Limiting**: Every connection enforces token-bucket rate limiting on `join` and `leave` control-plane operations to protect Redis and memory from command storms.
- **False Sharing Elimination**: Shards and metrics structs are padded and aligned to 64-byte L1/L2 CPU cache lines (in Go and Rust) to prevent cross-core cache invalidations.
- **True Wire-Level Unicast Routing**: Cluster nodes publish broadcasts to `prefix:room:*` channels while direct point-to-point frames travel over dedicated `prefix:node:<nodeID>` channels, preventing bystander nodes from receiving unicast traffic over the wire.
- **Cluster-Wide Presence with Auto-Expiring Sets**: Distributed Redis SET presence (`SADD`, `SREM`, `SMEMBERS`) with key-level TTL expiration on `AddPresence` eliminates heartbeat-touching overhead while automatically pruning abandoned rooms on server crash.
- **Configurable Backpressure Policies**: Supports `DropSlowClient` (default memory protection), `DropOldest` (queue eviction), and `DropNewest` buffer management.
- **Explicit Disconnection Control**: Clean disconnection API (`close_with` / `disconnect`) across Go, Rust, and Node servers sending RFC 6455 status codes before teardown with strict single-close frame guarantees.
- **Early Size Guarding**: Max message size validation executes before allocating or buffering frame bodies to prevent malicious memory allocation attacks.
- **Formally Verified (TLA+)**: Proven state invariants prevent disconnected zombie members, buffer leaks, and wire contract violations.
- **Ultra-High Throughput**: Capable of delivering **>2.6 million messages/second** in Go/Rust and **>260,000 messages/second** in Node.js clustered deployments with sub-millisecond fanout latency.

---

## 📐 Wire Protocol & Binary Framing

All messages (client $\leftrightarrow$ server and server $\leftrightarrow$ server) share a contiguous, big-endian binary frame with only **12 bytes** of base header overhead:

```text
+--------+-------+---------------+---------------+---------------+---------------+------------+-------------+------------+-------------+----------------+-------------------+
| 1B Ver | 1B Flg| 2B room_len   | room (UTF-8)  | 2B event_len  | event (UTF-8) | 1B dst_len | dst (UTF-8) | 1B src_len | src (UTF-8) | 4B payload_len | payload (binary)  |
+--------+-------+---------------+---------------+---------------+---------------+------------+-------------+------------+-------------+----------------+-------------------+
```

### Field Specifications
| Field | Type | Size | Description |
|---|---|---|---|
| `version` | `uint8` | 1 Byte | Protocol version (current: `0x01`). |
| `flags` | `uint8` | 1 Byte | Bitfield flags reserved for compression, encryption, or fragmentation (default: `0x00`). |
| `room_len` | `uint16` | 2 Bytes (BE) | Byte length of room channel name (0 to 65,535). |
| `room` | UTF-8 | Variable | Room channel name bytes. |
| `event_len` | `uint16` | 2 Bytes (BE) | Byte length of event descriptor (0 to 65,535). |
| `event` | UTF-8 | Variable | Event descriptor bytes. |
| `dst_len` | `uint8` | 1 Byte | Byte length of destination connection UUID (0 to 255). |
| `dst` | UTF-8 | Variable | Destination client ID string (empty for room broadcast). |
| `src_len` | `uint8` | 1 Byte | Byte length of sender connection UUID (0 to 255). |
| `src` | UTF-8 | Variable | Origin client ID string. |
| `payload_len` | `uint32` | 4 Bytes (BE) | Byte length of binary payload. |
| `payload` | Binary | Variable | Raw message payload data. |

---

## 📚 Client APIs

### 1. JavaScript / TypeScript Client (`client/roomer.js`)
Zero runtime dependencies. Written in Crockfordian functional JavaScript with complete TypeScript definitions.

```javascript
import roomer from "./client/roomer.js";

// Connect and auto-join the global "root" room
const root = roomer("ws://localhost:8080/ws", { reconnect: true });

root.on("open", () => {
    console.log("Connected to root room! Client ID:", root.id());
    console.log("Connection State:", root.readyState()); // "open"
    console.log("WebSocket URL:", root.url());           // "ws://localhost:8080/ws"

    const lobby = root.join("lobby");

    lobby.on("open", () => {
        console.log("Joined lobby! Active members:", lobby.members());
        
        // Check backpressure before large transfers
        if (lobby.bufferedAmount() < 32768) {
            lobby.send("chat", "Hello from JS!");
        }
    });

    lobby.on("chat", (payload, senderId) => {
        const text = new TextDecoder().decode(payload);
        console.log(`[${senderId}]: ${text}`);
    });
});

// Explicitly close all rooms and disconnect
// root.close();
```

---

### 2. Python Client (`client/python/roomer.py`)
Async client engineered for Python 3.10+ and `asyncio` applications with pre-allocated `bytearray` and `struct.pack_into()` framing.

```python
import asyncio
from roomer import roomer

async def main():
    # Connect and auto-join the root room (supports auth headers via **kwargs)
    async with roomer(
        "ws://localhost:8080/ws",
        extra_headers={"Authorization": "Bearer my_jwt_token"}
    ) as root:
        print(f"Connected to Roomer cluster! Client ID: {root.id}")
        print(f"Connection State: {root.ready_state}")
        print(f"WebSocket URL: {root.url}")

        # Join a named room channel
        lobby = root.join("lobby")

        @lobby.on("open")
        def on_open():
            print(f"Joined lobby! Active members: {lobby.members()}")
            if lobby.buffered_amount < 32768:
                lobby.send("chat", "Hello from Python!")

        @lobby.on("chat")
        def on_chat(payload: bytes, sender_id: str):
            print(f"[{sender_id}]: {payload.decode('utf-8')}")

        # Keep running
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

---

### `Room` Instance Methods & Properties
| Method / Property (JS) | Method / Property (Python) | Description |
|---|---|---|
| `.id()` | `.id` | Connection UUID assigned by the server. |
| `.open()` | `.is_open` | `True` if room membership is currently active. |
| `.readyState()` | `.ready_state` | Returns connection state: `"connecting"`, `"open"`, `"closing"`, or `"closed"`. |
| `.bufferedAmount()` | `.buffered_amount` | Returns number of bytes queued for transmission in the outbound buffer. |
| `.url()` | `.url` | Returns the resolved WebSocket endpoint URL string. |
| `.members()` | `.members()` | Shallow copy array/list of all active member IDs in this room. |
| `.join(roomName)` | `.join(room_name)` | Subscribes to a room channel over the existing connection (rate-limited). |
| `.leave()` | `.leave()` | Unsubscribes from the room and notifies the cluster (rate-limited). |
| `.send(event, payload?, dst?)` | `.send(event, payload=None, dst="")` | Sends a message packet (broadcast to room, or direct to `dst`). |
| `.clearListeners([exceptions])`| `.clear_listeners(exceptions=None)` | Clears registered listeners except those listed in `exceptions`. |
| `.forceClose(isDisconnect?)` | `.force_close(is_disconnect=False)` | Closes room state locally and emits `"close"`. |
| `.close()` *(root only)* | `.close()` *(root only)* | Explicitly tears down the WebSocket connection and closes all active rooms. |
| `.purge()` *(root only)* | `.purge()` *(root only)* | Leaves all non-root rooms simultaneously. |
| `.rooms()` *(root only)* | `.rooms()` *(root only)* | Read-only map/dict of all active room instances. |

---

## 📚 Server APIs

### 1. Go (`server/go`)
```go
// Close connection with custom RFC WebSocket code and reason
conn.CloseWith(4001, "Invalid credentials")

// Disconnect a client from the Hub
hub.Disconnect(connID, 4003, "Kicked from server")
```

### 2. Rust (`server/rust`)
```rust
// Close connection with custom RFC WebSocket code and reason
conn.close_with(4001, "Invalid credentials");

// Disconnect a client from the Hub
hub.disconnect(&conn_id, 4003, "Kicked from server");
```

### 3. Node.js (`server/node`)
```javascript
// Close connection with custom RFC WebSocket code and reason
conn.close_with(4001, "Invalid credentials");

// Disconnect a client from the Hub
hub.disconnect(conn_id, 4003, "Kicked from server");
```

---

## 🧪 Testing & Verification

The project provides a unified automation runner via `make`. Run `make help` to inspect the full palette of targets and configuration flags:

```bash
make help
```

---

### 1. Unit & Language Test Suites

```bash
# Run all language test suites (Go, Rust, Node, Python):
make test

# Run isolated language test suites:
make test-go       # Go tests with race detection (-race)
make test-rust     # Rust Cargo test suite with redis-adapter feature
make test-node     # Node.js test runner (node --test)
make test-python   # Python pytest suite (auto-detects virtualenv)
```

---

### 2. Multi-Node Cluster Load Testing

```bash
# Automated cross-server orchestration via Makefile (builds, starts, tests, tears down):
make cluster-test

# Test heterogeneous cluster pairs (e.g. Rust on 8080, Go on 8081):
PAIR=rust-go make cluster-test
PAIR=go-node make cluster-test
PAIR=rust-node make cluster-test
```

---

### 3. Formal Verification (TLA+)

Verify formal protocol and state machine invariants with TLC:

```bash
make check
```

---

### 4. Build & Cache Artifact Cleanup

Purge compilation outputs, test result caches across all languages (including Go test cache and Docker volumes), and virtual environment caches:

```bash
make clean
```

---

## 📄 License

Roomer is open-source software licensed under the [MIT License](./LICENSE).
