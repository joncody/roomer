# `roomer-client` – Python Client SDK

[![PyPI Version](https://img.shields.io/pypi/v/roomer-client.svg?color=3776AB&logo=pypi&logoColor=white)](https://pypi.org/project/roomer-client/)
[![Python Version](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![AsyncIO](https://img.shields.io/badge/AsyncIO-Native-00599C?style=flat&logo=python&logoColor=white)](https://docs.python.org/3/library/asyncio.html)
[![Typing: Typed](https://img.shields.io/badge/Typing-PEP%20484%20%2F%20561-blue?style=flat)](https://peps.python.org/pep-0561/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

High-performance, asynchronous Python client for the Roomer WebSocket framework with 12-byte zero-copy binary framing, automatic exponential reconnection with jitter, cluster-wide auto-expiring SET presence synchronization, and 100% wire protocol parity across Go, Rust, and Node.js servers.

> 📖 **For Wire Protocol specifications and Server documentation, see the [Root README](../../README.md).**

---

## 📦 Scope & Architecture

The `roomer-client` library provides an asynchronous, non-blocking interface for Python applications (FastAPI backends, AI/LLM streaming pipelines, data processing workers, CLI tools) to communicate over Roomer clusters.

```text
               +---------------------------------------------------+
               |               Python Application                  |
               |     (FastAPI / LangChain / PyTorch Worker)        |
               +-------------------------+-------------------------+
                                         |
               +-------------------------v-------------------------+
               |               Roomer Client SDK                   |
               |  - asyncio / websockets async connection manager  |
               |  - Async / Sync Dual-Mode Event Emitter           |
               +-------------------+-------------------+-----------+
                                   |                   |
                     +-------------v----+        +-----v-------------+
                     | Room Multiplexer |        | 12B Wire Framing  |
                     | (Presence & Acks)|        | (struct.pack_into)|
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |              WebSocket Connection                 |
               |     (Auto-Reconnect with Exponential Jitter)      |
               +---------------------------------------------------+
```

---

## ⚡ Key Features

- **High-Performance 12-Byte Wire Framing**: Serializes and unpacks a 2-byte `[1B Version][1B Flags]` header and right-sized Big-Endian length prefixes via pre-allocated `bytearray` and zero-copy `struct.pack_into()` for maximum CPU efficiency.
- **Client-Controllable Protocol Flags**: Control wire-level `flags` directly on `.send(..., flags=...)` for compression, encryption, or prioritization markers.
- **Dual-Mode Event Emitter**: Register event listeners as either standard synchronous functions (`def handler(...)`) or native coroutines (`async def handler(...)`).
- **Async Context Manager**: Native `async with roomer("ws://...") as root:` pattern for deterministic lifecycle management and cleanup.
- **Automatic Exponential Reconnection**: Recovers from abrupt socket disconnects with randomized jitter backoff while preserving active room subscriptions across reconnects.
- **Cluster Presence Tracking**: Automatic handling of `join_ack` snapshots, `new_member` notifications, and `member_left` presence events.
- **Flow Control & Backpressure**: Real-time `.ready_state`, `.buffered_amount`, and `.url` property inspection for backpressure regulation and connection discovery.
- **Direct 1-to-1 Point-to-Point Unicast**: Route messages directly to specific client UUIDs across cluster nodes with $O(1)$ efficiency.
- **Custom Handshake & Auth Headers**: Supports passing `extra_headers` (e.g. Bearer authorization tokens) and SSL contexts directly into `websockets.connect`.

---

## 🚀 Installation

Install from PyPI:

```bash
pip install roomer-client
```

Or install in editable mode for local development:

```bash
cd client/python
pip install -e ".[dev]"
```

---

## 🧠 Quick Start

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
                # Transmit with optional destination and bitfield flags
                lobby.send("chat", "Hello from Python!", dst="", flags=0)

        @lobby.on("chat")
        def on_chat(payload: bytes, sender_id: str):
            print(f"[{sender_id}]: {payload.decode('utf-8')}")

        @lobby.on("new_member")
        def on_new_member(member_id: str):
            print(f"User joined lobby: {member_id}")

        @lobby.on("member_left")
        def on_member_left(member_id: str):
            print(f"User left lobby: {member_id}")

        # Keep running
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 📚 API Reference

### `Room` Instance Properties & Methods

#### Properties
- **`room.name -> str`**: Channel name for this room instance.
- **`room.id -> str`**: Assigned connection UUID string.
- **`room.is_open -> bool`**: Returns `True` if room membership is currently active.
- **`room.ready_state -> str`**: Connection lifecycle state (`"connecting"`, `"open"`, `"closing"`, `"closed"`).
- **`room.buffered_amount -> int`**: Number of bytes currently queued in the outbound transmission buffer.
- **`room.url -> str`**: Resolved WebSocket server endpoint URL.

#### Methods
| Method | Returns | Description |
|---|---|---|
| `room.members()` | `list[str]` | Shallow copy array of active member connection IDs. |
| `room.join(room_name)` | `Room` | Subscribes to another room channel over the active connection. |
| `room.leave()` | `Room` | Unsubscribes from the room and notifies the cluster. |
| `room.send(event, payload=None, dst="", flags=0)` | `Room` | Sends a message packet to the room or directly to `dst` with optional protocol flags. |
| `room.on(event, listener)` | `Callable` | Subscribes a synchronous or asynchronous callback. Supports `@room.on(event)`. |
| `room.once(event, listener)` | `Callable` | Subscribes a one-time event callback. |
| `room.off(event, listener)` | `None` | Unsubscribes a registered listener callback. |
| `room.clear_listeners(exceptions=None)` | `Room` | Clears custom listeners except those listed in `exceptions`. |
| `room.force_close(is_disconnect=False)` | `Room` | Clears local member state and emits `"close"`. |
| `root.close()` *(root only)* | `Coroutine` | Gracefully closes all rooms and the WebSocket connection. |
| `root.purge()` *(root only)* | `Room` | Unsubscribes from all non-root rooms simultaneously. |
| `root.rooms()` *(root only)* | `dict[str, Room]` | Dictionary mapping of all active room handles. |

---

## 🧪 Testing & Verification

Run the test suite using `pytest`:

```bash
cd client/python
pip install -e ".[dev]"
pytest -v
```

---

## 📄 License

Roomer is open-source software licensed under the [MIT License](../../LICENSE).
