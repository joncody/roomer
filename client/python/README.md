# `roomer-client` – Python Client SDK

[![PyPI Version](https://img.shields.io/pypi/v/roomer-client.svg?color=3776AB&logo=pypi&logoColor=white)](https://pypi.org/project/roomer-client/)
[![Python Version](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![AsyncIO](https://img.shields.io/badge/AsyncIO-Native-00599C?style=flat&logo=python&logoColor=white)](https://docs.python.org/3/library/asyncio.html)
[![Typing: Typed](https://img.shields.io/badge/Typing-PEP%20484%20%2F%20561-blue?style=flat)](https://peps.python.org/pep-0561/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

High-performance, asynchronous Python client for the Roomer WebSocket framework with zero-copy binary framing, automatic exponential reconnection with jitter, cluster-wide presence synchronization, and 100% wire protocol parity across Go, Rust, and Node.js servers.

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
                     | Room Multiplexer |        | Binary Wire Frame |
                     | (Presence & Acks)|        | (struct.pack >I)  |
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |              WebSocket Connection                 |
               |     (Auto-Reconnect with Exponential Jitter)      |
               +---------------------------------------------------+
```

---

## ⚡ Key Features

- **High-Performance Binary Wire Framing**: Serializes and unpacks 5 big-endian length-prefixed fields via native `struct.pack(">I", ...)` with zero-copy `memoryview` slicing.
- **Dual-Mode Event Emitter**: Register event listeners as either standard synchronous functions (`def handler(...)`) or native coroutines (`async def handler(...)`).
- **Async Context Manager**: Native `async with roomer("ws://...") as root:` pattern for deterministic lifecycle management and cleanup.
- **Automatic Exponential Reconnection**: Recovers from abrupt socket disconnects with randomized jitter backoff while preserving room subscriptions across reconnects.
- **Cluster Presence Tracking**: Automatic handling of `join_ack` snapshots, `new_member` notifications, and `member_left` presence events.
- **Direct 1-to-1 Point-to-Point Unicast**: Route messages directly to specific client UUIDs across cluster nodes with $O(1)$ efficiency.

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
    # Connect and auto-join the root room
    async with roomer("ws://localhost:8080/ws") as root:
        print(f"Connected to Roomer cluster! Client ID: {root.id}")

        # Join a named room channel
        lobby = root.join("lobby")

        @lobby.on("open")
        def on_open():
            print(f"Joined lobby! Active members: {lobby.members()}")
            lobby.send("chat", "Hello from Python!")

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

## 🛠️ Real-World Recipes & Patterns

### 1. Streaming AI / LLM Tokens into a Room
Stream token completions from OpenAI, Anthropic, or local HuggingFace/vLLM models in real-time to all clients subscribed to a room:

```python
import asyncio
from roomer import roomer

async def stream_ai_response(prompt: str, room_name: str):
    async with roomer("ws://localhost:8080/ws") as root:
        ai_room = root.join(room_name)

        # Simulated token generator (e.g. from vLLM or Ollama)
        tokens = ["The", " future", " of", " real-time", " messaging", " is", " binary."]
        
        for token in tokens:
            ai_room.send("ai_token", token)
            await asyncio.sleep(0.040)  # 40ms token interval

        ai_room.send("ai_complete", {"prompt": prompt, "status": "done"})

asyncio.run(stream_ai_response("Explain binary framing", "generation-101"))
```

---

### 2. FastAPI Background Task Bridge
Publish real-time telemetry, job notifications, or database changes from a FastAPI backend to connected browser clients:

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
from roomer import RoomerClient

client = RoomerClient("ws://localhost:8080/ws")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Connect Roomer client on FastAPI startup
    await client.connect()
    yield
    # Gracefully close on shutdown
    await client.close()

app = FastAPI(lifespan=lifespan)

@app.post("/notifications/broadcast")
async def notify_users(message: str, background_tasks: BackgroundTasks):
    def send_broadcast():
        alerts_room = client.get_room("system-alerts")
        alerts_room.send("alert", {"message": message, "severity": "info"})

    background_tasks.add_task(send_broadcast)
    return {"status": "broadcast scheduled"}
```

---

### 3. Targeted 1-to-1 Direct Unicast
Send direct private messages targeted at a specific client ID without broadcasting to the entire room:

```python
import asyncio
from roomer import roomer

async def main():
    async with roomer("ws://localhost:8080/ws") as root:
        target_client_id = "038edeb7-7823-4537-92c0-ba479cc2329c"

        @root.on("direct_message")
        def on_dm(payload: bytes, sender_id: str):
            print(f"[Private DM from {sender_id}]: {payload.decode('utf-8')}")

        # Send direct point-to-point packet (dst=target_client_id)
        root.send("direct_message", "Secret private message", dst=target_client_id)
        
        await asyncio.sleep(2)

asyncio.run(main())
```

---

### 4. Custom Reconnection Backoff Configuration
Fine-tune initial delay, backoff multiplier, and max backoff ceiling:

```python
from roomer import roomer

# Configured for high-resilience environments
root_context = roomer(
    "ws://localhost:8080/ws",
    reconnect=True,
    initial_delay=0.250,   # Start at 250ms backoff
    max_delay=10.0,        # Max backoff ceiling of 10s
    backoff_factor=2.0     # Double backoff duration on consecutive drops
)
```

---

## 📚 API Reference

### `roomer(url, **kwargs) -> RoomerContext`
Factory function creating an asynchronous context manager.

| Argument | Type | Default | Description |
|---|---|---|---|
| `url` | `str` | *Required* | WebSocket endpoint URL (e.g. `ws://localhost:8080/ws`). |
| `reconnect` | `bool` | `True` | Automatically reconnect on connection drop. |
| `initial_delay` | `float` | `0.5` | Initial backoff delay in seconds. |
| `max_delay` | `float` | `5.0` | Maximum reconnection delay ceiling in seconds. |
| `backoff_factor` | `float` | `1.5` | Backoff multiplier applied on consecutive failures. |

---

### `Room` Instance Properties & Methods

#### Properties
- **`room.name -> str`**: Channel name for this room instance.
- **`room.id -> str`**: Assigned connection UUID string.
- **`room.is_open -> bool`**: Returns `True` if room subscription is active.

#### Methods
| Method | Returns | Description |
|---|---|---|
| `room.members()` | `list[str]` | Shallow copy array of active member connection IDs. |
| `room.join(room_name)` | `Room` | Subscribes to another room channel over the active connection. |
| `room.leave()` | `Room` | Unsubscribes from the room and notifies the cluster. |
| `room.send(event, payload=None, dst="")` | `Room` | Sends a message packet to the room or directly to `dst`. |
| `room.on(event, listener)` | `Callable` | Subscribes a synchronous or asynchronous callback. Supports `@room.on(event)`. |
| `room.once(event, listener)` | `Callable` | Subscribes a one-time event callback. |
| `room.off(event, listener)` | `None` | Unsubscribes a registered listener callback. |
| `room.clear_listeners(exceptions=None)` | `Room` | Clears custom listeners except those listed in `exceptions`. |
| `room.force_close(is_disconnect=False)` | `Room` | Clears local member state and emits `"close"`. |
| `root.close()` *(root only)* | `Coroutine` | Gracefully closes all rooms and the WebSocket connection. |
| `root.purge()` *(root only)* | `Room` | Unsubscribes from all non-root rooms simultaneously. |
| `root.rooms()` *(root only)* | `dict[str, Room]` | Dictionary mapping of all active room handles. |

---

### `Packet` Data Attributes & Helpers

Decoded binary packet object passed into event handlers:

| Attribute / Helper | Type | Description |
|---|---|---|
| `packet.room` | `str` | Target channel / room name. |
| `packet.event` | `str` | Event descriptor string. |
| `packet.dst` | `str` | Destination member ID (empty string if room broadcast). |
| `packet.src` | `str` | Sender client ID. |
| `packet.payload` | `bytes` | Raw binary payload bytes. |
| `packet.payload_text()` | `str` | Decodes payload as UTF-8 string. |
| `packet.payload_json()` | `Any` | Unmarshals binary payload as JSON. |

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
