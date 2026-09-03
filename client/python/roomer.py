"""
Roomer Python Client SDK.

High-performance, room-based WebSocket client with zero-copy binary framing,
exponential reconnection with jitter, presence synchronization, and full parity
with Go, Rust, and Node.js servers.

License: MIT
"""

from __future__ import annotations

import asyncio
import inspect
import json
import random
import struct
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import websockets

HEADER_OVERHEAD: int = 20

RESERVED_EVENTS: frozenset[str] = frozenset({
    "close",
    "join",
    "join_ack",
    "leave",
    "leave_ack",
    "member_left",
    "new_member",
    "open"
})

Listener = Callable[..., Any]


# ==============================================================================
# 1. Wire Protocol & Binary Framing
# ==============================================================================

@dataclass(frozen=True, slots=True)
class Packet:
    """Represents an immutable decoded Roomer binary wire frame."""
    room: str
    event: str
    dst: str
    src: str
    payload: bytes

    def payload_text(self, encoding: str = "utf-8") -> str:
        """Decodes the binary payload as a text string."""
        return self.payload.decode(encoding)

    def payload_json(self) -> Any:
        """Parses the binary payload as JSON."""
        if not self.payload:
            raise ValueError("Payload is empty")
        return json.loads(self.payload.decode("utf-8"))


def encode_message(
    room: str = "",
    event: str = "",
    dst: str = "",
    src: str = "",
    payload: bytes | bytearray | memoryview | str | dict | list | int | float | bool | None = None
) -> bytes:
    """
    Serializes message parameters into a contiguous big-endian length-prefixed binary packet.
    Format: [4B room_len][room][4B event_len][event][4B dst_len][dst][4B src_len][src][4B payload_len][payload]
    """
    room_bytes = room.encode("utf-8") if isinstance(room, str) else b""
    event_bytes = event.encode("utf-8") if isinstance(event, str) else b""
    dst_bytes = dst.encode("utf-8") if isinstance(dst, str) else b""
    src_bytes = src.encode("utf-8") if isinstance(src, str) else b""

    if payload is None:
        payload_bytes = b""
    elif isinstance(payload, bytes):
        payload_bytes = payload
    elif isinstance(payload, (bytearray, memoryview)):
        payload_bytes = bytes(payload)
    elif isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    elif isinstance(payload, (dict, list)):
        payload_bytes = json.dumps(payload).encode("utf-8")
    elif isinstance(payload, (int, float, bool)):
        payload_bytes = str(payload).encode("utf-8")
    else:
        payload_bytes = b""

    fmt = f">I{len(room_bytes)}sI{len(event_bytes)}sI{len(dst_bytes)}sI{len(src_bytes)}sI{len(payload_bytes)}s"
    return struct.pack(
        fmt,
        len(room_bytes),
        room_bytes,
        len(event_bytes),
        event_bytes,
        len(dst_bytes),
        dst_bytes,
        len(src_bytes),
        src_bytes,
        len(payload_bytes),
        payload_bytes
    )


def decode_message(data: bytes | bytearray | memoryview) -> Packet | None:
    """
    Decodes raw binary bytes into a Packet instance. Returns None on malformed input.
    """
    buf = memoryview(data)
    if len(buf) < HEADER_OVERHEAD:
        return None

    offset = 0

    try:
        # 1. Room
        (room_len,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        if offset + room_len > len(buf):
            return None
        room = bytes(buf[offset : offset + room_len]).decode("utf-8")
        offset += room_len

        # 2. Event
        if offset + 4 > len(buf):
            return None
        (event_len,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        if offset + event_len > len(buf):
            return None
        event = bytes(buf[offset : offset + event_len]).decode("utf-8")
        offset += event_len

        # 3. Dst
        if offset + 4 > len(buf):
            return None
        (dst_len,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        if offset + dst_len > len(buf):
            return None
        dst = bytes(buf[offset : offset + dst_len]).decode("utf-8")
        offset += dst_len

        # 4. Src
        if offset + 4 > len(buf):
            return None
        (src_len,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        if offset + src_len > len(buf):
            return None
        src = bytes(buf[offset : offset + src_len]).decode("utf-8")
        offset += src_len

        # 5. Payload
        if offset + 4 > len(buf):
            return None
        (payload_len,) = struct.unpack_from(">I", buf, offset)
        offset += 4
        if offset + payload_len != len(buf):
            return None
        payload = bytes(buf[offset : offset + payload_len])

        return Packet(room=room, event=event, dst=dst, src=src, payload=payload)
    except (struct.error, UnicodeDecodeError):
        return None


# ==============================================================================
# 2. Async-Compatible Event Emitter
# ==============================================================================

class EventEmitter:
    """Lightweight event emitter supporting both sync functions and async coroutines."""

    def __init__(self) -> None:
        self._events: dict[str, list[Listener]] = defaultdict(list)

    def on(self, event: str, listener: Listener | None = None) -> Listener | Callable[[Listener], Listener]:
        """Registers an event listener callback or decorator."""
        def decorator(fn: Listener) -> Listener:
            self._events[event].append(fn)
            return fn

        if listener is not None:
            return decorator(listener)
        return decorator

    def once(self, event: str, listener: Listener | None = None) -> Listener | Callable[[Listener], Listener]:
        """Registers a one-time event listener callback."""
        def decorator(fn: Listener) -> Listener:
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                self.off(event, wrapper)
                return fn(*args, **kwargs)

            setattr(wrapper, "_original_fn", fn)
            self._events[event].append(wrapper)
            return fn

        if listener is not None:
            return decorator(listener)
        return decorator

    def off(self, event: str, listener: Listener) -> None:
        """Removes a registered event listener callback."""
        if event in self._events:
            self._events[event] = [
                fn for fn in self._events[event]
                if fn != listener and getattr(fn, "_original_fn", None) != listener
            ]
            if not self._events[event]:
                del self._events[event]

    def emit(self, event: str, *args: Any, **kwargs: Any) -> bool:
        """Synchronously invokes listeners, dispatching coroutines to the event loop."""
        listeners = list(self._events.get(event, []))
        if not listeners:
            return False

        for fn in listeners:
            try:
                res = fn(*args, **kwargs)
                if inspect.isawaitable(res):
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(res)
                    except RuntimeError:
                        pass
            except Exception as err:
                print(f"[Roomer] Unhandled exception in listener for '{event}': {err}")

        return True

    def remove_all_listeners(self, event: str | None = None) -> None:
        """Removes all registered listeners, or those for a specific event."""
        if event is None:
            self._events.clear()
        elif event in self._events:
            del self._events[event]

    def listeners(self, event: str) -> list[Listener]:
        """Returns a copy of registered listener callbacks for an event."""
        return list(self._events.get(event, []))


# ==============================================================================
# 3. Room Channel Handle
# ==============================================================================

class Room(EventEmitter):
    """Represents a room channel subscription over the WebSocket connection."""

    def __init__(
        self,
        name: str,
        send_packet_fn: Callable[[str, str, str, str, Any], None],
        get_room_fn: Callable[[str], Room],
        is_socket_open_fn: Callable[[], bool],
    ) -> None:
        super().__init__()
        self.name = name
        self._send_packet = send_packet_fn
        self._get_room = get_room_fn
        self._is_socket_open = is_socket_open_fn

        self._member_id: str = ""
        self._is_open: bool = False
        self._members: list[str] = []
        self._custom_events: set[str] = set()

    @property
    def id(self) -> str:
        """Connection UUID string assigned by the server."""
        return self._member_id

    @property
    def is_open(self) -> bool:
        """Whether room membership is currently active."""
        return self._is_open

    def open(self) -> bool:
        """Method alias for is_open."""
        return self._is_open

    def members(self) -> list[str]:
        """Returns a shallow copy of active member IDs in this room."""
        return list(self._members)

    def join(self, room_name: str) -> Room:
        """Subscribes to a new room channel over the existing connection."""
        if not self._is_open:
            raise RuntimeError("Cannot join: current room is closed.")
        if not isinstance(room_name, str):
            raise TypeError("Room name must be a string.")
        return self._get_room(room_name)

    def leave(self) -> Room:
        """Leaves the current room and notifies the cluster."""
        if not self._is_open:
            raise RuntimeError("Cannot leave: room is closed.")
        if self._is_socket_open():
            self._send_packet(self.name, "leave", "", "", b"")
        return self

    def send(self, event: str, payload: Any = None, dst: str = "") -> Room:
        """Sends an event message to the room or directly to a member ID."""
        if not self._is_open:
            raise RuntimeError("Cannot send: socket is closed.")
        if not isinstance(event, str):
            raise TypeError("Event name must be a string.")
        if event in RESERVED_EVENTS:
            raise ValueError(f"Cannot send reserved event: '{event}'")

        if self._is_socket_open():
            self._send_packet(self.name, event, dst, self._member_id, payload)
        return self

    def clear_listeners(self, exceptions: list[str] | set[str] | None = None) -> Room:
        """Clears registered event listeners except those listed in exceptions."""
        exc = set(exceptions or [])
        for event_name in list(self._custom_events):
            if event_name not in exc:
                self.remove_all_listeners(event_name)
                self._custom_events.discard(event_name)
        return self

    def force_close(self, is_disconnect: bool = False) -> Room:
        """Closes the room locally and clears tracked state."""
        if self._is_open:
            self._is_open = False
            self._members.clear()
            self.emit("close")
            if not is_disconnect:
                self._member_id = ""
        return self

    def parse(self, packet: Packet) -> None:
        """Dispatches an incoming parsed packet to room event listeners."""
        match packet.event:
            case "join_ack":
                self._member_id = packet.src
                self._members.clear()
                try:
                    parsed = json.loads(packet.payload_text())
                    if isinstance(parsed, list):
                        self._members.extend(str(m) for m in parsed)
                except Exception:
                    pass
                self._is_open = True
                self.emit("open")

            case "new_member":
                member_id = packet.payload_text()
                if member_id and member_id not in self._members:
                    self._members.append(member_id)
                    self.emit("new_member", member_id)

            case "leave_ack":
                self.emit("close")
                self._is_open = False
                self._members.clear()
                self._member_id = ""

            case "member_left":
                member_id = packet.payload_text()
                if member_id in self._members:
                    self._members.remove(member_id)
                    self.emit("member_left", member_id)

            case _:
                self.emit(packet.event, packet.payload, packet.src)

    def on(self, event: str, listener: Listener | None = None) -> Any:
        if event not in RESERVED_EVENTS:
            self._custom_events.add(event)
        return super().on(event, listener)


# ==============================================================================
# 4. Connection Lifecycle & Context Manager
# ==============================================================================

class RoomerClient:
    """Manages the underlying WebSocket connection and room multiplexing."""

    def __init__(
        self,
        url: str,
        reconnect: bool = True,
        initial_delay: float = 0.5,
        max_delay: float = 5.0,
        backoff_factor: float = 1.5,
    ) -> None:
        if not isinstance(url, str):
            raise TypeError("WebSocket URL must be a string.")

        self.url = url
        self.reconnect = reconnect
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor

        self._rooms: dict[str, Room] = {}
        self._ws: Any = None
        self._running: bool = False
        self._manual_close: bool = False
        self._task: asyncio.Task[None] | None = None
        self._reconnect_delay: float = initial_delay
        self._send_queue: asyncio.Queue[bytes] = asyncio.Queue()

        self._root = self.get_room("root")

    @property
    def root(self) -> Room:
        """Returns the default 'root' room instance."""
        return self._root

    def get_room(self, name: str) -> Room:
        """Retrieves or instantiates a room client interface by name."""
        if not isinstance(name, str):
            raise TypeError("Room name must be a string.")
        if name in self._rooms:
            return self._rooms[name]

        room = Room(
            name=name,
            send_packet_fn=self._send_packet,
            get_room_fn=self.get_room,
            is_socket_open_fn=self.is_connected,
        )

        if name == "root":
            def purge() -> Room:
                for r_name in list(self._rooms.keys()):
                    if r_name != "root":
                        self._rooms[r_name].leave()
                return room

            def rooms_map() -> dict[str, Room]:
                return dict(self._rooms)

            async def close() -> None:
                await self.close()

            setattr(room, "purge", purge)
            setattr(room, "rooms", rooms_map)
            setattr(room, "close", close)

        self._rooms[name] = room

        if name != "root" and self.is_connected():
            self._send_packet(name, "join", "", "", b"")

        return room

    def is_connected(self) -> bool:
        """Returns True if the underlying WebSocket connection is active."""
        return self._ws is not None and not getattr(self._ws, "closed", False)

    def _send_packet(
        self,
        room: str,
        event: str,
        dst: str,
        src: str,
        payload: Any
    ) -> None:
        """Serializes and enqueues a binary frame for ordered transmission over WebSocket."""
        if self.is_connected():
            raw = encode_message(room, event, dst, src, payload)
            try:
                self._send_queue.put_nowait(raw)
            except (asyncio.QueueFull, RuntimeError):
                pass

    async def connect(self) -> Room:
        """Starts the background event loop and waits for root join_ack."""
        if self._running:
            return self._root

        self._running = True
        self._manual_close = False
        self._task = asyncio.create_task(self._run_loop())

        while self._running and not self._root.is_open:
            await asyncio.sleep(0.010)

        if not self._root.is_open and not self._running:
            raise ConnectionError(f"Failed to connect to Roomer server at {self.url}")

        return self._root

    async def _run_loop(self) -> None:
        """Background connection supervisor with ordered writer task, backoff, and jitter."""
        while self._running:
            writer_task: asyncio.Task[None] | None = None
            try:
                async with websockets.connect(self.url) as ws:
                    self._ws = ws
                    self._reconnect_delay = self.initial_delay

                    async def _writer_loop() -> None:
                        while self._running and self._ws is ws:
                            try:
                                raw = await self._send_queue.get()
                                await ws.send(raw)
                                self._send_queue.task_done()
                            except (asyncio.CancelledError, OSError, websockets.exceptions.WebSocketException):
                                break

                    writer_task = asyncio.create_task(_writer_loop())

                    # Re-join all non-root active rooms upon reconnection
                    for r_name in list(self._rooms.keys()):
                        if r_name != "root":
                            raw = encode_message(r_name, "join", "", "", b"")
                            await ws.send(raw)

                    # Reader loop
                    async for raw_message in ws:
                        if isinstance(raw_message, (bytes, bytearray, memoryview)):
                            packet = decode_message(raw_message)
                            if packet is not None and packet.room in self._rooms:
                                self._rooms[packet.room].parse(packet)

            except (websockets.exceptions.WebSocketException, OSError, asyncio.CancelledError):
                pass
            finally:
                if writer_task is not None:
                    writer_task.cancel()
                    try:
                        await writer_task
                    except asyncio.CancelledError:
                        pass
                self._ws = None
                is_reconnecting = self.reconnect and not self._manual_close and self._running
                for r in list(self._rooms.values()):
                    r.force_close(is_disconnect=is_reconnecting)

            if not self.reconnect or self._manual_close or not self._running:
                self._running = False
                break

            jitter = random.uniform(0, 0.200)
            await asyncio.sleep(self._reconnect_delay + jitter)
            self._reconnect_delay = min(self._reconnect_delay * self.backoff_factor, self.max_delay)

    async def close(self) -> None:
        """Gracefully closes all rooms and the WebSocket connection."""
        self._manual_close = True
        self._running = False
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        for r in list(self._rooms.values()):
            r.force_close(is_disconnect=False)
        self._rooms.clear()


class RoomerContext:
    """Async context manager wrapper for Roomer."""

    def __init__(self, url: str, **kwargs: Any) -> None:
        self.client = RoomerClient(url, **kwargs)

    async def __aenter__(self) -> Room:
        await self.client.connect()
        return self.client.root

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.client.close()


def roomer(url: str, **kwargs: Any) -> RoomerContext:
    """
    Initializes a Roomer client instance. Supports async context manager:
    async with roomer("ws://localhost:8080/ws") as root:
        lobby = root.join("lobby")
    """
    return RoomerContext(url, **kwargs)
