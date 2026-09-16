"""
Comprehensive test suite for the Roomer Python client.
Covers 12-byte wire framing, pre-allocated struct packing, malformed
input rejection, event emissions, room state machines, readyState, buffered_amount,
url properties, RFC close codes and reasons, and lifecycle cleanup.
"""

import json
import pytest
from roomer import (
    DEFAULT_FLAGS,
    HEADER_OVERHEAD,
    PROTOCOL_VERSION,
    EventEmitter,
    Packet,
    Room,
    RoomerClient,
    decode_message,
    encode_message,
    roomer,
)


# ------------------------------------------------------------------------------
# 1. Wire Protocol & Binary Framing Tests (12-Byte Overhead Contract)
# ------------------------------------------------------------------------------

def test_protocol_roundtrip_text():
    original = encode_message(
        room="lobby",
        event="chat",
        dst="user_dst",
        src="user_src",
        payload="Hello from Python!"
    )

    decoded = decode_message(original)
    assert decoded is not None
    assert decoded.version == PROTOCOL_VERSION
    assert decoded.flags == 0
    assert decoded.room == "lobby"
    assert decoded.event == "chat"
    assert decoded.dst == "user_dst"
    assert decoded.src == "user_src"
    assert decoded.payload_text() == "Hello from Python!"


def test_protocol_roundtrip_binary():
    payload_bin = bytes([0x00, 0xFF, 0xCA, 0xFE, 0xBA, 0xBE])
    raw = encode_message(
        room="games",
        event="state",
        dst="",
        src="server",
        payload=payload_bin,
        flags=0x07
    )

    decoded = decode_message(raw)
    assert decoded is not None
    assert decoded.version == PROTOCOL_VERSION
    assert decoded.flags == 0x07
    assert decoded.payload == payload_bin


def test_protocol_roundtrip_json():
    data = {"score": 100, "tags": ["pro", "active"], "meta": {"level": 5}}
    raw = encode_message(
        room="leaderboard",
        event="update",
        dst="",
        src="bot",
        payload=data
    )

    decoded = decode_message(raw)
    assert decoded is not None
    assert decoded.payload_json() == data


def test_protocol_empty_fields():
    raw = encode_message()
    assert len(raw) == HEADER_OVERHEAD, "Empty packet must be exactly 12 header bytes"

    decoded = decode_message(raw)
    assert decoded is not None
    assert decoded.version == PROTOCOL_VERSION
    assert decoded.flags == 0
    assert decoded.room == ""
    assert decoded.event == ""
    assert decoded.dst == ""
    assert decoded.src == ""
    assert decoded.payload == b""


def test_protocol_malformed_packets():
    assert decode_message(b"") is None
    assert decode_message(b"\x01\x00\x00\x05") is None

    # Length specifies 50 bytes for room name, buffer only has 15
    truncated = bytearray(15)
    truncated[0] = 1  # version
    truncated[1] = 0  # flags
    truncated[2:4] = (50).to_bytes(2, "big")
    assert decode_message(truncated) is None

    # Trailing unconsumed bytes
    valid = encode_message("r", "e", "", "", "p")
    assert decode_message(valid + b"\x01\x02\x03") is None


def test_protocol_length_prefix_boundaries():
    # Valid maximum lengths
    valid_max_dst = "a" * 255
    raw = encode_message(room="r", event="e", dst=valid_max_dst, src="", payload=b"ok")
    decoded = decode_message(raw)
    assert decoded is not None
    assert decoded.dst == valid_max_dst

    # Exceeding uint8 dst length (> 255)
    with pytest.raises(ValueError, match="Destination ID exceeds maximum uint8 length"):
        encode_message(dst="a" * 256)

    # Exceeding uint8 src length (> 255)
    with pytest.raises(ValueError, match="Source ID exceeds maximum uint8 length"):
        encode_message(src="a" * 256)

    # Exceeding uint16 room length (> 65535)
    with pytest.raises(ValueError, match="Room name exceeds maximum uint16 length"):
        encode_message(room="a" * 65536)

    # Exceeding uint16 event length (> 65535)
    with pytest.raises(ValueError, match="Event name exceeds maximum uint16 length"):
        encode_message(event="a" * 65536)


# ------------------------------------------------------------------------------
# 2. Event Emitter Tests
# ------------------------------------------------------------------------------

def test_emitter_sync_and_once():
    emitter = EventEmitter()
    received = []

    emitter.on("test", lambda x: received.append(f"on:{x}"))
    emitter.once("test", lambda x: received.append(f"once:{x}"))

    emitter.emit("test", 1)
    assert received == ["on:1", "once:1"]

    emitter.emit("test", 2)
    assert received == ["on:1", "once:1", "on:2"]


def test_emitter_off():
    emitter = EventEmitter()
    received = []

    def handler(x):
        received.append(x)

    emitter.on("msg", handler)
    emitter.emit("msg", "hello")
    assert received == ["hello"]

    emitter.off("msg", handler)
    emitter.emit("msg", "world")
    assert received == ["hello"]


# ------------------------------------------------------------------------------
# 3. Room State Machine, Lifecycle Cleanup & State Accessors
# ------------------------------------------------------------------------------

def test_room_join_ack_state_transition():
    rooms = {}

    def mock_get_room(name):
        if name not in rooms:
            rooms[name] = Room(name, lambda *args: None, mock_get_room, lambda: True)
        return rooms[name]

    root = mock_get_room("root")
    assert not root.is_open
    assert root.id == ""

    # Server responds with join_ack
    join_ack = Packet(
        room="root",
        event="join_ack",
        dst="",
        src="client_uuid_12345",
        payload=b'["client_uuid_12345", "other_user_67890"]',
        version=1,
        flags=0
    )
    root.parse(join_ack)

    assert root.is_open
    assert root.id == "client_uuid_12345"
    assert root.members() == ["client_uuid_12345", "other_user_67890"]


def test_room_member_presence_events():
    rooms = {}
    root = Room(
        "root",
        lambda *args: None,
        lambda n: rooms.setdefault(n, Room(n, lambda *a: None, lambda n2: None, lambda: True)),
        lambda: True
    )
    root._is_open = True
    root._member_id = "self_id"
    root._members = ["self_id"]

    new_members = []
    left_members = []

    root.on("new_member", lambda uid: new_members.append(uid))
    root.on("member_left", lambda uid: left_members.append(uid))

    # New member joins
    root.parse(Packet("root", "new_member", "", "", b"user_abc"))
    assert "user_abc" in root.members()
    assert new_members == ["user_abc"]

    # Member leaves
    root.parse(Packet("root", "member_left", "", "", b"user_abc"))
    assert "user_abc" not in root.members()
    assert left_members == ["user_abc"]


def test_room_leave_ack_cleans_up_client_registry():
    client = RoomerClient("ws://localhost:8080/ws", reconnect=False)
    lobby = client.get_room("lobby")
    lobby._is_open = True
    assert "lobby" in client._rooms

    closed = False
    captured_code = None
    captured_reason = None

    @lobby.on("close")
    def on_close(code=None, reason=None):
        nonlocal closed, captured_code, captured_reason
        closed = True
        captured_code = code
        captured_reason = reason

    # Server confirms leave with leave_ack
    lobby.parse(Packet("lobby", "leave_ack", "", "", b""))

    assert closed
    assert captured_code == 1000
    assert captured_reason == "Left room"
    assert not lobby.is_open
    assert "lobby" not in client._rooms

    # Joining lobby again creates a clean new room handle
    rejoined_lobby = client.get_room("lobby")
    assert "lobby" in client._rooms
    assert rejoined_lobby is not lobby


def test_room_force_close_propagates_rfc_code_and_reason():
    room = Room("test_room", lambda *a: None, lambda n: None, lambda: True)
    room._is_open = True

    captured_code = None
    captured_reason = None

    @room.on("close")
    def on_close(code=None, reason=None):
        nonlocal captured_code, captured_reason
        captured_code = code
        captured_reason = reason

    room.force_close(False, code=4001, reason="Authentication failed")

    assert not room.is_open
    assert captured_code == 4001
    assert captured_reason == "Authentication failed"


def test_roomer_client_default_reconnect_suppression():
    # 1. Fatal codes must NOT attempt reconnection
    assert not RoomerClient._default_should_reconnect(1000, "Normal closure")
    assert not RoomerClient._default_should_reconnect(1008, "Policy violation")
    assert not RoomerClient._default_should_reconnect(4001, "Unauthorized")
    assert not RoomerClient._default_should_reconnect(4003, "Forbidden")

    # 2. Abnormal / transient network drops MUST attempt reconnection
    assert RoomerClient._default_should_reconnect(1006, "Abnormal closure")
    assert RoomerClient._default_should_reconnect(1001, "Going away")


def test_reserved_event_guard():
    root = Room(
        "root",
        lambda *args: None,
        lambda n: None,
        lambda: True
    )
    root._is_open = True

    with pytest.raises(ValueError, match="Cannot send reserved event"):
        root.send("join", "invalid")

    with pytest.raises(ValueError, match="Cannot send reserved event"):
        root.send("leave_ack", "invalid")


def test_root_room_special_methods():
    client = RoomerClient("ws://localhost:8080/ws", reconnect=False)
    root = client.root
    assert hasattr(root, "close")
    assert hasattr(root, "purge")
    assert hasattr(root, "rooms")
    assert callable(getattr(root, "close"))
    assert callable(getattr(root, "purge"))
    assert callable(getattr(root, "rooms"))


def test_client_and_room_ready_state_and_buffered_amount_and_url():
    client = RoomerClient("ws://localhost:8080/ws", reconnect=False)
    assert client.ready_state == "closed"
    assert client.buffered_amount == 0
    assert client.url == "ws://localhost:8080/ws"

    root = client.root
    assert root.ready_state == "closed"
    assert root.get_ready_state() == "closed"
    assert root.buffered_amount == 0
    assert root.get_buffered_amount() == 0
    assert root.url == "ws://localhost:8080/ws"
    assert root.get_url() == "ws://localhost:8080/ws"

    lobby = client.get_room("lobby")
    assert lobby.ready_state == "closed"
    assert lobby.get_ready_state() == "closed"
    assert lobby.buffered_amount == 0
    assert lobby.get_buffered_amount() == 0
    assert lobby.url == "ws://localhost:8080/ws"
    assert lobby.get_url() == "ws://localhost:8080/ws"
