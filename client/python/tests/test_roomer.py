"""
Comprehensive test suite for the Roomer Python client.
Covers wire framing, malformed input rejection, event emissions,
room state machines, and async mock transports.
"""

import json
import pytest
from roomer import Packet, Room, RoomerClient, decode_message, encode_message, EventEmitter, roomer


# ------------------------------------------------------------------------------
# 1. Wire Protocol & Binary Framing Tests
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
        payload=payload_bin
    )

    decoded = decode_message(raw)
    assert decoded is not None
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
    assert len(raw) == 20, "Empty packet must be exactly 20 header bytes"

    decoded = decode_message(raw)
    assert decoded is not None
    assert decoded.room == ""
    assert decoded.event == ""
    assert decoded.dst == ""
    assert decoded.src == ""
    assert decoded.payload == b""


def test_protocol_malformed_packets():
    assert decode_message(b"") is None
    assert decode_message(b"\x00\x00\x00\x05") is None

    # Length specifies 50 bytes, buffer only has 25
    truncated = bytearray(25)
    truncated[0:4] = (50).to_bytes(4, "big")
    assert decode_message(truncated) is None

    # Trailing unconsumed bytes
    valid = encode_message("r", "e", "", "", "p")
    assert decode_message(valid + b"\x01\x02\x03") is None


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
# 3. Room State Machine Tests
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
        payload=b'["client_uuid_12345", "other_user_67890"]'
    )
    root.parse(join_ack)

    assert root.is_open
    assert root.id == "client_uuid_12345"
    assert root.members() == ["client_uuid_12345", "other_user_67890"]


def test_room_member_presence_events():
    rooms = {}
    root = Room("root", lambda *args: None, lambda n: rooms.setdefault(n, Room(n, lambda *a: None, lambda n2: None, lambda: True)), lambda: True)
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


def test_reserved_event_guard():
    root = Room("root", lambda *args: None, lambda n: None, lambda: True)
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
