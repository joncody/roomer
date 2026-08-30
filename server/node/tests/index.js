/**
 * @fileoverview Comprehensive unit, integration, and stress test suite for the
 * Roomer Node.js server implementation.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import Redis from "ioredis";
import {
    create_message,
    decode_message,
    create_hub,
    create_conn,
    create_local_adapter,
    create_redis_adapter,
    create_in_memory_metrics,
    encode_envelope,
    decode_envelope,
    BACKPRESSURE
} from "../index.js";

function create_mock_ws() {
    return {
        bufferedAmount: 0,
        close: function () {},
        on: function () {},
        ping: function () {},
        readyState: 1,
        send: function (_data, _opts, cb) {
            if (typeof cb === "function") {
                cb();
            }
        }
    };
}

// -----------------------------------------------------------------------------
// 1. Wire Framing, Serialization & Malformed Packet Fuzzing
// -----------------------------------------------------------------------------

test("Message framing: Standard string payload roundtrip", function () {
    const original = create_message("lobby", "chat", "client_dst", "client_src", "Hello World!");
    const raw = original.encode();

    const decoded = decode_message(raw);
    assert.ok(decoded !== null);
    assert.equal(decoded.room, "lobby");
    assert.equal(decoded.event, "chat");
    assert.equal(decoded.dst, "client_dst");
    assert.equal(decoded.src, "client_src");
    assert.equal(decoded.payloadString(), "Hello World!");
});

test("Message framing: Raw Buffer & Uint8Array binary payloads", function () {
    const bin_payload = Buffer.from([0x00, 0xff, 0xca, 0xfe, 0xba, 0xbe]);
    const original = create_message("games", "state", "", "server", bin_payload);
    const raw = original.encode();

    const decoded = decode_message(raw);
    assert.ok(decoded !== null);
    assert.deepEqual(decoded.payload, bin_payload);
});

test("Message framing: JSON object serialization helpers", function () {
    const data = { score: 100, tags: ["pro", "active"], meta: { level: 5 } };
    const original = create_message("leaderboard", "update", "", "bot", data);
    const raw = original.encode();

    const decoded = decode_message(raw);
    assert.ok(decoded !== null);
    assert.deepEqual(decoded.payloadJSON(), data);
});

test("Message framing: Empty strings and zero-byte payload", function () {
    const original = create_message("", "", "", "", "");
    const raw = original.encode();

    assert.equal(raw.length, 20, "Empty packet must be exactly 20 header bytes");
    const decoded = decode_message(raw);
    assert.ok(decoded !== null);
    assert.equal(decoded.room, "");
    assert.equal(decoded.event, "");
    assert.equal(decoded.dst, "");
    assert.equal(decoded.src, "");
    assert.equal(decoded.payload.length, 0);
});

test("Message framing: Rejection of malformed / truncated inputs", function () {
    // Underflow (< 20 bytes)
    assert.equal(decode_message(Buffer.from([0, 0, 0, 5])), null);
    assert.equal(decode_message(Buffer.alloc(19)), null);

    // Truncated room length header (claims 50 bytes on a 25 byte buffer)
    const truncated_room = Buffer.alloc(25);
    truncated_room.writeUInt32BE(50, 0);
    assert.equal(decode_message(truncated_room), null);

    // Truncated payload length header (claims 100 bytes, buffer only has 20)
    const truncated_payload = Buffer.alloc(20);
    truncated_payload.writeUInt32BE(100, 16);
    assert.equal(decode_message(truncated_payload), null);

    // Trailing bytes beyond declared length
    const valid = create_message("r", "e", "", "", "p").encode();
    const with_trailing = Buffer.concat([valid, Buffer.from([1, 2, 3])]);
    assert.equal(decode_message(with_trailing), null);
});

// -----------------------------------------------------------------------------
// 2. Hub Room Lifecycle & Garbage Collection
// -----------------------------------------------------------------------------

test("Hub: Atomic join, leave, presence tracking, and empty room cleanup", async function () {
    const hub = create_hub();
    const c1 = create_conn("user_1", create_mock_ws(), hub);
    const c2 = create_conn("user_2", create_mock_ws(), hub);

    hub.add_conn(c1);
    hub.add_conn(c2);

    await hub.join_room("lobby", c1);
    await hub.join_room("lobby", c2);

    const room = hub.get_room("lobby");
    assert.ok(room !== undefined);
    assert.equal(room.len(), 2);

    const presence = await hub.get_cluster_presence("lobby");
    assert.equal(presence.length, 2);
    assert.ok(presence.includes("user_1"));
    assert.ok(presence.includes("user_2"));

    // User 1 leaves -> room remains with user 2
    hub.leave_room("lobby", c1);
    assert.equal(room.len(), 1);
    assert.equal(hub.get_room("lobby") !== undefined, true);

    // User 2 leaves -> room is garbage-collected
    hub.leave_room("lobby", c2);
    assert.equal(hub.get_room("lobby"), undefined);
});

test("Hub: Reserved event registration & duplicate handler guards", function () {
    const hub = create_hub();

    assert.throws(function () {
        hub.register_handler("join", function () {});
    }, /Cannot register handler for reserved event/);

    assert.throws(function () {
        hub.register_handler("leave_ack", function () {});
    }, /Cannot register handler for reserved event/);

    assert.doesNotThrow(function () {
        hub.register_handler("custom_action", function () {});
    });

    assert.throws(function () {
        hub.register_handler("custom_action", function () {});
    }, /Handler already registered for event/);
});

// -----------------------------------------------------------------------------
// 3. Local Adapter Presence & Node Registry
// -----------------------------------------------------------------------------

test("LocalAdapter: In-memory presence and node registry contract", async function () {
    const adapter = create_local_adapter("test-node-1");
    assert.equal(adapter.node_id(), "test-node-1");

    await adapter.add_presence("channel_a", "client_100");
    await adapter.add_presence("channel_a", "client_200");

    let presence = await adapter.get_presence("channel_a");
    assert.equal(presence.length, 2);
    assert.ok(presence.includes("client_100"));
    assert.ok(presence.includes("client_200"));

    await adapter.remove_presence("channel_a", "client_100");
    presence = await adapter.get_presence("channel_a");
    assert.equal(presence.length, 1);
    assert.equal(presence[0], "client_200");

    // Node registry
    await adapter.register_node("client_200");
    const node = await adapter.get_node_for_conn("client_200");
    assert.equal(node, "test-node-1");

    await adapter.unregister_node("client_200");
    assert.equal(await adapter.get_node_for_conn("client_200"), null);
});

// -----------------------------------------------------------------------------
// 4. Redis Envelope Encoding & Loopback Suppression
// -----------------------------------------------------------------------------

test("Redis Adapter: Envelope encoding and loopback decoding", function () {
    const original_msg = create_message("lobby", "chat", "", "client_1", "cluster message");
    const raw = original_msg.encode();

    const envelope = encode_envelope("node-alpha-123", raw);
    const decoded = decode_envelope(envelope);

    assert.ok(decoded !== null);
    assert.equal(decoded.sender_node_id, "node-alpha-123");

    const unpacked_msg = decode_message(decoded.raw_msg);
    assert.ok(unpacked_msg !== null);
    assert.equal(unpacked_msg.room, "lobby");
    assert.equal(unpacked_msg.payloadString(), "cluster message");
});

test("Redis Adapter: Malformed envelope detection", function () {
    assert.equal(decode_envelope(Buffer.from([0, 0])), null);

    const invalid_len = Buffer.alloc(10);
    invalid_len.writeUInt32BE(50, 0);
    assert.equal(decode_envelope(invalid_len), null);
});

// -----------------------------------------------------------------------------
// 5. Custom Pluggable Adapter Conformance Test
// -----------------------------------------------------------------------------

test("Custom Adapter: User-provided mock adapter integrates seamlessly", async function () {
    const published_channels = [];
    const published_messages = [];

    // Define a custom user adapter conforming to the specification
    const custom_adapter = Object.freeze({
        add_presence: async function () {},
        close: async function () {},
        get_node_for_conn: async function () { return "custom-node-id"; },
        get_presence: async function () { return ["user_mock_1", "user_mock_2"]; },
        node_id: function () { return "custom-node-id"; },
        publish: async function (room, msg) {
            published_channels.push(room);
            published_messages.push(msg);
        },
        publish_direct: async function () {},
        publish_direct_raw: async function () {},
        publish_raw: async function (room, raw) {
            published_channels.push(room);
            published_messages.push(decode_message(raw));
        },
        register_node: async function () {},
        remove_presence: async function () {},
        subscribe: async function () {},
        unregister_node: async function () {}
    });

    const hub = create_hub({ adapter: custom_adapter });
    const conn = create_conn("c1", create_mock_ws(), hub);

    hub.add_conn(conn);
    await hub.join_room("news", conn);

    // Broadcast triggers custom adapter publish
    const msg = create_message("news", "headline", "", "c1", "Breaking news");
    hub.broadcast_room("c1", msg);

    assert.ok(published_channels.includes("news"));

    // Find the headline message (published after the initial new_member presence event)
    const headline_msg = published_messages.find(function (m) {
        return m !== null && m.event === "headline";
    });
    assert.ok(headline_msg !== undefined, "Headline message should be published to custom adapter");
    assert.equal(headline_msg.payloadString(), "Breaking news");

    // Presence is the union of local member c1 and remote cluster presence (user_mock_1, user_mock_2)
    const presence = await hub.get_cluster_presence("news");
    assert.equal(presence.length, 3);
    assert.ok(presence.includes("c1"));
    assert.ok(presence.includes("user_mock_1"));
    assert.ok(presence.includes("user_mock_2"));
});

// -----------------------------------------------------------------------------
// 6. High-Concurrency Simulation
// -----------------------------------------------------------------------------

test("Concurrency: 50 concurrent connections joining, messaging, and leaving", async function () {
    const metrics = create_in_memory_metrics();
    const hub = create_hub({ metrics });

    const total_conns = 50;
    const conns = [];

    for (let i = 0; i < total_conns; i += 1) {
        const id = "conn_" + i;
        const c = create_conn(id, create_mock_ws(), hub, {}, 2048, BACKPRESSURE.DROP_SLOW_CLIENT);
        conns.push(c);
        hub.add_conn(c);
    }

    assert.equal(metrics.getStats().active_connections, total_conns);

    // Concurrently join 5 different rooms
    await Promise.all(conns.map(function (c, idx) {
        const room_name = "room_" + (idx % 5);
        return hub.join_room(room_name, c);
    }));

    assert.equal(metrics.getStats().active_rooms, 5);

    // Concurrently broadcast
    conns.forEach(function (c, idx) {
        const room_name = "room_" + (idx % 5);
        const msg = create_message(room_name, "chat", "", c.id, "ping " + idx);
        hub.broadcast_room(c.id, msg);
    });

    // Concurrently leave all rooms and clean up
    conns.forEach(function (c) {
        hub.leave_all_rooms(c);
        hub.remove_conn(c.id);
        c.cleanup();
    });

    assert.equal(metrics.getStats().active_connections, 0);
    assert.equal(metrics.getStats().active_rooms, 0);
});

// -----------------------------------------------------------------------------
// 7. Live Redis Adapter Cluster Sync, Suppression & Presence Integration Test
// -----------------------------------------------------------------------------

test("Live Redis: Two-node cluster synchronization, loopback suppression, and presence", async function (t) {
    const redis_addr = process.env.REDIS_ADDR || "localhost:6379";
    let redis_url = redis_addr;
    if (
        redis_url.startsWith("redis://") === false &&
        redis_url.startsWith("rediss://") === false
    ) {
        redis_url = "redis://" + redis_url;
    }

    const pub_a = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 1, connectTimeout: 1000 });
    const sub_a = pub_a.duplicate();
    const pub_b = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 1, connectTimeout: 1000 });
    const sub_b = pub_b.duplicate();

    try {
        await pub_a.connect();
        await sub_a.connect();
        await pub_b.connect();
        await sub_b.connect();
    } catch (err) {
        t.skip("Skipping live Redis integration test: Redis not reachable at " + redis_addr);
        pub_a.disconnect();
        sub_a.disconnect();
        pub_b.disconnect();
        sub_b.disconnect();
        return;
    }

    const prefix = "roomer:test:" + Date.now() + ":";
    const node_a = create_redis_adapter(pub_a, sub_a, { node_id: "server_node_A", prefix });
    const node_b = create_redis_adapter(pub_b, sub_b, { node_id: "server_node_B", prefix });

    let node_a_received = 0;
    let node_b_received = 0;

    await node_a.subscribe(function (_channel, _sender, _raw) {
        node_a_received += 1;
    });

    await node_b.subscribe(function (_channel, _sender, _raw) {
        node_b_received += 1;
    });

    // 1. Verify Cluster Presence Synchronization
    await node_a.add_presence("lobby", "client_on_A");
    await node_b.add_presence("lobby", "client_on_B");

    const presence = await node_a.get_presence("lobby");
    assert.equal(presence.length, 2, "Presence set must contain members across all nodes");
    assert.ok(presence.includes("client_on_A"));
    assert.ok(presence.includes("client_on_B"));

    // 2. Verify Node Registry & Targeted Unicast Routing
    await node_b.register_node("client_on_B");
    const target_node = await node_a.get_node_for_conn("client_on_B");
    assert.equal(target_node, "server_node_B");

    // 3. Verify Broadcast Delivery and Loopback Suppression
    const total_messages = 50;
    for (let i = 0; i < total_messages; i += 1) {
        const msg = create_message("lobby", "chat", "", "client_on_A", "msg_" + i);
        await node_a.publish("lobby", msg);
    }

    // Wait up to 3 seconds for messages to arrive at Node B
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline) {
        if (node_b_received >= total_messages) {
            break;
        }
        await new Promise(function (resolve) {
            setTimeout(resolve, 20);
        });
    }

    assert.equal(node_b_received, total_messages, "Node B must receive all broadcast messages");
    assert.equal(node_a_received, 0, "Node A must receive 0 messages (loopback suppressed)");

    // 4. Cleanup Presence and Close Connections
    await node_a.remove_presence("lobby", "client_on_A");
    await node_b.remove_presence("lobby", "client_on_B");
    await node_b.unregister_node("client_on_B");

    await node_a.close();
    await node_b.close();
});
