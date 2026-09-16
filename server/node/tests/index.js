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
    BACKPRESSURE,
    HEADER_OVERHEAD,
    PROTOCOL_VERSION
} from "../index.js";

function create_mock_ws() {
    let closed_code = null;
    let closed_reason = null;
    let close_call_count = 0;
    let ready_state = 1;

    const mock = {
        bufferedAmount: 0,
        close: function (code, reason) {
            close_call_count += 1;
            closed_code = code;
            closed_reason = reason;
            ready_state = 3;
        },
        get_close_info: function () {
            return { code: closed_code, count: close_call_count, reason: closed_reason };
        },
        on: function () {},
        ping: function () {},
        get readyState() {
            return ready_state;
        },
        set readyState(val) {
            ready_state = val;
        },
        send: function (_data, _opts, cb) {
            if (typeof cb === "function") {
                cb();
            }
        }
    };
    return mock;
}

// -----------------------------------------------------------------------------
// 1. 12-Byte Wire Framing, Serialization & Malformed Packet Fuzzing
// -----------------------------------------------------------------------------

test("Message framing: Standard string payload roundtrip", function () {
    const original = create_message("lobby", "chat", "client_dst", "client_src", "Hello World!");
    const raw = original.encode();

    const decoded = decode_message(raw);
    assert.ok(decoded !== null);
    assert.equal(decoded.version, PROTOCOL_VERSION);
    assert.equal(decoded.flags, 0);
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
    assert.equal(decoded.version, PROTOCOL_VERSION);
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

    assert.equal(raw.length, HEADER_OVERHEAD, "Empty packet must be exactly 12 header bytes");
    const decoded = decode_message(raw);
    assert.ok(decoded !== null);
    assert.equal(decoded.version, PROTOCOL_VERSION);
    assert.equal(decoded.flags, 0);
    assert.equal(decoded.room, "");
    assert.equal(decoded.event, "");
    assert.equal(decoded.dst, "");
    assert.equal(decoded.src, "");
    assert.equal(decoded.payload.length, 0);
});

test("Message framing: Rejection of malformed / truncated inputs", function () {
    assert.equal(decode_message(Buffer.from([1, 0, 0, 5])), null);
    assert.equal(decode_message(Buffer.alloc(11)), null);

    const truncated_room = Buffer.alloc(15);
    truncated_room.writeUInt8(1, 0);
    truncated_room.writeUInt8(0, 1);
    truncated_room.writeUInt16BE(50, 2); // claims 50 bytes for room, but buffer only has 15
    assert.equal(decode_message(truncated_room), null);

    const valid = create_message("r", "e", "", "", "p").encode();
    const with_trailing = Buffer.concat([valid, Buffer.from([1, 2, 3])]);
    assert.equal(decode_message(with_trailing), null);

    // Max message size guard enforcement
    assert.equal(decode_message(valid, 5), null, "Should reject frame exceeding max_message_size");
});

test("Message framing: Length prefix overflow boundary checks", function () {
    assert.throws(function () {
        create_message("a".repeat(65536), "event", "", "", "p").encode();
    }, /Room name exceeds uint16 maximum length/);

    assert.throws(function () {
        create_message("room", "a".repeat(65536), "", "", "p").encode();
    }, /Event name exceeds uint16 maximum length/);

    assert.throws(function () {
        create_message("room", "event", "a".repeat(256), "", "p").encode();
    }, /Destination ID exceeds uint8 maximum length/);

    assert.throws(function () {
        create_message("room", "event", "", "a".repeat(256), "p").encode();
    }, /Source ID exceeds uint8 maximum length/);
});

// -----------------------------------------------------------------------------
// 2. Hub Room Lifecycle, Rate Limiter & Explicit CloseWith Disconnect
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

    hub.leave_room("lobby", c1);
    assert.equal(room.len(), 1);
    assert.equal(hub.get_room("lobby") !== undefined, true);

    hub.leave_room("lobby", c2);
    assert.equal(hub.get_room("lobby"), undefined);
});

test("Hub: Token-bucket control-plane rate limiting for join/leave", function () {
    const hub = create_hub();
    const conn = create_conn("limited_user", create_mock_ws(), hub, {}, 2048, BACKPRESSURE.DROP_SLOW_CLIENT, 5.0, 3);

    // 3 immediate events consume burst
    assert.equal(conn.allow_control_event(), true);
    assert.equal(conn.allow_control_event(), true);
    assert.equal(conn.allow_control_event(), true);

    // 4th event must be rate limited
    assert.equal(conn.allow_control_event(), false);
});

test("Hub: Explicit close_with and hub.disconnect with status codes and no duplicate close calls", function () {
    const hub = create_hub();
    const mock_ws = create_mock_ws();
    const conn = create_conn("disconnect_user", mock_ws, hub);

    hub.add_conn(conn);
    assert.ok(hub.get_conn("disconnect_user") !== undefined);

    const ok = hub.disconnect("disconnect_user", 4001, "Authentication failed");
    assert.equal(ok, true);

    const close_info = mock_ws.get_close_info();
    assert.equal(close_info.code, 4001);
    assert.equal(close_info.reason, "Authentication failed");
    assert.equal(close_info.count, 1, "ws.close must be called exactly once");
    assert.equal(hub.get_conn("disconnect_user"), undefined);

    // Subsequent close/disconnect attempts must NOT trigger a second close message
    conn.close_with(4002, "Duplicate disconnect");
    conn.cleanup();
    assert.equal(mock_ws.get_close_info().count, 1, "ws.close must not be called a second time");
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
    assert.equal(unpacked_msg.version, PROTOCOL_VERSION);
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
// 5. Custom Pluggable Adapter Conformance & Cluster Metrics
// -----------------------------------------------------------------------------

test("Custom Adapter: User-provided mock adapter integrates seamlessly", async function () {
    const published_channels = [];
    const published_messages = [];

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

    const msg = create_message("news", "headline", "", "c1", "Breaking news");
    hub.broadcast_room("c1", msg);

    assert.ok(published_channels.includes("news"));

    const headline_msg = published_messages.find(function (m) {
        return m !== null && m.event === "headline";
    });
    assert.ok(headline_msg !== undefined);
    assert.equal(headline_msg.payloadString(), "Breaking news");

    const presence = await hub.get_cluster_presence("news");
    assert.equal(presence.length, 3);
    assert.ok(presence.includes("c1"));
    assert.ok(presence.includes("user_mock_1"));
    assert.ok(presence.includes("user_mock_2"));
});

test("Hub: Cluster metrics tracking for publish and receive", async function () {
    const metrics = create_in_memory_metrics();
    let sub_cb = null;

    const mock_adapter = Object.freeze({
        add_presence: async function () {},
        close: async function () {},
        get_node_for_conn: async function () { return null; },
        get_presence: async function () { return []; },
        node_id: function () { return "mock-node"; },
        publish: async function () {},
        publish_direct: async function () {},
        publish_direct_raw: async function () {},
        publish_raw: async function () {},
        register_node: async function () {},
        remove_presence: async function () {},
        subscribe: async function (cb) { sub_cb = cb; },
        unregister_node: async function () {}
    });

    const hub = create_hub({ adapter: mock_adapter, metrics });
    await hub.configure(mock_adapter, metrics);

    const msg = create_message("news", "headline", "", "sender", "Cluster payload");
    hub.broadcast_room(null, msg);

    await new Promise(function (resolve) {
        setTimeout(resolve, 50);
    });

    const stats1 = metrics.getStats();
    assert.equal(stats1.cluster_published, 1);
    assert.ok(stats1.bytes_cluster_published > 0);

    const raw = msg.encode();
    sub_cb("room:news", "other_node", raw);

    const stats2 = metrics.getStats();
    assert.equal(stats2.cluster_received, 1);
    assert.equal(stats2.bytes_cluster_received, raw.length);
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

    await Promise.all(conns.map(function (c, idx) {
        const room_name = "room_" + (idx % 5);
        return hub.join_room(room_name, c);
    }));

    assert.equal(metrics.getStats().active_rooms, 5);

    conns.forEach(function (c, idx) {
        const room_name = "room_" + (idx % 5);
        const msg = create_message(room_name, "chat", "", c.id, "ping " + idx);
        hub.broadcast_room(c.id, msg);
    });

    conns.forEach(function (c) {
        hub.leave_all_rooms(c);
        hub.remove_conn(c.id);
        c.cleanup();
    });

    assert.equal(metrics.getStats().active_connections, 0);
    assert.equal(metrics.getStats().active_rooms, 0);
});

// -----------------------------------------------------------------------------
// 7. Live Redis Adapter Cluster Sync, Suppression & SET Presence Test
// -----------------------------------------------------------------------------

test("Live Redis: Two-node cluster synchronization, loopback suppression, and SET presence", async function (t) {
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
    const node_a = create_redis_adapter(pub_a, sub_a, { node_id: "server_node_A", prefix, presence_ttl: 180 });
    const node_b = create_redis_adapter(pub_b, sub_b, { node_id: "server_node_B", prefix, presence_ttl: 180 });

    let node_a_received = 0;
    let node_b_received = 0;

    await node_a.subscribe(function (_channel, _sender, _raw) {
        node_a_received += 1;
    });

    await node_b.subscribe(function (_channel, _sender, _raw) {
        node_b_received += 1;
    });

    // 1. Verify Cluster SET Presence Synchronization and Key Expiration
    await node_a.add_presence("lobby", "client_on_A");
    await node_b.add_presence("lobby", "client_on_B");

    let presence = await node_a.get_presence("lobby");
    assert.equal(presence.length, 2, "Presence set must contain members across all nodes");
    assert.ok(presence.includes("client_on_A"));
    assert.ok(presence.includes("client_on_B"));

    // Verify Redis key expiration TTL is active
    const presence_key = prefix + "presence:lobby";
    const key_ttl = await pub_a.ttl(presence_key);
    assert.ok(key_ttl > 0 && key_ttl <= 180, "Presence key must have active expiration TTL");

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

test("Live Redis: True wire-level unicast isolation across 3 nodes", async function (t) {
    const redis_addr = process.env.REDIS_ADDR || "localhost:6379";
    let redis_url = redis_addr;
    if (
        redis_url.startsWith("redis://") === false &&
        redis_url.startsWith("rediss://") === false
    ) {
        redis_url = "redis://" + redis_url;
    }

    const pub_s = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 1, connectTimeout: 1000 });
    const sub_s = pub_s.duplicate();
    const pub_t = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 1, connectTimeout: 1000 });
    const sub_t = pub_t.duplicate();
    const pub_b = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 1, connectTimeout: 1000 });
    const sub_b = pub_b.duplicate();

    try {
        await Promise.all([
            pub_s.connect(), sub_s.connect(),
            pub_t.connect(), sub_t.connect(),
            pub_b.connect(), sub_b.connect()
        ]);
    } catch (err) {
        t.skip("Skipping live Redis unicast test: Redis not reachable at " + redis_addr);
        pub_s.disconnect(); sub_s.disconnect();
        pub_t.disconnect(); sub_t.disconnect();
        pub_b.disconnect(); sub_b.disconnect();
        return;
    }

    const prefix = "roomer:unicast:" + Date.now() + ":";
    const node_sender = create_redis_adapter(pub_s, sub_s, { node_id: "node_sender", prefix });
    const node_target = create_redis_adapter(pub_t, sub_t, { node_id: "node_target", prefix });
    const node_bystander = create_redis_adapter(pub_b, sub_b, { node_id: "node_bystander", prefix });

    let target_received = 0;
    let bystander_received = 0;

    await node_target.subscribe(function () {
        target_received += 1;
    });

    await node_bystander.subscribe(function () {
        bystander_received += 1;
    });

    await new Promise(function (resolve) {
        setTimeout(resolve, 100);
    });

    const dm = create_message("root", "dm", "client_target", "client_sender", "wire_secret");
    await node_sender.publish_direct("node_target", dm);

    const deadline = Date.now() + 2000;
    while (Date.now() < deadline) {
        if (target_received >= 1) {
            break;
        }
        await new Promise(function (resolve) {
            setTimeout(resolve, 20);
        });
    }

    assert.equal(target_received, 1, "Target node must receive the direct unicast message");
    assert.equal(bystander_received, 0, "Bystander node must NOT receive unicast traffic over the wire");

    await node_sender.close();
    await node_target.close();
    await node_bystander.close();
});
