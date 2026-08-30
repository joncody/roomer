/**
 * @fileoverview Redis Pub/Sub cluster adapter with loopback suppression,
 * presence synchronization sets, and O(1) unicast direct routing.
 */

import { randomUUID } from "node:crypto";

/**
 * Packs a node identifier and raw packet into a binary envelope.
 *
 * @param {string} node_id - Originating cluster node UUID.
 * @param {Buffer} raw_msg - Serialized message frame.
 * @returns {Buffer} Enveloped byte buffer.
 */
function encode_envelope(node_id, raw_msg) {
    const node_buf = Buffer.from(node_id, "utf-8");
    const env = Buffer.allocUnsafe(4 + node_buf.length + raw_msg.length);
    env.writeUInt32BE(node_buf.length, 0);
    node_buf.copy(env, 4);
    raw_msg.copy(env, 4 + node_buf.length);
    return env;
}

/**
 * Unpacks a node envelope into sender node ID and raw message bytes.
 *
 * @param {Buffer} payload - Enveloped byte buffer.
 * @returns {{ sender_node_id: string, raw_msg: Buffer }|null} Decoded envelope.
 */
function decode_envelope(payload) {
    if (payload.length < 4) {
        return null;
    }
    const node_len = payload.readUInt32BE(0);
    if (payload.length < 4 + node_len) {
        return null;
    }
    const sender_node_id = payload.toString("utf-8", 4, 4 + node_len);
    const raw_msg = payload.subarray(4 + node_len);
    return {
        raw_msg,
        sender_node_id
    };
}

/**
 * Creates a Redis cluster adapter.
 *
 * @param {object} pub_client - Connected ioredis publishing client.
 * @param {object} [sub_client] - Connected ioredis subscriber client.
 * @param {object} [options] - Configuration options.
 * @returns {Readonly<object>} Frozen Redis adapter instance.
 */
function create_redis_adapter(pub_client, sub_client, options) {
    const opts = (
        typeof options === "object" && options !== null
        ? options
        : Object.create(null)
    );

    const active_sub = (
        sub_client !== undefined
        ? sub_client
        : pub_client.duplicate()
    );

    const node_id_val = (
        typeof opts.node_id === "string" && opts.node_id !== ""
        ? opts.node_id
        : randomUUID()
    );

    let prefix_val = (
        typeof opts.prefix === "string" && opts.prefix !== ""
        ? opts.prefix
        : "roomer:demo:"
    );

    if (prefix_val.endsWith(":") === false) {
        prefix_val += ":";
    }

    function node_id() {
        return node_id_val;
    }

    async function publish_raw(room, raw_msg) {
        const channel = prefix_val + room;
        const envelope = encode_envelope(node_id_val, raw_msg);
        // Use pub_client.publish (ioredis natively handles Buffer arguments)
        await pub_client.publish(channel, envelope);
    }

    async function publish(room, msg) {
        await publish_raw(room, msg.encode());
    }

    async function publish_direct_raw(target_node_id, raw_msg) {
        const channel = prefix_val + "node:" + target_node_id;
        const envelope = encode_envelope(node_id_val, raw_msg);
        await pub_client.publish(channel, envelope);
    }

    async function publish_direct(target_node_id, msg) {
        await publish_direct_raw(target_node_id, msg.encode());
    }

    async function add_presence(room, conn_id) {
        const key = prefix_val + "presence:" + room;
        await pub_client.sadd(key, conn_id);
    }

    async function remove_presence(room, conn_id) {
        const key = prefix_val + "presence:" + room;
        await pub_client.srem(key, conn_id);
    }

    async function get_presence(room) {
        const key = prefix_val + "presence:" + room;
        return await pub_client.smembers(key);
    }

    async function register_node(conn_id) {
        const key = prefix_val + "conn_node:" + conn_id;
        await pub_client.set(key, node_id_val, "EX", 86400);
    }

    async function unregister_node(conn_id) {
        const key = prefix_val + "conn_node:" + conn_id;
        await pub_client.del(key);
    }

    async function get_node_for_conn(conn_id) {
        const key = prefix_val + "conn_node:" + conn_id;
        return await pub_client.get(key);
    }

    async function subscribe(callback) {
        const pattern = prefix_val + "*";
        await active_sub.psubscribe(pattern);

        active_sub.on("pmessageBuffer", function (_pat, channel_buf, msg_buf) {
            const channel = channel_buf.toString("utf-8");
            const decoded = decode_envelope(msg_buf);
            if (decoded === null) {
                return;
            }

            // Loopback suppression
            if (decoded.sender_node_id === node_id_val) {
                return;
            }

            const channel_suffix = (
                channel.startsWith(prefix_val) === true
                ? channel.slice(prefix_val.length)
                : channel
            );

            callback(channel_suffix, decoded.sender_node_id, decoded.raw_msg);
        });
    }

    async function close() {
        try {
            await active_sub.quit();
            await pub_client.quit();
        } catch (ignore) {}
    }

    return Object.freeze({
        add_presence,
        close,
        get_node_for_conn,
        get_presence,
        node_id,
        publish,
        publish_direct,
        publish_direct_raw,
        publish_raw,
        register_node,
        remove_presence,
        subscribe,
        unregister_node
    });
}

export {
    create_redis_adapter,
    decode_envelope,
    encode_envelope
};
