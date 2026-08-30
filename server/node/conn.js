/**
 * @fileoverview WebSocket connection handle with high-throughput native
 * kernel backpressure monitoring and zero-delay binary dispatching.
 */

import { WebSocket } from "ws";
import { create_message } from "./message.js";

const BACKPRESSURE = Object.freeze({
    DROP_NEWEST: 2,
    DROP_OLDEST: 1,
    DROP_SLOW_CLIENT: 0
});

/**
 * Creates an active WebSocket connection wrapper.
 *
 * @param {string} id - Connection UUID.
 * @param {WebSocket|object} ws - Active ws socket instance.
 * @param {object} hub - Owning hub coordinator.
 * @param {object} [claims] - Authenticated handshake claims.
 * @param {number} [capacity=8192] - Max queued frames buffer ceiling.
 * @param {number} [backpressure=0] - Backpressure strategy enum.
 * @returns {Readonly<object>} Frozen connection instance.
 */
function create_conn(id, ws, hub, claims, capacity, backpressure) {
    const conn_claims = (
        typeof claims === "object" && claims !== null
        ? claims
        : Object.create(null)
    );

    // 32MB maximum buffer ceiling before slow-client eviction
    const max_buffer_bytes = (
        typeof capacity === "number" && capacity > 0
        ? capacity * 4096
        : 32 * 1024 * 1024
    );

    const strategy = (
        typeof backpressure === "number"
        ? backpressure
        : BACKPRESSURE.DROP_SLOW_CLIENT
    );

    const rooms = Object.create(null);
    let is_closed = false;
    let is_alive = true;
    let self;

    // Disable Nagle's algorithm for sub-millisecond real-time frame delivery
    if (
        ws !== null &&
        typeof ws === "object" &&
        ws._socket !== undefined &&
        typeof ws._socket.setNoDelay === "function"
    ) {
        ws._socket.setNoDelay(true);
    }

    // Heartbeat pong tracking
    if (
        ws !== null &&
        typeof ws === "object" &&
        typeof ws.on === "function"
    ) {
        ws.on("pong", function () {
            is_alive = true;
        });
    }

    function track_room(room) {
        rooms[room] = true;
    }

    function untrack_room(room) {
        delete rooms[room];
    }

    function is_in_room(room) {
        return rooms[room] === true;
    }

    function joined_rooms() {
        return Object.keys(rooms);
    }

    function cleanup() {
        if (is_closed === true) {
            return;
        }
        is_closed = true;
        hub.leave_all_rooms(self);
        hub.remove_conn(id);
        if (
            ws !== null &&
            typeof ws === "object" &&
            (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) &&
            typeof ws.close === "function"
        ) {
            try {
                ws.close();
            } catch (ignore) {}
        }
    }

    function try_send(msg) {
        if (is_closed === true || ws === null || ws.readyState !== WebSocket.OPEN) {
            return false;
        }

        // Native libuv backpressure monitoring
        if (typeof ws.bufferedAmount === "number" && ws.bufferedAmount > max_buffer_bytes) {
            hub.metrics.onMessageDropped();

            if (strategy === BACKPRESSURE.DROP_NEWEST) {
                return false;
            }

            // DropSlowClient
            cleanup();
            return false;
        }

        if (typeof ws.send === "function") {
            ws.send(msg, { binary: true });
        }

        hub.metrics.onMessageSent(msg.length);
        return true;
    }

    function send_to_room(room, event, payload) {
        const msg = create_message(room, event, "", id, payload);
        hub.broadcast_room(id, msg);
    }

    function send_to_client(dst_id, event, payload) {
        const msg = create_message("root", event, dst_id, id, payload);
        const target = hub.get_conn(dst_id);
        if (target !== undefined) {
            target.try_send(msg.encode());
        } else {
            hub.send_direct_to_cluster(msg);
        }
    }

    function check_heartbeat() {
        if (is_alive === false) {
            cleanup();
            return;
        }
        is_alive = false;
        if (ws !== null && ws.readyState === WebSocket.OPEN && typeof ws.ping === "function") {
            ws.ping();
        }
    }

    self = Object.freeze({
        check_heartbeat,
        claims: Object.freeze(conn_claims),
        cleanup,
        id,
        is_in_room,
        joined_rooms,
        send_to_client,
        send_to_room,
        track_room,
        try_send,
        untrack_room,
        ws
    });

    return self;
}

export {
    BACKPRESSURE,
    create_conn
};
