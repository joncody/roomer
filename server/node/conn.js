/**
 * @fileoverview WebSocket connection handle with token-bucket control-plane rate
 * limiting, native kernel backpressure monitoring, and zero-delay binary dispatching.
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
 * @param {number} [control_rate_limit=10] - Token-bucket refill rate for control events (tokens/sec).
 * @param {number} [control_burst=20] - Token-bucket max burst capacity for control events.
 * @returns {Readonly<object>} Frozen connection instance.
 */
function create_conn(id, ws, hub, claims, capacity, backpressure, control_rate_limit, control_burst) {
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

    const control_rate = (
        typeof control_rate_limit === "number" && control_rate_limit > 0
        ? control_rate_limit
        : 10.0
    );

    const control_burst_val = (
        typeof control_burst === "number" && control_burst > 0
        ? control_burst
        : 20
    );

    const rooms = Object.create(null);
    let is_closed = false;
    let close_sent = false;
    let is_alive = true;
    let control_tokens = control_burst_val;
    let last_control_check = Date.now();
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

    /**
     * Evaluates token-bucket rate limiter for control-plane requests (join/leave).
     *
     * @returns {boolean} True if event is allowed, false if rate limited.
     */
    function allow_control_event() {
        const now = Date.now();
        const elapsed_sec = (now - last_control_check) / 1000;
        last_control_check = now;

        control_tokens += elapsed_sec * control_rate;
        if (control_tokens > control_burst_val) {
            control_tokens = control_burst_val;
        }

        if (control_tokens >= 1.0) {
            control_tokens -= 1.0;
            return true;
        }

        return false;
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

    function cleanup() {
        if (is_closed === true) {
            return;
        }
        is_closed = true;
        hub.leave_all_rooms(self);
        hub.remove_conn(id);
        if (
            close_sent === false &&
            ws !== null &&
            typeof ws === "object" &&
            (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) &&
            typeof ws.close === "function"
        ) {
            close_sent = true;
            try {
                ws.close();
            } catch (ignore) {}
        }
    }

    function close_with(code, reason) {
        if (is_closed === true || close_sent === true) {
            return;
        }
        close_sent = true;
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
                const status_code = (typeof code === "number" && code >= 1000 ? code : 1000);
                const reason_str = (typeof reason === "string" ? reason : "");
                ws.close(status_code, reason_str);
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

            if (strategy === BACKPRESSURE.DROP_NEWEST || strategy === BACKPRESSURE.DROP_OLDEST) {
                return false;
            }

            // DropSlowClient: send RFC 6455 1008 Policy Violation close frame
            close_with(1008, "Slow client buffer overflow");
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
            close_with(1000, "Heartbeat timeout");
            return;
        }
        is_alive = false;
        if (ws !== null && ws.readyState === WebSocket.OPEN && typeof ws.ping === "function") {
            ws.ping();
        }
    }

    self = Object.freeze({
        allow_control_event,
        check_heartbeat,
        claims: Object.freeze(conn_claims),
        cleanup,
        close_with,
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
