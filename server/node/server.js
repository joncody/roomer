/**
 * @fileoverview HTTP / WebSocket server mount helper with immediate synchronous
 * listener attachment to prevent event-loop race conditions.
 */

import { WebSocketServer } from "ws";
import { randomUUID } from "node:crypto";
import { create_hub } from "./hub.js";
import { create_conn, BACKPRESSURE } from "./conn.js";
import { create_message, decode_message } from "./message.js";

/**
 * Creates and mounts a Roomer WebSocket server on an HTTP server.
 *
 * @param {object} http_server - Node.js HTTP server.
 * @param {object} [options] - Server options.
 * @returns {Readonly<{ hub: object, wss: WebSocketServer }>} Mounted server handle.
 */
function create_roomer_server(http_server, options) {
    const opts = (
        typeof options === "object" && options !== null
        ? options
        : Object.create(null)
    );

    const hub = (
        opts.hub !== undefined
        ? opts.hub
        : create_hub({
            adapter: opts.adapter,
            metrics: opts.metrics
        })
    );

    const max_payload = (
        typeof opts.max_message_size === "number"
        ? opts.max_message_size
        : 16 * 1024 * 1024
    );

    const capacity = (
        typeof opts.channel_capacity === "number"
        ? opts.channel_capacity
        : 8192
    );

    const backpressure = (
        typeof opts.backpressure === "number"
        ? opts.backpressure
        : BACKPRESSURE.DROP_SLOW_CLIENT
    );

    const ping_interval_ms = (
        typeof opts.ping_interval === "number"
        ? opts.ping_interval
        : 54000
    );

    const wss = new WebSocketServer({
        clientTracking: false,
        maxPayload: max_payload,
        path: "/ws",
        perMessageDeflate: false,
        server: http_server
    });

    const active_connections = Object.create(null);

    const ping_timer = setInterval(function () {
        Object.keys(active_connections).forEach(function (id) {
            active_connections[id].check_heartbeat();
        });
    }, ping_interval_ms);

    if (typeof ping_timer.unref === "function") {
        ping_timer.unref();
    }

    wss.on("connection", function (ws, req) {
        let claims = Object.create(null);
        if (typeof opts.authorize === "function") {
            try {
                const auth_result = opts.authorize(req);
                if (typeof auth_result === "object" && auth_result !== null) {
                    claims = auth_result;
                }
            } catch (ignore) {
                ws.close(1008, "Unauthorized");
                return;
            }
        }

        const conn_id = randomUUID();
        const conn = create_conn(conn_id, ws, hub, claims, capacity, backpressure);

        active_connections[conn_id] = conn;
        hub.add_conn(conn);
        hub.join_room("root", conn);

        // 1. ATTACH LISTENERS SYNCHRONOUSLY FIRST (Eliminates race condition)
        ws.on("message", function (data) {
            const buf = (
                Buffer.isBuffer(data)
                ? data
                : (
                    Array.isArray(data)
                    ? Buffer.concat(data)
                    : Buffer.from(data)
                )
            );

            hub.metrics.onMessageReceived(buf.length);
            const msg = decode_message(buf);
            if (msg !== null) {
                hub.dispatch(conn, msg).catch(function () {});
            }
        });

        ws.on("close", function () {
            delete active_connections[conn_id];
            conn.cleanup();
        });

        ws.on("error", function () {
            delete active_connections[conn_id];
            conn.cleanup();
        });

        // 2. Fetch cluster presence and send root join_ack asynchronously
        hub.get_cluster_presence("root").then(function (snap) {
            const ack = create_message(
                "root",
                "join_ack",
                "",
                conn.id,
                JSON.stringify(snap)
            );
            conn.try_send(ack.encode());
        }).catch(function () {});
    });

    return Object.freeze({
        hub,
        wss
    });
}

export {
    create_roomer_server
};
