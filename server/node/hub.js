/**
 * @fileoverview Central hub coordinator supporting direct adapter injection.
 */

import { create_room } from "./room.js";
import { create_message, decode_message } from "./message.js";
import { create_local_adapter } from "./adapter.js";
import { create_nop_metrics } from "./metrics.js";

const RESERVED_EVENTS = Object.freeze([
    "close",
    "join",
    "join_ack",
    "leave",
    "leave_ack",
    "member_left",
    "new_member",
    "open"
]);

/**
 * Creates a Roomer Hub coordinator.
 *
 * @param {object} [options] - Initial configuration options.
 * @param {object} [options.adapter] - Custom or Redis distributed adapter.
 * @param {object} [options.metrics] - Telemetry collector.
 * @param {(conn: object, room: string) => boolean} [options.authorize_room] - Room subscription auth.
 * @returns {Readonly<object>} Frozen hub coordinator instance.
 */
function create_hub(options) {
    const opts = (
        typeof options === "object" && options !== null
        ? options
        : Object.create(null)
    );

    const conns = Object.create(null);
    const rooms = Object.create(null);
    const handlers = Object.create(null);
    let adapter = (
        opts.adapter !== undefined
        ? opts.adapter
        : create_local_adapter()
    );
    let metrics = (
        opts.metrics !== undefined
        ? opts.metrics
        : create_nop_metrics()
    );
    let self;

    async function configure(new_adapter, new_metrics) {
        if (typeof new_metrics === "object" && new_metrics !== null) {
            metrics = new_metrics;
        }
        if (typeof new_adapter === "object" && new_adapter !== null) {
            adapter = new_adapter;
            await adapter.subscribe(function (channel_suffix, _sender_node, raw_frame) {
                // Targeted unicast direct message
                if (channel_suffix.startsWith("node:") === true || channel_suffix === "root") {
                    const packet = decode_message(raw_frame);
                    if (packet !== null && packet.dst !== "") {
                        const dst_conn = conns[packet.dst];
                        if (dst_conn !== undefined) {
                            dst_conn.try_send(raw_frame);
                        }
                        return;
                    }
                }

                // Local room fanout
                const room = rooms[channel_suffix];
                if (room !== undefined) {
                    room.emit_local(null, raw_frame);
                }
            });
        }
    }

    function register_handler(event, handler) {
        if (RESERVED_EVENTS.includes(event) === true) {
            throw new Error("Cannot register handler for reserved event: '" + event + "'");
        }
        if (handlers[event] !== undefined) {
            throw new Error("Handler already registered for event: '" + event + "'");
        }
        handlers[event] = handler;
    }

    function get_conn(id) {
        return conns[id];
    }

    function add_conn(conn) {
        conns[conn.id] = conn;
        adapter.register_node(conn.id).catch(function () {});
        metrics.onConnect();
    }

    function remove_conn(id) {
        if (conns[id] !== undefined) {
            delete conns[id];
            adapter.unregister_node(id).catch(function () {});
            metrics.onDisconnect();
        }
    }

    function get_room(name) {
        return rooms[name];
    }

    function broadcast_room(exclude_id, msg) {
        const raw = msg.encode();
        const room = rooms[msg.room];
        if (room !== undefined) {
            room.emit_local(exclude_id, raw);
        }
        adapter.publish_raw(msg.room, raw).catch(function (err) {
            console.error("Failed to publish to cluster adapter:", err);
        });
    }

    async function join_room(name, conn) {
        let room = rooms[name];
        if (room === undefined) {
            room = create_room(name);
            rooms[name] = room;
            metrics.onRoomCreated(name);
        }

        conn.track_room(name);
        room.add_member(conn);

        await adapter.add_presence(name, conn.id).catch(function () {});

        const new_member_msg = create_message(name, "new_member", "", "", conn.id);
        broadcast_room(conn.id, new_member_msg);
    }

    function leave_room(name, conn) {
        conn.untrack_room(name);
        const room = rooms[name];
        if (room !== undefined) {
            const is_empty = room.remove_member(conn.id);
            if (is_empty === true) {
                delete rooms[name];
                metrics.onRoomDeleted(name);
            }

            adapter.remove_presence(name, conn.id).catch(function () {});

            const left_msg = create_message(name, "member_left", "", "", conn.id);
            broadcast_room(conn.id, left_msg);
        }
    }

    function leave_all_rooms(conn) {
        conn.joined_rooms().forEach(function (room_name) {
            leave_room(room_name, conn);
        });
    }

    async function get_cluster_presence(room_name) {
        const member_set = Object.create(null);
        const room = rooms[room_name];
        if (room !== undefined) {
            room.snapshot().forEach(function (id) {
                member_set[id] = true;
            });
        }

        try {
            const members = await adapter.get_presence(room_name);
            if (Array.isArray(members)) {
                members.forEach(function (id) {
                    member_set[id] = true;
                });
            }
        } catch (ignore) {}

        return Object.keys(member_set);
    }

    function send_direct_to_cluster(msg) {
        const raw = msg.encode();
        adapter.get_node_for_conn(msg.dst).then(function (target_node) {
            if (typeof target_node === "string" && target_node !== "") {
                return adapter.publish_direct_raw(target_node, raw);
            }
            return adapter.publish_raw("root", raw);
        }).catch(function () {});
    }

    async function dispatch(conn, msg) {
        switch (msg.event) {
        case "join": {
            if (typeof opts.authorize_room === "function") {
                const allowed = opts.authorize_room(conn, msg.room);
                if (allowed === false) {
                    break;
                }
            }
            await join_room(msg.room, conn);
            const presence = await get_cluster_presence(msg.room);
            const ack = create_message(
                msg.room,
                "join_ack",
                "",
                conn.id,
                JSON.stringify(presence)
            );
            conn.try_send(ack.encode());
            break;
        }

        case "leave": {
            leave_room(msg.room, conn);
            const ack = create_message(msg.room, "leave_ack", "", conn.id, conn.id);
            conn.try_send(ack.encode());
            break;
        }

        default: {
            if (msg.dst !== "") {
                const dst = conns[msg.dst];
                if (dst !== undefined) {
                    dst.try_send(msg.encode());
                } else {
                    send_direct_to_cluster(msg);
                }
                return;
            }

            const handler = handlers[msg.event];
            if (typeof handler === "function") {
                try {
                    await handler(conn, msg);
                } catch (err) {
                    console.error("Handler error for event '" + msg.event + "':", err);
                }
                return;
            }

            broadcast_room(conn.id, msg);
        }
        }
    }

    async function shutdown() {
        Object.keys(conns).forEach(function (id) {
            try {
                conns[id].ws.close(1001, "Server shutting down");
            } catch (ignore) {}
        });
        Object.keys(conns).forEach(function (id) {
            delete conns[id];
        });
        Object.keys(rooms).forEach(function (name) {
            delete rooms[name];
        });
        await adapter.close();
    }

    self = Object.freeze({
        add_conn,
        broadcast_room,
        configure,
        dispatch,
        get_cluster_presence,
        get_conn,
        get_room,
        join_room,
        leave_all_rooms,
        leave_room,
        metrics: Object.freeze({
            onConnect: function () { metrics.onConnect(); },
            onDisconnect: function () { metrics.onDisconnect(); },
            onMessageDropped: function () { metrics.onMessageDropped(); },
            onMessageReceived: function (b) { metrics.onMessageReceived(b); },
            onMessageSent: function (b) { metrics.onMessageSent(b); },
            onRoomCreated: function (r) { metrics.onRoomCreated(r); },
            onRoomDeleted: function (r) { metrics.onRoomDeleted(r); }
        }),
        register_handler,
        remove_conn,
        send_direct_to_cluster,
        shutdown
    });

    // Auto-subscribe if an adapter was passed directly
    if (opts.adapter !== undefined) {
        configure(opts.adapter, opts.metrics).catch(function () {});
    }

    return self;
}

export {
    create_hub,
    RESERVED_EVENTS
};
