/**
 * @fileoverview High-performance, room-based WebSocket client library with
 * zero-copy binary framing, event-driven subscription channels, and
 * automatic exponential backoff reconnection.
 *
 * @license MIT
 */

import bytecursor from "./bytecursor.js";
import emitter from "./emitter.js";

/**
 * UTF-8 text decoder instance.
 * @type {TextDecoder}
 */
const decoder = new TextDecoder("utf-8");

/**
 * UTF-8 text encoder instance.
 * @type {TextEncoder}
 */
const encoder = new TextEncoder();

/**
 * Internal protocol event names reserved by the roomer framework.
 * Handlers for these names cannot be emitted or overridden directly
 * via `.send()`.
 * @type {readonly string[]}
 */
const reserved_events = Object.freeze([
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
 * Checks whether a given value is an ArrayBuffer.
 *
 * @param {*} value - Value to validate.
 * @returns {boolean} True if value is an ArrayBuffer, false otherwise.
 */
function is_array_buffer(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        Object.prototype.toString.call(value) === "[object ArrayBuffer]"
    );
}

/**
 * Serializes message parameters into a length-prefixed binary packet.
 *
 * Wire Format:
 * [4B room_len][room][4B event_len][event]...
 *
 * @param {string} room_name - Destination room channel name.
 * @param {string} event_name - Message event name.
 * @param {string} dst_id - Destination client ID (or empty string).
 * @param {string} src_id - Origin client ID.
 * @param {*} payload_data - Payload data (string, buffer, object).
 * @returns {Uint8Array} Contiguous serialized binary packet data.
 */
function new_message(room_name, event_name, dst_id, src_id, payload_data) {
    let dst = dst_id;
    let event = event_name;
    let payload = payload_data;
    let room = room_name;
    let src = src_id;

    if (typeof room !== "string") {
        room = "";
    }
    if (typeof event !== "string") {
        event = "";
    }
    if (typeof dst !== "string") {
        dst = "";
    }
    if (typeof src !== "string") {
        src = "";
    }
    if (payload === undefined || payload === null) {
        payload = "";
    }

    const room_bytes = encoder.encode(room);
    const event_bytes = encoder.encode(event);
    const dst_bytes = encoder.encode(dst);
    const src_bytes = encoder.encode(src);

    let payload_bytes;
    let payload_len = 0;

    if (typeof payload === "string") {
        payload_bytes = encoder.encode(payload);
        payload_len = payload_bytes.byteLength;
    } else if (ArrayBuffer.isView(payload)) {
        payload_bytes = new Uint8Array(
            payload.buffer,
            payload.byteOffset,
            payload.byteLength
        );
        payload_len = payload_bytes.byteLength;
    } else if (is_array_buffer(payload)) {
        payload_bytes = new Uint8Array(payload);
        payload_len = payload_bytes.byteLength;
    } else if (typeof payload === "object") {
        payload_bytes = encoder.encode(JSON.stringify(payload));
        payload_len = payload_bytes.byteLength;
    } else if (
        typeof payload === "number" ||
        typeof payload === "boolean"
    ) {
        payload_bytes = encoder.encode(String(payload));
        payload_len = payload_bytes.byteLength;
    } else {
        payload_bytes = new Uint8Array(0);
    }

    const total_bytes = (
        room_bytes.byteLength +
        event_bytes.byteLength +
        dst_bytes.byteLength +
        src_bytes.byteLength +
        payload_len +
        20
    );

    const data = bytecursor(new ArrayBuffer(total_bytes));
    data.writeUint32(room_bytes.byteLength);
    data.writeBytes(room_bytes);
    data.writeUint32(event_bytes.byteLength);
    data.writeBytes(event_bytes);
    data.writeUint32(dst_bytes.byteLength);
    data.writeBytes(dst_bytes);
    data.writeUint32(src_bytes.byteLength);
    data.writeBytes(src_bytes);
    data.writeUint32(payload_len);

    if (payload_len > 0) {
        data.writeBytes(payload_bytes);
    }

    data.rewind();
    return data.getBytes();
}

/**
 * @typedef {Object} Packet
 * @property {string} dst - Targeted destination client ID.
 * @property {string} event - Event descriptor name.
 * @property {Uint8Array} payload - Raw binary payload data.
 * @property {string} room - Channel or room name.
 * @property {string} src - Source client ID of the sender.
 */

/**
 * @typedef {Object} RoomerOptions
 * @property {boolean} [reconnect=true]
 *     Whether to automatically reconnect on connection drop.
 * @property {number} [initial_delay=500]
 *     Initial reconnection backoff delay in milliseconds.
 * @property {number} [max_delay=5000]
 *     Maximum reconnection backoff ceiling in milliseconds.
 */

/**
 * @typedef {Object} Room
 * @property {string} name
 *     The room channel name.
 * @property {(exceptions?: string[]) => Room} clearListeners
 *     Removes registered event listeners except those in exceptions.
 * @property {(is_disconnect?: boolean) => Room} forceClose
 *     Forces the room to close locally and clears member state.
 * @property {() => string} id
 *     Returns the client ID assigned to this connection.
 * @property {(room_name: string) => Room} join
 *     Subscribes to a new room over the WebSocket connection.
 * @property {() => Room} leave
 *     Leaves the room and notifies the server.
 * @property {() => string[]} members
 *     Returns a shallow copy array of all active member IDs.
 * @property {() => boolean} open
 *     Returns whether the room connection is active.
 * @property {(packet: Packet) => void} parse
 *     Parses an incoming binary packet and dispatches events.
 * @property {() => Room} [purge]
 *     Leaves all non-root rooms simultaneously (root only).
 * @property {() => Readonly<Object.<string, Room>>} [rooms]
 *     Returns a read-only map of all active room instances.
 * @property {(event: string, payload?: *, dst?: string) => Room} send
 *     Sends a message packet to the room or directly to a member.
 * @property {(type: string, fn: Function) => Room} on
 *     Subscribes a listener callback to an event.
 * @property {(type: string, fn: Function) => Room} once
 *     Subscribes a one-time listener callback to an event.
 * @property {(type: string, fn: Function) => Room} off
 *     Unsubscribes a listener callback from an event.
 * @property {(type: string, ...args: *) => boolean} emit
 *     Synchronously invokes listener callbacks for an event.
 * @property {(type?: string) => Room} removeAllListeners
 *     Removes all listeners or those for a specified event.
 * @property {(type?: string) => Function[]} listeners
 *     Returns an array of listeners for an event type.
 */

/**
 * Initializes a roomer WebSocket connection and returns root room.
 *
 * @param {string} url - WebSocket server endpoint URL.
 * @param {RoomerOptions} [options] - Reconnection options.
 * @throws {TypeError} If the url parameter is not a string.
 * @returns {Room} The root room client instance.
 */
function roomer(url, options) {
    if (typeof url !== "string") {
        throw new TypeError("WebSocket URL must be a string.");
    }

    /** @type {Required<RoomerOptions>} */
    const opts = Object.assign({
        initial_delay: 500,
        max_delay: 5000,
        reconnect: true
    }, options);

    /** @type {Object.<string, Room>} */
    const rooms = Object.create(null);

    /** @type {WebSocket|undefined} */
    let socket;
    let manual_close = false;
    let reconnect_delay = opts.initial_delay;

    /**
     * Establishes the WebSocket connection and sets up binary handlers.
     * @returns {void}
     */
    function connect() {
        if (WebSocket === undefined) {
            return;
        }

        socket = new WebSocket(url);
        socket.binaryType = "arraybuffer";

        socket.onopen = function () {
            reconnect_delay = opts.initial_delay;

            // Re-join previously active rooms upon reconnect
            Object.keys(rooms).forEach(function (r_name) {
                if (r_name !== "root") {
                    socket.send(
                        new_message(
                            r_name,
                            "join",
                            "",
                            "",
                            ""
                        )
                    );
                }
            });
        };

        socket.onmessage = function (e) {
            try {
                const data = bytecursor(e.data);
                const room_str = data.getString(data.getUint32());
                const event_str = data.getString(data.getUint32());
                const dst_str = data.getString(data.getUint32());
                const src_str = data.getString(data.getUint32());
                const payload_bytes = data.getBytes(data.getUint32());

                /** @type {Packet} */
                const packet = {
                    dst: dst_str,
                    event: event_str,
                    payload: payload_bytes,
                    room: room_str,
                    src: src_str
                };

                if (rooms[packet.room] !== undefined) {
                    rooms[packet.room].parse(packet);
                }
            } catch (err) {
                console.error("Failed to parse binary WebSocket frame: ", err);
            }
        };

        socket.onclose = function () {
            const is_reconnecting = (manual_close === false && opts.reconnect === true);
            Object.keys(rooms).forEach(function (r_name) {
                rooms[r_name].forceClose(is_reconnecting);
            });

            if (is_reconnecting === true) {
                const jitter = Math.random() * 200;
                setTimeout(function () {
                    reconnect_delay = Math.min(
                        reconnect_delay * 1.5,
                        opts.max_delay
                    );
                    connect();
                }, reconnect_delay + jitter);
            }
        };

        socket.onerror = function (err) {
            console.error("Roomer WebSocket error: ", err);
        };
    }

    /**
     * Retrieves or instantiates a room client interface by name.
     *
     * @param {string} name - Room channel name.
     * @throws {TypeError} If name is not a string.
     * @returns {Room} Room interface instance.
     */
    function get_room(name) {
        if (typeof name !== "string") {
            throw new TypeError("Room name must be a string");
        }
        if (rooms[name] !== undefined) {
            return rooms[name];
        }

        /** @type {string[]} */
        const members = [];
        const registered_events = Object.create(null);
        let is_open = false;
        let member_id = "";
        let self;

        /**
         * Clears registered listeners except those explicitly listed.
         *
         * @param {string[]} [exceptions] - Event names to preserve.
         * @returns {Room} The room instance.
         */
        function clearListeners(exceptions) {
            let exc_list = exceptions;
            if (!Array.isArray(exc_list)) {
                exc_list = [];
            }
            Object.keys(registered_events).forEach(function (event_type) {
                if (exc_list.includes(event_type) === false) {
                    self.removeAllListeners(event_type);
                    delete registered_events[event_type];
                }
            });
            return self;
        }

        /**
         * Closes the room locally and clears all tracked state.
         *
         * @param {boolean} [is_disconnect=false] - Whether this close is due to socket drop.
         * @returns {Room} The room instance.
         */
        function forceClose(is_disconnect) {
            if (is_open === true) {
                is_open = false;
                members.length = 0;
                self.emit("close");
                if (is_disconnect !== true) {
                    member_id = "";
                    delete rooms[name];
                }
            }
            return self;
        }

        /**
         * Returns the member client ID assigned to this connection.
         *
         * @returns {string} Assigned client ID string.
         */
        function getId() {
            return member_id;
        }

        /**
         * Joins a new room channel on the current connection.
         *
         * @param {string} room_name - Room name to join.
         * @throws {Error} If the current room is closed.
         * @throws {TypeError} If room_name is not a string.
         * @returns {Room} Joined room instance.
         */
        function join(room_name) {
            if (is_open === false) {
                throw new Error("Cannot join: room is closed.");
            }
            if (typeof room_name !== "string") {
                throw new TypeError("Room name must be a string.");
            }
            return get_room(room_name);
        }

        /**
         * Leaves the current room and notifies the server.
         *
         * @throws {Error} If the room is closed.
         * @returns {Room} The room instance.
         */
        function leave() {
            if (is_open === false) {
                throw new Error("Cannot leave: room is closed.");
            }
            if (
                socket !== undefined &&
                socket.readyState === WebSocket.OPEN
            ) {
                socket.send(
                    new_message(
                        name,
                        "leave",
                        "",
                        "",
                        ""
                    )
                );
            }
            return self;
        }

        /**
         * Returns a shallow copy array of all active member IDs.
         *
         * @returns {string[]} Member ID array.
         */
        function getMembers() {
            return members.slice();
        }

        /**
         * Returns whether the room connection is open and active.
         *
         * @returns {boolean} True if active, false otherwise.
         */
        function getIsOpen() {
            return is_open;
        }

        /**
         * Dispatches an incoming parsed packet to room listeners.
         *
         * @param {Packet} packet - Incoming packet frame.
         * @returns {void}
         */
        function parse(packet) {
            let member_index;
            let parsed;
            let payload_text;

            switch (packet.event) {
            case "join_ack":
                member_id = packet.src;
                members.length = 0;
                try {
                    parsed = JSON.parse(decoder.decode(packet.payload));
                    if (Array.isArray(parsed) === true) {
                        members.push(...parsed);
                    }
                } catch (ignore) {
                    parsed = null;
                }
                is_open = true;
                self.emit("open");
                break;

            case "new_member":
                payload_text = decoder.decode(packet.payload);
                if (members.includes(payload_text) === false) {
                    members.push(payload_text);
                    self.emit("new_member", payload_text);
                }
                break;

            case "leave_ack":
                self.emit("close");
                is_open = false;
                members.length = 0;
                member_id = "";
                delete rooms[name];
                break;

            case "member_left":
                payload_text = decoder.decode(packet.payload);
                if (members.includes(payload_text) === true) {
                    member_index = members.indexOf(payload_text);
                    members.splice(member_index, 1);
                    self.emit("member_left", payload_text);
                }
                break;

            default:
                self.emit(packet.event, packet.payload, packet.src);
            }
        }

        /**
         * Sends an event message to the room or a recipient ID.
         *
         * @param {string} event - Event name to transmit.
         * @param {*} [payload] - Optional payload data.
         * @param {string} [dst] - Optional destination member ID.
         * @throws {Error} If room is closed or event is reserved.
         * @returns {Room} The room instance.
         */
        function send(event, payload, dst) {
            if (is_open === false) {
                throw new Error("Cannot send: socket is closed.");
            }
            if (typeof event !== "string") {
                throw new Error("Event name must be a string.");
            }
            if (reserved_events.includes(event) === true) {
                throw new Error("Reserved event: " + event);
            }
            if (
                socket !== undefined &&
                socket.readyState === WebSocket.OPEN
            ) {
                socket.send(new_message(name, event, dst, member_id, payload));
            }
            return self;
        }

        const room_methods = {
            clearListeners,
            forceClose,
            id: getId,
            join,
            leave,
            members: getMembers,
            name,
            open: getIsOpen,
            parse,
            send
        };

        if (name === "root") {
            /**
             * Leaves all active non-root rooms simultaneously.
             *
             * @returns {Room} The root room instance.
             */
            room_methods.purge = function () {
                Object.keys(rooms).forEach(function (r_name) {
                    if (r_name !== "root") {
                        rooms[r_name].leave();
                    }
                });
                return self;
            };

            /**
             * Returns a frozen copy of all active room instances.
             *
             * @returns {Readonly<Object.<string, Room>>} Active rooms map.
             */
            room_methods.rooms = function () {
                const room_copy = Object.create(null);
                Object.keys(rooms).forEach(function (r_key) {
                    room_copy[r_key] = rooms[r_key];
                });
                return Object.freeze(room_copy);
            };
        }

        // Mix in EventEmitter methods and freeze the instance
        self = emitter(room_methods);

        // Track custom registered event names for clearListeners()
        self.on("newListener", function (event_type) {
            if (event_type !== "newListener") {
                registered_events[event_type] = true;
            }
        });

        rooms[name] = self;

        if (
            name !== "root" &&
            socket !== undefined &&
            socket.readyState === WebSocket.OPEN
        ) {
            socket.send(
                new_message(name, "join", "", "", "")
            );
        }

        return self;
    }

    connect();
    return get_room("root");
}

export default Object.freeze(roomer);
