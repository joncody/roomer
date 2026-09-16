/**
 * @fileoverview High-performance, room-based WebSocket client library with
 * 12-byte binary framing, event-driven subscription channels, and
 * automatic exponential backoff reconnection.
 *
 * @license MIT
 */

import bytecursor from "./bytecursor.js";
import emitter from "./emitter.js";

/**
 * Protocol version constant.
 * @type {number}
 */
const PROTOCOL_VERSION = 1;

/**
 * Base header byte overhead:
 * [1B Version][1B Flags][2B RoomLen][2B EventLen][1B DstLen][1B SrcLen][4B PayloadLen]
 * @type {number}
 */
const HEADER_OVERHEAD = 12;

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
 * Serializes message parameters into a 12-byte header length-prefixed binary packet.
 *
 * Wire Format:
 * [1B Version][1B Flags][2B RoomLen][Room][2B EventLen][Event]
 * [1B DstLen][Dst][1B SrcLen][Src][4B PayloadLen][Payload]
 *
 * @param {string} room_name - Destination room channel name.
 * @param {string} event_name - Message event name.
 * @param {string} dst_id - Destination client ID (or empty string).
 * @param {string} src_id - Origin client ID.
 * @param {*} payload_data - Payload data (string, buffer, object).
 * @param {number} [flags=0] - Header flags bitfield.
 * @returns {Uint8Array} Contiguous serialized binary packet data.
 */
function new_message(room_name, event_name, dst_id, src_id, payload_data, flags) {
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

    if (room_bytes.byteLength > 65535) {
        throw new RangeError("Room name exceeds uint16 maximum length (65535 bytes)");
    }
    if (event_bytes.byteLength > 65535) {
        throw new RangeError("Event name exceeds uint16 maximum length (65535 bytes)");
    }
    if (dst_bytes.byteLength > 255) {
        throw new RangeError("Destination ID exceeds uint8 maximum length (255 bytes)");
    }
    if (src_bytes.byteLength > 255) {
        throw new RangeError("Source ID exceeds uint8 maximum length (255 bytes)");
    }

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
        HEADER_OVERHEAD +
        room_bytes.byteLength +
        event_bytes.byteLength +
        dst_bytes.byteLength +
        src_bytes.byteLength +
        payload_len
    );

    const data = bytecursor(new ArrayBuffer(total_bytes));
    data.writeUint8(PROTOCOL_VERSION);
    data.writeUint8(typeof flags === "number" ? flags : 0);
    data.writeUint16(room_bytes.byteLength);
    data.writeBytes(room_bytes);
    data.writeUint16(event_bytes.byteLength);
    data.writeBytes(event_bytes);
    data.writeUint8(dst_bytes.byteLength);
    data.writeBytes(dst_bytes);
    data.writeUint8(src_bytes.byteLength);
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
 * @property {number} flags - Wire protocol flags.
 * @property {Uint8Array} payload - Raw binary payload data.
 * @property {string} room - Channel or room name.
 * @property {string} src - Source client ID of the sender.
 * @property {number} version - Protocol version.
 */

/**
 * @typedef {Object} RoomerOptions
 * @property {boolean} [reconnect=true]
 *     Whether to automatically reconnect on connection drop.
 * @property {number} [initial_delay=500]
 *     Initial reconnection backoff delay in milliseconds.
 * @property {number} [max_delay=5000]
 *     Maximum reconnection backoff ceiling in milliseconds.
 * @property {(code: number, reason: string) => boolean} [should_reconnect]
 *     Optional callback to evaluate whether to reconnect given an RFC close code and reason.
 */

/**
 * @typedef {Object} Room
 * @property {string} name
 *     The room channel name.
 * @property {() => number} bufferedAmount
 *     Returns the number of bytes queued for transmission on the WebSocket.
 * @property {(exceptions?: string[]) => Room} clearListeners
 *     Removes registered event listeners except those in exceptions.
 * @property {(code?: number, reason?: string) => Room} [close]
 *     Explicitly closes connection and all active rooms (root only).
 * @property {(is_disconnect?: boolean, code?: number, reason?: string) => Room} forceClose
 *     Forces the room to close locally, emits close with code/reason, and clears member state.
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
 * @property {() => "connecting" | "open" | "closing" | "closed"} readyState
 *     Returns the current connection lifecycle state.
 * @property {() => Readonly<Object.<string, Room>>} [rooms]
 *     Returns a read-only map of all active room instances.
 * @property {(event: string, payload?: *, dst?: string) => Room} send
 *     Sends a message packet to the room or directly to a member.
 * @property {() => string} url
 *     Returns the WebSocket server endpoint URL.
 * @property {(type: string, fn: Function) => Room} on
 *     Subscribes a listener callback to an event.
 * @property {(type: string, fn: Function) => Room} once
 *     Subscribes a one-time listener callback to an event.
 * @property {(type: string, fn: Function) => Room} off
 *     Unsubscribes a listener callback from an event.
 * @property {(type: string, ...args: *) => boolean} emit
 *     Synchronously invokes listener callbacks for an event.
 * @property {(type: string, fn: Function) => Room} removeListener
 *     Removes a listener callback for the specified event type.
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
        reconnect: true,
        should_reconnect: function (code) {
            // Do not reconnect on clean exit (1000), policy violation (1008), or auth/kick error codes (4000-4999)
            if (code === 1000 || code === 1008 || (code >= 4000 && code < 5000)) {
                return false;
            }
            return true;
        }
    }, options);

    /** @type {Object.<string, Room>} */
    const rooms = Object.create(null);

    /** @type {WebSocket|undefined} */
    let socket;
    let manual_close = false;
    let reconnect_delay = opts.initial_delay;
    let reconnect_timer = null;

    /**
     * Clears any active reconnection timer.
     * @returns {void}
     */
    function clear_reconnect_timer() {
        if (reconnect_timer !== null) {
            clearTimeout(reconnect_timer);
            reconnect_timer = null;
        }
    }

    /**
     * Schedules an exponential backoff reconnection attempt.
     * @returns {void}
     */
    function schedule_reconnect() {
        if (manual_close === true || opts.reconnect !== true) {
            return;
        }
        clear_reconnect_timer();
        const jitter = Math.random() * 200;
        reconnect_timer = setTimeout(function () {
            reconnect_timer = null;
            reconnect_delay = Math.min(
                reconnect_delay * 1.5,
                opts.max_delay
            );
            connect();
        }, reconnect_delay + jitter);
    }

    /**
     * Establishes the WebSocket connection and sets up binary handlers.
     * @returns {void}
     */
    function connect() {
        if (WebSocket === undefined) {
            return;
        }
        if (manual_close === true) {
            return;
        }

        clear_reconnect_timer();

        try {
            socket = new WebSocket(url);
        } catch (err) {
            console.error("Roomer WebSocket connection error: ", err);
            const is_reconnecting = (
                manual_close === false &&
                opts.reconnect === true
            );
            Object.keys(rooms).forEach(function (r_name) {
                if (rooms[r_name] !== undefined) {
                    rooms[r_name].forceClose(is_reconnecting, 1006, "Connection error");
                }
            });
            if (is_reconnecting === true) {
                schedule_reconnect();
            }
            return;
        }

        socket.binaryType = "arraybuffer";

        socket.onopen = function () {
            reconnect_delay = opts.initial_delay;

            // Re-join previously active rooms upon reconnect
            Object.keys(rooms).forEach(function (r_name) {
                if (r_name !== "root") {
                    try {
                        socket.send(
                            new_message(
                                r_name,
                                "join",
                                "",
                                "",
                                ""
                            )
                        );
                    } catch (err) {
                        console.error(
                            "Failed to send join frame on reconnect: ",
                            err
                        );
                    }
                }
            });
        };

        socket.onmessage = function (e) {
            try {
                const data = bytecursor(e.data);
                if (data.length < HEADER_OVERHEAD) {
                    return;
                }
                const version = data.getUint8();
                const flags = data.getUint8();
                const room_str = data.getString(data.getUint16());
                const event_str = data.getString(data.getUint16());
                const dst_str = data.getString(data.getUint8());
                const src_str = data.getString(data.getUint8());
                const payload_bytes = data.getBytes(data.getUint32());

                /** @type {Packet} */
                const packet = {
                    dst: dst_str,
                    event: event_str,
                    flags,
                    payload: payload_bytes,
                    room: room_str,
                    src: src_str,
                    version
                };

                if (rooms[packet.room] !== undefined) {
                    rooms[packet.room].parse(packet);
                }
            } catch (err) {
                console.error("Failed to parse binary WebSocket frame: ", err);
            }
        };

        socket.onclose = function (e) {
            const code = (e && typeof e.code === "number") ? e.code : 1006;
            const reason = (e && typeof e.reason === "string") ? e.reason : "";
            const should_retry = (
                typeof opts.should_reconnect === "function"
                ? opts.should_reconnect(code, reason)
                : true
            );
            const is_reconnecting = (
                manual_close === false &&
                opts.reconnect === true &&
                should_retry === true
            );

            Object.keys(rooms).forEach(function (r_name) {
                if (rooms[r_name] !== undefined) {
                    rooms[r_name].forceClose(is_reconnecting, code, reason);
                }
            });

            if (is_reconnecting === true) {
                schedule_reconnect();
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
         * Returns the number of bytes queued for transmission on the WebSocket.
         *
         * @returns {number} Queued buffer byte count.
         */
        function getBufferedAmount() {
            if (socket !== undefined && socket !== null) {
                return socket.bufferedAmount;
            }
            return 0;
        }

        /**
         * Returns current lifecycle state of underlying WebSocket connection.
         *
         * @returns {"connecting" | "open" | "closing" | "closed"} State string.
         */
        function getReadyState() {
            if (socket === undefined || socket === null || manual_close === true) {
                return "closed";
            }
            switch (socket.readyState) {
            case 0:
                return "connecting";
            case 1:
                return "open";
            case 2:
                return "closing";
            case 3:
                return "closed";
            default:
                return "closed";
            }
        }

        /**
         * Returns the WebSocket server endpoint URL.
         *
         * @returns {string} WebSocket URL.
         */
        function getUrl() {
            if (
                socket !== undefined &&
                socket !== null &&
                typeof socket.url === "string" &&
                socket.url !== ""
            ) {
                return socket.url;
            }
            return url;
        }

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
         * @param {boolean} [is_disconnect=false] - Whether this close is due
         *     to socket drop.
         * @param {number} [code=1000] - RFC 6455 closure status code.
         * @param {string} [reason=""] - Closure reason string.
         * @returns {Room} The room instance.
         */
        function forceClose(is_disconnect, code, reason) {
            const close_code = (typeof code === "number") ? code : 1000;
            const close_reason = (typeof reason === "string") ? reason : "";
            if (is_open === true) {
                is_open = false;
                members.length = 0;
                self.emit("close", close_code, close_reason);
            }
            if (is_disconnect !== true) {
                member_id = "";
                delete rooms[name];
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
                try {
                    socket.send(
                        new_message(
                            name,
                            "leave",
                            "",
                            "",
                            ""
                        )
                    );
                } catch (err) {
                    console.error("Failed to send leave packet: ", err);
                }
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
                self.emit("close", 1000, "Left room");
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
                try {
                    socket.send(
                        new_message(name, event, dst, member_id, payload)
                    );
                } catch (err) {
                    console.error("Failed to send message frame: ", err);
                }
            }
            return self;
        }

        const room_methods = {
            bufferedAmount: getBufferedAmount,
            clearListeners,
            forceClose,
            id: getId,
            join,
            leave,
            members: getMembers,
            name,
            open: getIsOpen,
            parse,
            readyState: getReadyState,
            send,
            url: getUrl
        };

        if (name === "root") {
            /**
             * Explicitly closes the WebSocket connection and all active rooms.
             *
             * @param {number} [code=1000] - RFC 6455 close status code.
             * @param {string} [reason="Client closed"] - Closure reason.
             * @returns {Room} The root room instance.
             */
            room_methods.close = function (code, reason) {
                manual_close = true;
                clear_reconnect_timer();
                const status_code = (typeof code === "number") ? code : 1000;
                const reason_str = (typeof reason === "string") ? reason : "Client closed";

                if (
                    socket !== undefined &&
                    socket !== null &&
                    (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)
                ) {
                    try {
                        socket.close(status_code, reason_str);
                    } catch (err) {
                        console.error("Failed to close WebSocket: ", err);
                    }
                }
                Object.keys(rooms).forEach(function (r_name) {
                    if (rooms[r_name] !== undefined) {
                        rooms[r_name].forceClose(false, status_code, reason_str);
                    }
                });
                return self;
            };

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
            try {
                socket.send(
                    new_message(name, "join", "", "", "")
                );
            } catch (err) {
                console.error("Failed to send join frame: ", err);
            }
        }

        return self;
    }

    connect();
    return get_room("root");
}

export default Object.freeze(roomer);
