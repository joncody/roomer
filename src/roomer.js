/**
 * @fileoverview WebSocket-based multi-room messaging client library
 * using binary packet framing with bytecursor and emitter mixins.
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
 * Internal event names reserved by the roomer protocol.
 * @type {string[]}
 */
const reserved_events = [
    "close",
    "join",
    "join_ack",
    "leave",
    "leave_ack",
    "member_left",
    "new_member",
    "open"
];

/**
 * Checks whether a value is an ArrayBuffer.
 *
 * @param {*} value - The value to check.
 * @returns {boolean} True if the value is an ArrayBuffer, false otherwise.
 */
function is_array_buffer(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        Object.prototype.toString.call(value) === "[object ArrayBuffer]"
    );
}

/**
 * Serializes a message packet into binary format using bytecursor.
 *
 * @param {string} room_name - Target room name.
 * @param {string} event_name - Name of the event to send.
 * @param {string} dst_id - Destination client ID.
 * @param {string} src_id - Source client ID.
 * @param {*} payload_data - Message payload data (string, buffer, object).
 * @returns {Uint8Array} Serialized binary packet data.
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
 * @property {string} dst - Destination client ID.
 * @property {string} event - Event name.
 * @property {Uint8Array} payload - Binary message payload.
 * @property {string} room - Room name.
 * @property {string} src - Source client ID.
 */

/**
 * @typedef {Object} Room
 * @property {(exceptions?: string[]) => Room} clearListeners
 *     Clears event listeners except for optional exceptions list.
 * @property {() => Room} forceClose
 *     Forces room closure and cleans up room state.
 * @property {() => string} id
 *     Returns the client ID assigned within the room.
 * @property {(room_name: string) => Room} join
 *     Joins another room on the same connection.
 * @property {() => Room} leave
 *     Leaves the current room.
 * @property {() => string[]} members
 *     Returns a copy of the list of current room member IDs.
 * @property {string} name
 *     The room name.
 * @property {() => boolean} open
 *     Returns whether the room connection is open.
 * @property {(packet: Packet) => void} parse
 *     Parses an incoming packet and dispatches room events.
 * @property {() => Room} [purge]
 *     Leaves all non-root rooms (root room only).
 * @property {() => Object.<string, Room>} [rooms]
 *     Returns a copy of all active room instances (root room only).
 * @property {(event: string, payload?: *, dst?: string) => Room} send
 *     Sends a message packet to the room or a specific recipient.
 */

/**
 * Initializes a WebSocket client and returns the root room interface.
 *
 * @param {string} url - WebSocket server endpoint URL.
 * @throws {TypeError} If url is not a string.
 * @returns {Room} Root room instance for managing connections.
 */
function roomer(url) {
    if (typeof url !== "string") {
        throw new TypeError("WebSocket URL must be a string.");
    }

    const rooms = Object.create(null);
    let socket;

    /**
     * Retrieves or creates a room instance by name.
     *
     * @param {string} name - The name of the room.
     * @throws {TypeError} If name is not a string.
     * @returns {Room} The room instance.
     */
    function get_room(name) {
        if (typeof name !== "string") {
            throw new TypeError("Room name must be a string");
        }
        if (rooms[name] !== undefined) {
            return rooms[name];
        }

        const members = [];
        const registered_events = Object.create(null);
        let is_open = false;
        let member_id = "";
        let self;

        /**
         * Clears all registered event listeners except those in exceptions.
         *
         * @param {string[]} [exceptions] - Event types to preserve.
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
         * Forces the room to close and cleans up state.
         *
         * @returns {Room} The room instance.
         */
        function forceClose() {
            if (is_open === true) {
                is_open = false;
                members.length = 0;
                member_id = "";
                self.emit("close");
                delete rooms[name];
            }
            return self;
        }

        /**
         * Returns the client ID assigned to this member in the room.
         *
         * @returns {string} The member ID.
         */
        function getId() {
            return member_id;
        }

        /**
         * Joins a new room on the existing WebSocket connection.
         *
         * @param {string} room_name - Room name to join.
         * @throws {Error} If current room is closed.
         * @throws {TypeError} If room_name is not a string.
         * @returns {Room} The joined room instance.
         */
        function join(room_name) {
            if (is_open === false) {
                throw new Error("Cannot join: room is closed.");
            }
            if (typeof room_name !== "string") {
                throw new TypeError("Room name must be a string.");
            }
            if (rooms[room_name] !== undefined) {
                return rooms[room_name];
            }
            return get_room(room_name);
        }

        /**
         * Leaves the current room and sends leave notification to server.
         *
         * @throws {Error} If current room is closed.
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
                        member_id,
                        member_id,
                        member_id
                    )
                );
            }
            return self;
        }

        /**
         * Returns a copy of the list of current member IDs in the room.
         *
         * @returns {string[]} Array of member ID strings.
         */
        function getMembers() {
            return members.slice();
        }

        /**
         * Checks if the room connection is open.
         *
         * @returns {boolean} True if open, false otherwise.
         */
        function getIsOpen() {
            return is_open;
        }

        /**
         * Dispatches an incoming packet to the room's event listeners.
         *
         * @param {Packet} packet - Parsed binary packet data.
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
                    parsed = JSON.parse(
                        decoder.decode(packet.payload)
                    );
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
         * Sends an event message to the room or a specific member.
         *
         * @param {string} event - Event name to emit.
         * @param {*} [payload] - Optional payload data.
         * @param {string} [dst] - Optional destination member ID.
         * @throws {Error} If room is closed, event is invalid, or reserved.
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
                socket.send(
                    new_message(name, event, dst, member_id, payload)
                );
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
            room_methods.purge = function () {
                Object.keys(rooms).forEach(function (r_name) {
                    if (r_name !== "root") {
                        rooms[r_name].leave();
                    }
                });
                return self;
            };

            room_methods.rooms = function () {
                const room_copy = Object.create(null);
                Object.keys(rooms).forEach(function (r_key) {
                    room_copy[r_key] = rooms[r_key];
                });
                return Object.freeze(room_copy);
            };
        }

        // 1. Create frozen room object directly via emitter mixin
        self = emitter(room_methods);

        // 2. Attach newListener handler to automatically track
        //    registered event names
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
                new_message(name, "join", member_id, member_id, member_id)
            );
        }

        return self;
    }

    if (WebSocket !== undefined) {
        socket = new WebSocket(url);
        socket.binaryType = "arraybuffer";

        socket.onmessage = function (e) {
            try {
                const data = bytecursor(e.data);
                const room_str = data.getString(data.getUint32());
                const event_str = data.getString(data.getUint32());
                const dst_str = data.getString(data.getUint32());
                const src_str = data.getString(data.getUint32());
                const payload_bytes = data.getBytes(data.getUint32());

                const packet = {
                    dst: dst_str,
                    event: event_str,
                    payload: payload_bytes,
                    room: room_str,
                    src: src_str
                };

                if (rooms[packet.room] === undefined) {
                    return;
                }
                rooms[packet.room].parse(packet);
            } catch (err) {
                console.error(
                    "Failed to parse incoming WebSocket message: ",
                    err
                );
            }
        };

        socket.onclose = function () {
            Object.keys(rooms).forEach(function (r_name) {
                rooms[r_name].forceClose();
            });
        };

        socket.onerror = function (err) {
            console.error("WebSocket error: " + err);
        };
    }

    return get_room("root");
}

export default Object.freeze(roomer);
