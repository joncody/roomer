import bytecursor from "./bytecursor.js";
import emitter from "./emitter.js";

const decoder = new TextDecoder("utf-8");
const encoder = new TextEncoder();

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

function is_array_buffer(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        Object.prototype.toString.call(value) === "[object ArrayBuffer]"
    );
}

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

function roomer(url) {
    if (typeof url !== "string") {
        throw new TypeError("WebSocket URL must be a string.");
    }

    const rooms = Object.create(null);
    let socket;

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

        function getId() {
            return member_id;
        }

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

        function getMembers() {
            return members.slice();
        }

        function getIsOpen() {
            return is_open;
        }

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
