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
    if (payload === undefined) {
        payload = "";
    }

    let payload_len = 0;
    if (typeof payload === "string") {
        payload_len = encoder.encode(payload).byteLength;
    } else if (
        typeof payload === "object" &&
        payload !== null &&
        typeof payload.byteLength === "number"
    ) {
        payload_len = payload.byteLength;
    }

    const total_bytes = (
        room.length +
        event.length +
        dst.length +
        src.length +
        payload_len +
        20
    );

    const data = bytecursor(new ArrayBuffer(total_bytes));
    data.writeUint32(room.length);
    data.writeString(room);
    data.writeUint32(event.length);
    data.writeString(event);
    data.writeUint32(dst.length);
    data.writeString(dst);
    data.writeUint32(src.length);
    data.writeString(src);
    data.writeUint32(payload_len);

    if (typeof payload === "string") {
        data.writeString(payload);
    } else {
        data.writeBytes(payload);
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
            if (socket !== undefined) {
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
            let payload_text;
            let member_index;

            switch (packet.event) {
            case "join_ack":
                member_id = packet.src;
                members.length = 0;
                members.push(...JSON.parse(decoder.decode(packet.payload)));
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
            if (socket !== undefined) {
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

        self = emitter(room_methods);

        const original_on = self.on;
        const original_once = self.once;

        self.on = function (type, fn, capture) {
            if (typeof type === "string") {
                registered_events[type] = true;
            }
            return original_on(type, fn, capture);
        };

        self.once = function (type, fn, capture) {
            if (typeof type === "string") {
                registered_events[type] = true;
            }
            return original_once(type, fn, capture);
        };

        rooms[name] = self;

        if (name !== "root" && socket !== undefined) {
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
