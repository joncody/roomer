"use strict";

import bytecursor from "./bytecursor.js";
import emitter from "./emitter.js";

const decoder = new TextDecoder("utf-8");
const reserved = ["open", "close", "join", "join_ack", "leave", "leave_ack", "new_member", "member_left"];

function newMessage(room, event, dst, src, payload = "") {
    if (typeof dst !== "string") {
        dst = "";
    }
    if (typeof src !== "string") {
        src = "";
    }
    const payloadlen = payload.byteLength || payload.length || 0;
    const data = bytecursor(new ArrayBuffer(room.length + event.length + dst.length + src.length + payloadlen + 20));
    data.writeUint32(room.length).writeString(room);
    data.writeUint32(event.length).writeString(event);
    data.writeUint32(dst.length).writeString(dst);
    data.writeUint32(src.length).writeString(src);
    data.writeUint32(payloadlen);
    if (typeof payload === "string") {
        data.writeString(payload);
    } else {
        data.writeBytes(payload);
    }
    return data.rewind().getBytes();
}

const roomer = function (url) {
    if (typeof url !== "string") {
        throw new TypeError("WebSocket URL must be a string.");
    }

    const rooms = {};
    let socket;

    function getRoom(name) {
        const room = emitter();
        const members = [];
        let open = false;
        let id = "";

        if (typeof name !== "string") {
            throw new TypeError("Room name must be a string");
        }
        if (rooms.hasOwnProperty(name)) {
            return rooms[name];
        }

        room.name = name;

        room.open = function () {
            return open;
        };

        room.members = function () {
            if (typeof structuredClone === "function") {
                return structuredClone(members);
            }
            return JSON.parse(JSON.stringify(members));
        };

        room.id = function () {
            return id;
        };

        room.send = function (event, payload, dst) {
            if (!open) {
                throw new Error("Cannot send: socket is closed.");
            }
            if (typeof event !== "string") {
                throw new Error("Event name must  be a string.");
            }
            if (reserved.includes(event)) {
                throw new Error("Reserved event: " + event);
            }
            socket.send(newMessage(name, event, dst, id, payload));
            return room;
        };

        room.join = function (roomname) {
            if (!open) {
                throw new Error("Cannot join: room is closed.");
            }
            if (typeof roomname !== "string") {
                throw new TypeError("Room name must be a string.");
            }
            return (
                rooms.hasOwnProperty(roomname)
                ? rooms[roomname]
                : getRoom(roomname)
            );
        };

        room.leave = function () {
            if (!open) {
                throw new Error("Cannot leave: room is closed.");
            }
            socket.send(newMessage(name, "leave", id, id, id));
            return room;
        };

        room.parse = function (packet) {
            let payload;
            switch (packet.event) {
            case "join_ack":
                id = packet.src;
                members.length = 0;
                members.push(...JSON.parse(decoder.decode(packet.payload)));
                open = true;
                room.emit("open");
                break;
            case "new_member":
                payload = decoder.decode(packet.payload);
                if (!members.includes(payload)) {
                    members.push(payload);
                    room.emit("new_member", payload);
                }
                break;
            case "leave_ack":
                room.emit("close");
                open = false;
                members.length = 0;
                id = "";
                delete rooms[name];
                break;
            case "member_left":
                payload = decoder.decode(packet.payload);
                if (members.includes(payload)) {
                    const index = members.indexOf(payload);
                    members.splice(index, 1);
                    room.emit("member_left", payload);
                }
                break;
            default:
                room.emit(packet.event, packet.payload, packet.src);
            }
        };

        room.clearListeners = function (exceptions = []) {
            Object.keys(room.events).forEach(function (event) {
                if (exceptions.indexOf(event) === -1) {
                    room.removeAllListeners(event);
                }
            });
        };

        room.forceClose = function () {
            if (open) {
                open = false;
                members.length = 0;
                id = "";
                room.emit("close");
                delete rooms[name];
            }
        };

        rooms[name] = room;

        if (name !== "root") {
            socket.send(newMessage(name, "join", id, id, id));
        } else {
            room.purge = function () {
                Object.keys(rooms).forEach(function (name) {
                    if (name !== "root") {
                        rooms[name].leave();
                    }
                });
            };
            room.rooms = function () {
                if (typeof structuredClone === "function") {
                    return structuredClone(rooms);
                }
                return JSON.parse(JSON.stringify(rooms));
            };
        }
        return Object.freeze(room);
    }

    socket = new WebSocket(url);
    socket.binaryType = "arraybuffer";
    socket.onmessage = function (e) {
        const data = bytecursor(e.data);
        const packet = {
            room: data.getString(data.getUint32()),
            event: data.getString(data.getUint32()),
            dst: data.getString(data.getUint32()),
            src: data.getString(data.getUint32()),
            payload: data.getBytes(data.getUint32())
        };

        if (!rooms.hasOwnProperty(packet.room)) {
            console.warn("Room " + packet.room + " does not exist.");
            return;
        }
        rooms[packet.room].parse(packet);
    };
    socket.onclose = function () {
        Object.values(rooms).forEach(function (room) {
            room.forceClose();
        });
    };
    socket.onerror = function (err) {
        console.error("WebSocket error: " + err);
    };
    return getRoom("root");
};

export default Object.freeze(roomer);
