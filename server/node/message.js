/**
 * @fileoverview High-performance binary message packet framing and encoding
 * adhering to strict Crockfordian functional conventions.
 *
 * Wire format (12-byte base header overhead):
 * [1B version][1B flags][2B room_len][room][2B event_len][event]
 * [1B dst_len][dst][1B src_len][src][4B payload_len][payload]
 */

const PROTOCOL_VERSION = 1;
const DEFAULT_FLAGS = 0;
const HEADER_OVERHEAD = 12;

/**
 * Checks whether a given value is a Buffer.
 *
 * @param {*} value - The value to check.
 * @returns {boolean} True if the value is a Buffer, false otherwise.
 */
function is_buffer(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        typeof value.readUInt32BE === "function"
    );
}

/**
 * Checks whether a given value is a Uint8Array.
 *
 * @param {*} value - The value to check.
 * @returns {boolean} True if the value is a Uint8Array, false otherwise.
 */
function is_uint8_array(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        Object.prototype.toString.call(value) === "[object Uint8Array]"
    );
}

/**
 * Creates and serializes a 12-byte header binary message frame.
 *
 * @param {string} [room_name=""] - Target room channel name.
 * @param {string} [event_name=""] - Event descriptor name.
 * @param {string} [dst_id=""] - Destination client ID (or empty string).
 * @param {string} [src_id=""] - Origin client ID.
 * @param {Buffer|Uint8Array|string|object} [payload_data=""] - Message payload.
 * @param {number} [version=1] - Protocol version.
 * @param {number} [flags=0] - Protocol flags.
 * @returns {Readonly<{
 *     dst: string,
 *     encode: () => Buffer,
 *     event: string,
 *     flags: number,
 *     payload: Buffer,
 *     payloadJSON: () => *,
 *     payloadString: () => string,
 *     room: string,
 *     src: string,
 *     version: number
 * }>} Frozen message object.
 */
function create_message(room_name, event_name, dst_id, src_id, payload_data, version, flags) {
    const room = (typeof room_name === "string" ? room_name : "");
    const event = (typeof event_name === "string" ? event_name : "");
    const dst = (typeof dst_id === "string" ? dst_id : "");
    const src = (typeof src_id === "string" ? src_id : "");
    const ver = (typeof version === "number" && version > 0 ? version : PROTOCOL_VERSION);
    const flg = (typeof flags === "number" ? flags : DEFAULT_FLAGS);

    let payload;
    if (typeof payload_data === "string") {
        payload = Buffer.from(payload_data, "utf-8");
    } else if (is_buffer(payload_data)) {
        payload = payload_data;
    } else if (is_uint8_array(payload_data)) {
        payload = Buffer.from(
            payload_data.buffer,
            payload_data.byteOffset,
            payload_data.byteLength
        );
    } else if (typeof payload_data === "object" && payload_data !== null) {
        payload = Buffer.from(JSON.stringify(payload_data), "utf-8");
    } else {
        payload = Buffer.alloc(0);
    }

    /**
     * Serializes the message into a contiguous big-endian binary Buffer.
     *
     * @returns {Buffer} The serialized wire frame.
     */
    function encode() {
        const room_buf = Buffer.from(room, "utf-8");
        const event_buf = Buffer.from(event, "utf-8");
        const dst_buf = Buffer.from(dst, "utf-8");
        const src_buf = Buffer.from(src, "utf-8");

        if (room_buf.length > 65535) {
            throw new RangeError("Room name exceeds uint16 maximum length (65535 bytes)");
        }
        if (event_buf.length > 65535) {
            throw new RangeError("Event name exceeds uint16 maximum length (65535 bytes)");
        }
        if (dst_buf.length > 255) {
            throw new RangeError("Destination ID exceeds uint8 maximum length (255 bytes)");
        }
        if (src_buf.length > 255) {
            throw new RangeError("Source ID exceeds uint8 maximum length (255 bytes)");
        }

        const total_len = (
            HEADER_OVERHEAD +
            room_buf.length +
            event_buf.length +
            dst_buf.length +
            src_buf.length +
            payload.length
        );

        const buf = Buffer.allocUnsafe(total_len);
        let offset = 0;

        buf.writeUInt8(ver, offset);
        offset += 1;

        buf.writeUInt8(flg, offset);
        offset += 1;

        buf.writeUInt16BE(room_buf.length, offset);
        offset += 2;
        room_buf.copy(buf, offset);
        offset += room_buf.length;

        buf.writeUInt16BE(event_buf.length, offset);
        offset += 2;
        event_buf.copy(buf, offset);
        offset += event_buf.length;

        buf.writeUInt8(dst_buf.length, offset);
        offset += 1;
        dst_buf.copy(buf, offset);
        offset += dst_buf.length;

        buf.writeUInt8(src_buf.length, offset);
        offset += 1;
        src_buf.copy(buf, offset);
        offset += src_buf.length;

        buf.writeUInt32BE(payload.length, offset);
        offset += 4;
        payload.copy(buf, offset);

        return buf;
    }

    /**
     * Decodes the payload as a UTF-8 string.
     *
     * @returns {string} Decoded string.
     */
    function payloadString() {
        return payload.toString("utf-8");
    }

    /**
     * Parses the payload as JSON.
     *
     * @throws {Error} If payload is empty or malformed JSON.
     * @returns {*} Parsed JSON value.
     */
    function payloadJSON() {
        if (payload.length === 0) {
            throw new Error("Payload is empty");
        }
        return JSON.parse(payload.toString("utf-8"));
    }

    return Object.freeze({
        dst,
        encode,
        event,
        flags: flg,
        payload,
        payloadJSON,
        payloadString,
        room,
        src,
        version: ver
    });
}

/**
 * Decodes raw binary bytes into a Message instance.
 *
 * @param {Buffer|Uint8Array} data - Binary packet buffer.
 * @param {number} [max_message_size=0] - Maximum allowed message size in bytes.
 * @returns {Readonly<{
 *     dst: string,
 *     encode: () => Buffer,
 *     event: string,
 *     flags: number,
 *     payload: Buffer,
 *     payloadJSON: () => *,
 *     payloadString: () => string,
 *     room: string,
 *     src: string,
 *     version: number
 * }>|null} Decoded message or null if malformed.
 */
function decode_message(data, max_message_size) {
    if (!is_buffer(data) && !is_uint8_array(data)) {
        return null;
    }

    const buf = (
        is_buffer(data)
        ? data
        : Buffer.from(data.buffer, data.byteOffset, data.byteLength)
    );

    if (buf.length < HEADER_OVERHEAD) {
        return null;
    }

    if (typeof max_message_size === "number" && max_message_size > 0 && buf.length > max_message_size) {
        return null;
    }

    let offset = 0;

    // 1. Version & Flags
    const version = buf.readUInt8(offset);
    offset += 1;
    const flags = buf.readUInt8(offset);
    offset += 1;

    // 2. Room (2B length)
    const room_len = buf.readUInt16BE(offset);
    offset += 2;
    if (offset + room_len > buf.length) {
        return null;
    }
    const room = buf.toString("utf-8", offset, offset + room_len);
    offset += room_len;

    // 3. Event (2B length)
    if (offset + 2 > buf.length) {
        return null;
    }
    const event_len = buf.readUInt16BE(offset);
    offset += 2;
    if (offset + event_len > buf.length) {
        return null;
    }
    const event = buf.toString("utf-8", offset, offset + event_len);
    offset += event_len;

    // 4. Dst (1B length)
    if (offset + 1 > buf.length) {
        return null;
    }
    const dst_len = buf.readUInt8(offset);
    offset += 1;
    if (offset + dst_len > buf.length) {
        return null;
    }
    const dst = buf.toString("utf-8", offset, offset + dst_len);
    offset += dst_len;

    // 5. Src (1B length)
    if (offset + 1 > buf.length) {
        return null;
    }
    const src_len = buf.readUInt8(offset);
    offset += 1;
    if (offset + src_len > buf.length) {
        return null;
    }
    const src = buf.toString("utf-8", offset, offset + src_len);
    offset += src_len;

    // 6. Payload (4B length)
    if (offset + 4 > buf.length) {
        return null;
    }
    const payload_len = buf.readUInt32BE(offset);
    offset += 4;
    if (offset + payload_len !== buf.length) {
        return null;
    }
    const payload = buf.subarray(offset, offset + payload_len);

    return create_message(room, event, dst, src, payload, version, flags);
}

export {
    DEFAULT_FLAGS,
    HEADER_OVERHEAD,
    PROTOCOL_VERSION,
    create_message,
    decode_message
};
