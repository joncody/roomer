const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8");

function is_array_buffer(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        Object.prototype.toString.call(value) === "[object ArrayBuffer]"
    );
}

function is_uint8_array(value) {
    return (
        typeof value === "object" &&
        value !== null &&
        Object.prototype.toString.call(value) === "[object Uint8Array]"
    );
}

function assert_integer(value, min, max, name) {
    if (typeof value !== "number") {
        throw new TypeError(name + " must be a number");
    }
    if (!Number.isInteger(value)) {
        throw new TypeError(name + " must be an integer");
    }
    if (value < min || value > max) {
        throw new RangeError(name + " must be between " + min + " and " + max);
    }
}

function bytecursor(buffer, view_offset, view_length) {
    if (view_offset === undefined) {
        view_offset = 0;
    }

    if (!is_array_buffer(buffer)) {
        throw new TypeError("requires an ArrayBuffer");
    }

    if (typeof view_offset !== "number") {
        throw new TypeError("viewOffset must be a number");
    }

    if (!Number.isInteger(view_offset) || view_offset < 0) {
        throw new RangeError("viewOffset must be a non-negative integer");
    }

    if (view_offset > buffer.byteLength) {
        throw new RangeError("viewOffset is out of bounds");
    }

    if (view_length !== undefined) {
        if (typeof view_length !== "number") {
            throw new TypeError("viewLength must be a number");
        }
        if (!Number.isInteger(view_length) || view_length < 0) {
            throw new RangeError("viewLength must be a non-negative integer");
        }
        if (view_offset + view_length > buffer.byteLength) {
            throw new RangeError("viewOffset + viewLength exceeds buffer size");
        }
    }

    const actual_length = (
        view_length === undefined
        ? buffer.byteLength - view_offset
        : view_length
    );

    const view = new DataView(buffer, view_offset, actual_length);
    let cursor = 0;
    let self;

    // -------------------------------------------------------------------------
    // Private Helpers
    // -------------------------------------------------------------------------

    function check(offset, size) {
        if (typeof offset !== "number") {
            throw new TypeError("Offset must be a number");
        }
        if (typeof size !== "number") {
            throw new TypeError("Size must be a number");
        }
        if (offset < 0) {
            throw new RangeError("Offset must be positive");
        }
        if (size < 0) {
            throw new RangeError("Size must be positive");
        }
        if (offset + size > view.byteLength) {
            throw new RangeError("Offset + size exceeds view bounds");
        }
    }

    function advance(size) {
        const pos = cursor;
        check(pos, size);
        cursor += size;
        return pos;
    }

    // -------------------------------------------------------------------------
    // Cursor Management
    // -------------------------------------------------------------------------

    function rewind() {
        cursor = 0;
        return self;
    }

    function tell() {
        return cursor;
    }

    function seek(pos) {
        check(pos, 0);
        cursor = pos;
        return self;
    }

    function skip(n) {
        check(cursor + n, 0);
        cursor += n;
        return self;
    }

    function eof() {
        return cursor >= view.byteLength;
    }

    // -------------------------------------------------------------------------
    // Buffer / Byte Operations
    // -------------------------------------------------------------------------

    function slice(start, end) {
        if (start === undefined) {
            start = 0;
        }
        if (end === undefined) {
            end = view.byteLength;
        }
        if (typeof start !== "number" || typeof end !== "number") {
            throw new TypeError("slice() arguments must be numbers");
        }
        if (start < 0 || end < 0) {
            throw new RangeError("slice() start and end must be non-negative");
        }
        if (start > end) {
            throw new RangeError("slice() start must not exceed end");
        }
        if (end > view.byteLength) {
            throw new RangeError("slice() end exceeds view bounds");
        }
        return buffer.slice(
            view.byteOffset + start,
            view.byteOffset + end
        );
    }

    function getBytes(len) {
        if (len === undefined) {
            len = view.byteLength - cursor;
        }
        const pos = advance(len);
        return new Uint8Array(
            buffer.slice(
                view.byteOffset + pos,
                view.byteOffset + pos + len
            )
        );
    }

    function writeBytes(bytes) {
        if (!is_uint8_array(bytes)) {
            throw new TypeError("writeBytes requires a Uint8Array");
        }
        const pos = advance(bytes.byteLength);
        new Uint8Array(
            buffer,
            view.byteOffset + pos,
            bytes.byteLength
        ).set(bytes);
        return self;
    }

    // -------------------------------------------------------------------------
    // Strings (UTF-8)
    // -------------------------------------------------------------------------

    function getString(length) {
        return decoder.decode(getBytes(length));
    }

    function writeString(string) {
        if (typeof string !== "string") {
            throw new TypeError("writeString() requires a string");
        }
        return writeBytes(encoder.encode(string));
    }

    // -------------------------------------------------------------------------
    // Numbers — Getters
    // -------------------------------------------------------------------------

    function getUint8() {
        return view.getUint8(advance(1));
    }

    function getInt8() {
        return view.getInt8(advance(1));
    }

    function getUint16(little_endian) {
        return view.getUint16(advance(2), Boolean(little_endian));
    }

    function getInt16(little_endian) {
        return view.getInt16(advance(2), Boolean(little_endian));
    }

    function getUint32(little_endian) {
        return view.getUint32(advance(4), Boolean(little_endian));
    }

    function getInt32(little_endian) {
        return view.getInt32(advance(4), Boolean(little_endian));
    }

    function getFloat32(little_endian) {
        return view.getFloat32(advance(4), Boolean(little_endian));
    }

    function getFloat64(little_endian) {
        return view.getFloat64(advance(8), Boolean(little_endian));
    }

    // -------------------------------------------------------------------------
    // Numbers — Writers
    // -------------------------------------------------------------------------

    function writeUint8(v) {
        assert_integer(v, 0, 255, "Uint8 value");
        view.setUint8(advance(1), v);
        return self;
    }

    function writeInt8(v) {
        assert_integer(v, -128, 127, "Int8 value");
        view.setInt8(advance(1), v);
        return self;
    }

    function writeUint16(v, little_endian) {
        assert_integer(v, 0, 65535, "Uint16 value");
        view.setUint16(advance(2), v, Boolean(little_endian));
        return self;
    }

    function writeInt16(v, little_endian) {
        assert_integer(v, -32768, 32767, "Int16 value");
        view.setInt16(advance(2), v, Boolean(little_endian));
        return self;
    }

    function writeUint32(v, little_endian) {
        assert_integer(v, 0, 4294967295, "Uint32 value");
        view.setUint32(advance(4), v, Boolean(little_endian));
        return self;
    }

    function writeInt32(v, little_endian) {
        assert_integer(v, -2147483648, 2147483647, "Int32 value");
        view.setInt32(advance(4), v, Boolean(little_endian));
        return self;
    }

    function writeFloat32(v, little_endian) {
        if (typeof v !== "number") {
            throw new TypeError("Float32 value must be a number");
        }
        view.setFloat32(advance(4), v, Boolean(little_endian));
        return self;
    }

    function writeFloat64(v, little_endian) {
        if (typeof v !== "number") {
            throw new TypeError("Float64 value must be a number");
        }
        view.setFloat64(advance(8), v, Boolean(little_endian));
        return self;
    }

    // -------------------------------------------------------------------------
    // Public Spec Export
    // -------------------------------------------------------------------------

    self = Object.freeze({
        buffer,
        eof,
        getBytes,
        getFloat32,
        getFloat64,
        getInt16,
        getInt32,
        getInt8,
        getString,
        getUint16,
        getUint32,
        getUint8,
        length: view.byteLength,
        rewind,
        seek,
        skip,
        slice,
        tell,
        view,
        writeBytes,
        writeFloat32,
        writeFloat64,
        writeInt16,
        writeInt32,
        writeInt8,
        writeString,
        writeUint16,
        writeUint32,
        writeUint8
    });

    return self;
}

export default Object.freeze(bytecursor);
