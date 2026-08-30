/**
 * @fileoverview Functional byte-level cursor for ArrayBuffer and DataView.
 * Provides seekable, sequential reading and writing for binary numbers,
 * raw bytes, and UTF-8 strings.
 */

/**
 * UTF-8 text encoder instance.
 * @type {TextEncoder}
 */
const encoder = new TextEncoder();

/**
 * UTF-8 text decoder instance.
 * @type {TextDecoder}
 */
const decoder = new TextDecoder("utf-8");

/**
 * Checks whether a given value is an ArrayBuffer.
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
 * Asserts that a value is an integer within a specified range.
 *
 * @param {number} value - The value to validate.
 * @param {number} min - The minimum allowed value (inclusive).
 * @param {number} max - The maximum allowed value (inclusive).
 * @param {string} name - The name of the parameter for error reporting.
 * @throws {TypeError} If the value is not a number or not an integer.
 * @throws {RangeError} If the value is outside the specified range.
 * @returns {void}
 */
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

/**
 * @typedef {Object} ByteCursor
 * @property {ArrayBuffer} buffer
 *     The underlying ArrayBuffer.
 * @property {() => boolean} eof
 *     Checks if the cursor is at the end of the view.
 * @property {(len?: number) => Uint8Array} getBytes
 *     Reads bytes from the current position.
 * @property {(little_endian?: boolean) => number} getFloat32
 *     Reads a 32-bit float.
 * @property {(little_endian?: boolean) => number} getFloat64
 *     Reads a 64-bit float.
 * @property {(little_endian?: boolean) => number} getInt16
 *     Reads a signed 16-bit integer.
 * @property {(little_endian?: boolean) => number} getInt32
 *     Reads a signed 32-bit integer.
 * @property {() => number} getInt8
 *     Reads a signed 8-bit integer.
 * @property {(length?: number) => string} getString
 *     Decodes a UTF-8 string from the cursor.
 * @property {(little_endian?: boolean) => number} getUint16
 *     Reads an unsigned 16-bit integer.
 * @property {(little_endian?: boolean) => number} getUint32
 *     Reads an unsigned 32-bit integer.
 * @property {() => number} getUint8
 *     Reads an unsigned 8-bit integer.
 * @property {number} length
 *     The byte length of the view.
 * @property {() => ByteCursor} rewind
 *     Resets the cursor to offset 0.
 * @property {(pos: number) => ByteCursor} seek
 *     Moves the cursor to a specific offset.
 * @property {(n: number) => ByteCursor} skip
 *     Moves the cursor by a relative offset.
 * @property {(start?: number, end?: number) => ArrayBuffer} slice
 *     Slices a copy of the buffer.
 * @property {() => number} tell
 *     Returns the current cursor position.
 * @property {DataView} view
 *     The underlying DataView.
 * @property {(bytes: Uint8Array) => ByteCursor} writeBytes
 *     Writes bytes into the buffer.
 * @property {(v: number, little_endian?: boolean) => ByteCursor} writeFloat32
 *     Writes a 32-bit float.
 * @property {(v: number, little_endian?: boolean) => ByteCursor} writeFloat64
 *     Writes a 64-bit float.
 * @property {(v: number, little_endian?: boolean) => ByteCursor} writeInt16
 *     Writes a signed 16-bit integer.
 * @property {(v: number, little_endian?: boolean) => ByteCursor} writeInt32
 *     Writes a signed 32-bit integer.
 * @property {(v: number) => ByteCursor} writeInt8
 *     Writes a signed 8-bit integer.
 * @property {(string: string) => ByteCursor} writeString
 *     Encodes and writes a UTF-8 string.
 * @property {(v: number, little_endian?: boolean) => ByteCursor} writeUint16
 *     Writes an unsigned 16-bit integer.
 * @property {(v: number, little_endian?: boolean) => ByteCursor} writeUint32
 *     Writes an unsigned 32-bit integer.
 * @property {(v: number) => ByteCursor} writeUint8
 *     Writes an unsigned 8-bit integer.
 */

/**
 * Creates a byte cursor over an ArrayBuffer with reading and writing
 * utilities.
 *
 * @param {ArrayBuffer} buffer - The ArrayBuffer to wrap.
 * @param {number} [view_offset=0] - The byte offset within the buffer to
 *     begin the view.
 * @param {number} [view_length] - The byte length of the view. Defaults to the
 *     remaining length.
 * @throws {TypeError} If buffer is not an ArrayBuffer, or if offset/length
 *     are not numbers.
 * @throws {RangeError} If offset/length are negative integers, or out of
 *     buffer bounds.
 * @returns {ByteCursor} A new ByteCursor instance.
 */
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

    /**
     * Validates that an offset and size fall within the DataView bounds.
     *
     * @param {number} offset - The starting offset within the view.
     * @param {number} size - The number of bytes to validate.
     * @throws {TypeError} If offset or size is not a number.
     * @throws {RangeError} If offset or size is negative, or if range exceeds
     *     view bounds.
     * @returns {void}
     */
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

    /**
     * Validates bounds for `size` bytes and advances the cursor position.
     *
     * @param {number} size - The number of bytes to advance.
     * @throws {TypeError} If size is not a number.
     * @throws {RangeError} If size is negative, or advancing exceeds bounds.
     * @returns {number} The cursor position prior to advancing.
     */
    function advance(size) {
        const pos = cursor;
        check(pos, size);
        cursor += size;
        return pos;
    }

    // -------------------------------------------------------------------------
    // Cursor Management
    // -------------------------------------------------------------------------

    /**
     * Resets the cursor position to 0.
     *
     * @returns {ByteCursor} The byte cursor instance.
     */
    function rewind() {
        cursor = 0;
        return self;
    }

    /**
     * Returns the current cursor position relative to the view offset.
     *
     * @returns {number} The current cursor offset.
     */
    function tell() {
        return cursor;
    }

    /**
     * Moves the cursor to the specified position.
     *
     * @param {number} pos - The target position relative to the view offset.
     * @throws {TypeError} If pos is not a number.
     * @throws {RangeError} If pos is negative or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function seek(pos) {
        check(pos, 0);
        cursor = pos;
        return self;
    }

    /**
     * Moves the cursor forward or backward by the specified number of bytes.
     *
     * @param {number} n - The number of bytes to skip.
     * @throws {TypeError} If n is not a number.
     * @throws {RangeError} If the resulting position is negative or exceeds
     *     view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function skip(n) {
        check(cursor + n, 0);
        cursor += n;
        return self;
    }

    /**
     * Checks if the cursor has reached or passed the end of the view.
     *
     * @returns {boolean} True if cursor is at or beyond view end, false
     *     otherwise.
     */
    function eof() {
        return cursor >= view.byteLength;
    }

    // -------------------------------------------------------------------------
    // Buffer / Byte Operations
    // -------------------------------------------------------------------------

    /**
     * Creates a copy of a slice of the underlying ArrayBuffer in the view.
     *
     * @param {number} [start=0] - The starting offset within the view.
     * @param {number} [end=view.byteLength] - The ending offset within the
     *     view (exclusive).
     * @throws {TypeError} If start or end is not a number.
     * @throws {RangeError} If start or end is negative, start > end, or end
     *     exceeds view bounds.
     * @returns {ArrayBuffer} A new ArrayBuffer containing the sliced bytes.
     */
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

    /**
     * Reads a sequence of bytes from current position and advances cursor.
     * Returns a zero-copy Uint8Array view over the buffer segment.
     *
     * @param {number} [len] - Number of bytes to read. Defaults to remaining
     *     bytes in view.
     * @throws {TypeError} If len is not a number when provided.
     * @throws {RangeError} If len is negative or exceeds view bounds.
     * @returns {Uint8Array} A Uint8Array view of the buffer bytes.
     */
    function getBytes(len) {
        if (len === undefined) {
            len = view.byteLength - cursor;
        }
        const pos = advance(len);
        return new Uint8Array(
            buffer,
            view.byteOffset + pos,
            len
        );
    }

    /**
     * Writes a Uint8Array into buffer at cursor position and advances cursor.
     *
     * @param {Uint8Array} bytes - The bytes to write.
     * @throws {TypeError} If bytes is not a Uint8Array.
     * @throws {RangeError} If writing the bytes exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
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

    /**
     * Reads and decodes a UTF-8 string from cursor and advances cursor.
     * Decodes directly from the buffer view without intermediary buffer slicing.
     *
     * @param {number} [length] - Number of bytes to read. Defaults to
     *     remaining bytes in view.
     * @throws {TypeError} If length is not a number when provided.
     * @throws {RangeError} If length is negative or exceeds view bounds.
     * @returns {string} The decoded UTF-8 string.
     */
    function getString(length) {
        if (length === undefined) {
            length = view.byteLength - cursor;
        }
        const pos = advance(length);
        const bytes = new Uint8Array(
            buffer,
            view.byteOffset + pos,
            length
        );
        return decoder.decode(bytes);
    }

    /**
     * Encodes string as UTF-8 directly into the buffer and advances cursor.
     *
     * @param {string} string - The string to encode and write.
     * @throws {TypeError} If string is not a string.
     * @throws {RangeError} If writing the encoded bytes exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeString(string) {
        if (typeof string !== "string") {
            throw new TypeError("writeString() requires a string");
        }
        if (typeof encoder.encodeInto === "function") {
            const target = new Uint8Array(
                buffer,
                view.byteOffset + cursor,
                view.byteLength - cursor
            );
            const result = encoder.encodeInto(string, target);
            if (result.read < string.length) {
                throw new RangeError("Offset + size exceeds view bounds");
            }
            advance(result.written);
            return self;
        }
        return writeBytes(encoder.encode(string));
    }

    // -------------------------------------------------------------------------
    // Numbers — Getters
    // -------------------------------------------------------------------------

    /**
     * Reads an unsigned 8-bit integer and advances the cursor by 1 byte.
     *
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} An unsigned 8-bit integer (0 to 255).
     */
    function getUint8() {
        return view.getUint8(advance(1));
    }

    /**
     * Reads a signed 8-bit integer and advances the cursor by 1 byte.
     *
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} A signed 8-bit integer (-128 to 127).
     */
    function getInt8() {
        return view.getInt8(advance(1));
    }

    /**
     * Reads an unsigned 16-bit integer and advances the cursor by 2 bytes.
     *
     * @param {boolean} [little_endian=false] - Whether to read little-endian.
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} An unsigned 16-bit integer (0 to 65535).
     */
    function getUint16(little_endian) {
        return view.getUint16(advance(2), Boolean(little_endian));
    }

    /**
     * Reads a signed 16-bit integer and advances the cursor by 2 bytes.
     *
     * @param {boolean} [little_endian=false] - Whether to read little-endian.
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} A signed 16-bit integer (-32768 to 32767).
     */
    function getInt16(little_endian) {
        return view.getInt16(advance(2), Boolean(little_endian));
    }

    /**
     * Reads an unsigned 32-bit integer and advances the cursor by 4 bytes.
     *
     * @param {boolean} [little_endian=false] - Whether to read little-endian.
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} An unsigned 32-bit integer (0 to 4294967295).
     */
    function getUint32(little_endian) {
        return view.getUint32(advance(4), Boolean(little_endian));
    }

    /**
     * Reads a signed 32-bit integer and advances the cursor by 4 bytes.
     *
     * @param {boolean} [little_endian=false] - Whether to read little-endian.
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} A signed 32-bit integer (-2147483648 to 2147483647).
     */
    function getInt32(little_endian) {
        return view.getInt32(advance(4), Boolean(little_endian));
    }

    /**
     * Reads a 32-bit float and advances the cursor by 4 bytes.
     *
     * @param {boolean} [little_endian=false] - Whether to read little-endian.
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} A 32-bit floating point number.
     */
    function getFloat32(little_endian) {
        return view.getFloat32(advance(4), Boolean(little_endian));
    }

    /**
     * Reads a 64-bit float and advances the cursor by 8 bytes.
     *
     * @param {boolean} [little_endian=false] - Whether to read little-endian.
     * @throws {RangeError} If the read exceeds view bounds.
     * @returns {number} A 64-bit floating point number.
     */
    function getFloat64(little_endian) {
        return view.getFloat64(advance(8), Boolean(little_endian));
    }

    // -------------------------------------------------------------------------
    // Numbers — Writers
    // -------------------------------------------------------------------------

    /**
     * Writes an unsigned 8-bit integer and advances the cursor by 1 byte.
     *
     * @param {number} v - An unsigned 8-bit integer (0 to 255).
     * @throws {TypeError} If v is not a number or not an integer.
     * @throws {RangeError} If v is out of range or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeUint8(v) {
        assert_integer(v, 0, 255, "Uint8 value");
        view.setUint8(advance(1), v);
        return self;
    }

    /**
     * Writes a signed 8-bit integer and advances the cursor by 1 byte.
     *
     * @param {number} v - A signed 8-bit integer (-128 to 127).
     * @throws {TypeError} If v is not a number or not an integer.
     * @throws {RangeError} If v is out of range or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeInt8(v) {
        assert_integer(v, -128, 127, "Int8 value");
        view.setInt8(advance(1), v);
        return self;
    }

    /**
     * Writes an unsigned 16-bit integer and advances the cursor by 2 bytes.
     *
     * @param {number} v - An unsigned 16-bit integer (0 to 65535).
     * @param {boolean} [little_endian=false] - Whether to write little-endian.
     * @throws {TypeError} If v is not a number or not an integer.
     * @throws {RangeError} If v is out of range or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeUint16(v, little_endian) {
        assert_integer(v, 0, 65535, "Uint16 value");
        view.setUint16(advance(2), v, Boolean(little_endian));
        return self;
    }

    /**
     * Writes a signed 16-bit integer and advances the cursor by 2 bytes.
     *
     * @param {number} v - A signed 16-bit integer (-32768 to 32767).
     * @param {boolean} [little_endian=false] - Whether to write little-endian.
     * @throws {TypeError} If v is not a number or not an integer.
     * @throws {RangeError} If v is out of range or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeInt16(v, little_endian) {
        assert_integer(v, -32768, 32767, "Int16 value");
        view.setInt16(advance(2), v, Boolean(little_endian));
        return self;
    }

    /**
     * Writes an unsigned 32-bit integer and advances the cursor by 4 bytes.
     *
     * @param {number} v - An unsigned 32-bit integer (0 to 4294967295).
     * @param {boolean} [little_endian=false] - Whether to write little-endian.
     * @throws {TypeError} If v is not a number or not an integer.
     * @throws {RangeError} If v is out of range or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeUint32(v, little_endian) {
        assert_integer(v, 0, 4294967295, "Uint32 value");
        view.setUint32(advance(4), v, Boolean(little_endian));
        return self;
    }

    /**
     * Writes a signed 32-bit integer and advances the cursor by 4 bytes.
     *
     * @param {number} v - A signed 32-bit integer (-2147483648 to 2147483647).
     * @param {boolean} [little_endian=false] - Whether to write little-endian.
     * @throws {TypeError} If v is not a number or not an integer.
     * @throws {RangeError} If v is out of range or exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeInt32(v, little_endian) {
        assert_integer(v, -2147483648, 2147483647, "Int32 value");
        view.setInt32(advance(4), v, Boolean(little_endian));
        return self;
    }

    /**
     * Writes a 32-bit float and advances the cursor by 4 bytes.
     *
     * @param {number} v - The floating point number to write.
     * @param {boolean} [little_endian=false] - Whether to write little-endian.
     * @throws {TypeError} If v is not a number.
     * @throws {RangeError} If writing exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
    function writeFloat32(v, little_endian) {
        if (typeof v !== "number") {
            throw new TypeError("Float32 value must be a number");
        }
        view.setFloat32(advance(4), v, Boolean(little_endian));
        return self;
    }

    /**
     * Writes a 64-bit float and advances the cursor by 8 bytes.
     *
     * @param {number} v - The floating point number to write.
     * @param {boolean} [little_endian=false] - Whether to write little-endian.
     * @throws {TypeError} If v is not a number.
     * @throws {RangeError} If writing exceeds view bounds.
     * @returns {ByteCursor} The byte cursor instance.
     */
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
