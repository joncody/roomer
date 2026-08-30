export interface ByteCursor {
    readonly buffer: ArrayBuffer;
    readonly length: number;
    readonly view: DataView;
    eof(): boolean;
    getBytes(len?: number): Uint8Array;
    getFloat32(little_endian?: boolean): number;
    getFloat64(little_endian?: boolean): number;
    getInt16(little_endian?: boolean): number;
    getInt32(little_endian?: boolean): number;
    getInt8(): number;
    getString(length?: number): string;
    getUint16(little_endian?: boolean): number;
    getUint32(little_endian?: boolean): number;
    getUint8(): number;
    rewind(): ByteCursor;
    seek(pos: number): ByteCursor;
    skip(n: number): ByteCursor;
    slice(start?: number, end?: number): ArrayBuffer;
    tell(): number;
    writeBytes(bytes: Uint8Array): ByteCursor;
    writeFloat32(v: number, little_endian?: boolean): ByteCursor;
    writeFloat64(v: number, little_endian?: boolean): ByteCursor;
    writeInt16(v: number, little_endian?: boolean): ByteCursor;
    writeInt32(v: number, little_endian?: boolean): ByteCursor;
    writeInt8(v: number): ByteCursor;
    writeString(string: string): ByteCursor;
    writeUint16(v: number, little_endian?: boolean): ByteCursor;
    writeUint32(v: number, little_endian?: boolean): ByteCursor;
    writeUint8(v: number): ByteCursor;
}

declare function bytecursor(
    buffer: ArrayBuffer,
    view_offset?: number,
    view_length?: number
): ByteCursor;

export default bytecursor;
