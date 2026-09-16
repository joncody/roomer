import { EventEmitter } from "./emitter";

export interface Packet {
    dst: string;
    event: string;
    flags: number;
    payload: Uint8Array;
    room: string;
    src: string;
    version: number;
}

export interface RoomerOptions {
    /** Whether to automatically reconnect on abrupt connection drop. Default: true */
    reconnect?: boolean;
    /** Initial reconnect delay in ms. Default: 500 */
    initial_delay?: number;
    /** Maximum backoff reconnect delay in ms. Default: 5000 */
    max_delay?: number;
    /** Optional predicate to decide if client should reconnect based on close code and reason. */
    should_reconnect?: (code: number, reason: string) => boolean;
}

export interface Room extends EventEmitter {
    readonly name: string;
    bufferedAmount(): number;
    clearListeners(exceptions?: string[]): Room;
    close?(code?: number, reason?: string): Room;
    forceClose(is_disconnect?: boolean, code?: number, reason?: string): Room;
    id(): string;
    join(room_name: string): Room;
    leave(): Room;
    members(): string[];
    open(): boolean;
    parse(packet: Packet): void;
    purge?(): Room;
    readyState(): "connecting" | "open" | "closing" | "closed";
    rooms?(): Readonly<Record<string, Room>>;
    send(event: string, payload?: string | ArrayBuffer | Uint8Array | object | number | boolean, dst?: string): Room;
    url(): string;
}

declare function roomer(url: string, options?: RoomerOptions): Room;

export default roomer;
