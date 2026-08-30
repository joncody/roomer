import { EventEmitter } from "./emitter";

export interface Packet {
    dst: string;
    event: string;
    payload: Uint8Array;
    room: string;
    src: string;
}

export interface RoomerOptions {
    /** Whether to automatically reconnect on abrupt connection drop. Default: true */
    reconnect?: boolean;
    /** Initial reconnect delay in ms. Default: 500 */
    initial_delay?: number;
    /** Maximum backoff reconnect delay in ms. Default: 5000 */
    max_delay?: number;
}

export interface Room extends EventEmitter {
    readonly name: string;
    clearListeners(exceptions?: string[]): Room;
    forceClose(is_disconnect?: boolean): Room;
    id(): string;
    join(room_name: string): Room;
    leave(): Room;
    members(): string[];
    open(): boolean;
    parse(packet: Packet): void;
    purge?(): Room;
    rooms?(): Readonly<Record<string, Room>>;
    send(event: string, payload?: string | ArrayBuffer | Uint8Array | object | number | boolean, dst?: string): Room;
}

declare function roomer(url: string, options?: RoomerOptions): Room;

export default roomer;
