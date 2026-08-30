export interface EventEmitter {
    addListener(type: string, fn: (...args: any[]) => void): EventEmitter;
    emit(type: string, ...args: any[]): boolean;
    listeners(type?: string): Function[];
    off(type: string, fn: (...args: any[]) => void): EventEmitter;
    on(type: string, fn: (...args: any[]) => void): EventEmitter;
    once(type: string, fn: (...args: any[]) => void): EventEmitter;
    removeAllListeners(type?: string): EventEmitter;
    removeListener(type: string, fn: (...args: any[]) => void): EventEmitter;
}

declare function create_emitter<T extends object = {}>(target?: T): Readonly<T & EventEmitter>;

export default create_emitter;
