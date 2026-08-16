/**
 * @fileoverview Lightweight, functional event emitter library adhering
 * to strict Crockfordian and Node.js-style EventEmitter conventions.
 */

/**
 * @typedef {Object} EventEmitter
 * @property {(type: string, fn: Function) => EventEmitter} addListener
 *     Appends a listener callback for the specified event type.
 * @property {(type: string, ...args: *) => boolean} emit
 *     Synchronously calls each listener registered for the event.
 * @property {(type?: string) => Function[]} listeners
 *     Returns an array of listeners for an event type or all listeners.
 * @property {(type: string, fn: Function) => EventEmitter} off
 *     Alias for removeListener.
 * @property {(type: string, fn: Function) => EventEmitter} on
 *     Alias for addListener.
 * @property {(type: string, fn: Function) => EventEmitter} once
 *     Adds a one-time listener callback for the specified event type.
 * @property {(type?: string) => EventEmitter} removeAllListeners
 *     Removes all listeners or those for a specified event type.
 * @property {(type: string, fn: Function) => EventEmitter} removeListener
 *     Removes a listener callback for the specified event type.
 */

/**
 * Creates an event emitter or mixes emitter methods into a target
 * object.
 *
 * @param {Object} [target] - Optional target object to augment with
 *     emitter methods.
 * @returns {Readonly<EventEmitter>} The frozen event emitter instance.
 */
function create_emitter(target) {
    const events = Object.create(null);
    let self;

    /**
     * Synchronously invokes all listeners registered for the event.
     *
     * @param {string} type - Event type name.
     * @param {...*} args - Arguments passed to each listener callback.
     * @returns {boolean} True if the event had listeners, false otherwise.
     */
    function emit(type, ...args) {
        if (typeof type !== "string") {
            return false;
        }
        const list = events[type];
        if (!Array.isArray(list) || list.length === 0) {
            return false;
        }
        const copy = list.slice();
        copy.forEach(function (fn) {
            fn(...args);
        });
        return true;
    }

    /**
     * Appends a listener callback for the specified event type.
     *
     * @param {string} type - Event type name.
     * @param {Function} listener - Callback function to invoke.
     * @returns {EventEmitter} The event emitter instance.
     */
    function addListener(type, listener) {
        if (typeof type !== "string" || typeof listener !== "function") {
            return self;
        }
        if (
            Array.isArray(events.newListener) &&
            events.newListener.length > 0
        ) {
            const reported = (
                typeof listener.listener === "function"
                ? listener.listener
                : listener
            );
            emit("newListener", type, reported);
        }
        if (!Array.isArray(events[type])) {
            events[type] = [listener];
        } else {
            events[type].push(listener);
        }
        return self;
    }

    /**
     * Removes a listener callback for the specified event type.
     *
     * @param {string} type - Event type name.
     * @param {Function} listener - Callback function to remove.
     * @returns {EventEmitter} The event emitter instance.
     */
    function removeListener(type, listener) {
        if (typeof type !== "string" || typeof listener !== "function") {
            return self;
        }
        const list = events[type];
        if (!Array.isArray(list) || list.length === 0) {
            return self;
        }
        const index = list.findIndex(function (v) {
            return (
                v === listener ||
                (v.listener !== undefined && v.listener === listener)
            );
        });
        if (index >= 0) {
            const removed = list[index];
            list.splice(index, 1);
            if (list.length === 0) {
                delete events[type];
            }
            if (
                Array.isArray(events.removeListener) &&
                events.removeListener.length > 0
            ) {
                const reported = (
                    typeof removed.listener === "function"
                    ? removed.listener
                    : removed
                );
                emit("removeListener", type, reported);
            }
        }
        return self;
    }

    /**
     * Adds a one-time listener callback for the specified event type.
     *
     * @param {string} type - Event type name.
     * @param {Function} listener - Callback function to invoke once.
     * @returns {EventEmitter} The event emitter instance.
     */
    function once(type, listener) {
        if (typeof type !== "string" || typeof listener !== "function") {
            return self;
        }
        function onetime(...args) {
            removeListener(type, onetime);
            listener(...args);
        }
        onetime.listener = listener;
        return addListener(type, onetime);
    }

    /**
     * Removes all listeners or those for a specified event type.
     *
     * @param {string} [type] - Optional event type name.
     * @returns {EventEmitter} The event emitter instance.
     */
    function removeAllListeners(type) {
        if (type === undefined) {
            if (
                !Array.isArray(events.removeListener) ||
                events.removeListener.length === 0
            ) {
                Object.keys(events).forEach(function (key) {
                    delete events[key];
                });
            } else {
                Object.keys(events).forEach(function (key) {
                    if (key !== "removeListener") {
                        removeAllListeners(key);
                    }
                });
                removeAllListeners("removeListener");
            }
            return self;
        }
        if (typeof type !== "string") {
            return self;
        }
        const list = events[type];
        if (Array.isArray(list)) {
            const copy = list.slice();
            copy.forEach(function (fn) {
                removeListener(type, fn);
            });
        }
        return self;
    }

    /**
     * Returns array of listeners for an event type or all listeners.
     *
     * @param {string} [type] - Optional event type name.
     * @returns {Function[]} Array of listener functions.
     */
    function listeners(type) {
        if (type === undefined) {
            const all = [];
            Object.keys(events).forEach(function (key) {
                events[key].forEach(function (v) {
                    all.push(
                        typeof v.listener === "function"
                        ? v.listener
                        : v
                    );
                });
            });
            return all;
        }
        if (typeof type === "string") {
            const list = events[type];
            if (Array.isArray(list)) {
                return list.map(function (v) {
                    return (
                        typeof v.listener === "function"
                        ? v.listener
                        : v
                    );
                });
            }
            return [];
        }
        return [];
    }

    const methods = {
        addListener,
        emit,
        listeners,
        off: removeListener,
        on: addListener,
        once,
        removeAllListeners,
        removeListener
    };

    if (typeof target === "object" && target !== null) {
        self = Object.assign({}, target, methods);
    } else {
        self = Object.assign({}, methods);
    }

    return Object.freeze(self);
}

export default Object.freeze(create_emitter);
