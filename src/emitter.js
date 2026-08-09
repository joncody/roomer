function create_emitter(target) {
    const events = Object.create(null);
    let self;

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
