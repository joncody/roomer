/**
 * @fileoverview Standard Adapter interface definition and default local adapter.
 *
 * Pluggable Adapter Specification:
 * Any custom adapter must return a frozen object implementing these methods:
 * - publish(room, msg)                     : Promise<void>
 * - publish_raw(room, raw_msg_buffer)     : Promise<void>
 * - publish_direct(target_node_id, msg)   : Promise<void>
 * - publish_direct_raw(target_node_id, b) : Promise<void>
 * - subscribe(callback(channel, src, raw)): Promise<void>
 * - add_presence(room, conn_id)           : Promise<void>
 * - remove_presence(room, conn_id)        : Promise<void>
 * - get_presence(room)                    : Promise<string[]>
 * - register_node(conn_id)                : Promise<void>
 * - unregister_node(conn_id)              : Promise<void>
 * - get_node_for_conn(conn_id)            : Promise<string|null>
 * - node_id()                             : string
 * - close()                               : Promise<void>
 */

/**
 * Creates the default in-memory adapter for standalone single-node servers.
 *
 * @param {string} [custom_node_id="local-node"] - Optional node identifier.
 * @returns {Readonly<object>} Frozen local adapter instance.
 */
function create_local_adapter(custom_node_id) {
    const node_id_val = (
        typeof custom_node_id === "string" && custom_node_id !== ""
        ? custom_node_id
        : "local-node"
    );
    const presence_map = Object.create(null);
    const node_map = Object.create(null);

    async function publish() {}
    async function publish_raw() {}
    async function publish_direct() {}
    async function publish_direct_raw() {}
    async function subscribe() {}

    async function add_presence(room, conn_id) {
        if (presence_map[room] === undefined) {
            presence_map[room] = Object.create(null);
        }
        presence_map[room][conn_id] = true;
    }

    async function remove_presence(room, conn_id) {
        if (presence_map[room] !== undefined) {
            delete presence_map[room][conn_id];
            if (Object.keys(presence_map[room]).length === 0) {
                delete presence_map[room];
            }
        }
    }

    async function get_presence(room) {
        if (presence_map[room] === undefined) {
            return [];
        }
        return Object.keys(presence_map[room]);
    }

    async function register_node(conn_id) {
        node_map[conn_id] = node_id_val;
    }

    async function unregister_node(conn_id) {
        delete node_map[conn_id];
    }

    async function get_node_for_conn(conn_id) {
        return (
            node_map[conn_id] !== undefined
            ? node_map[conn_id]
            : null
        );
    }

    function node_id() {
        return node_id_val;
    }

    async function close() {}

    return Object.freeze({
        add_presence,
        close,
        get_node_for_conn,
        get_presence,
        node_id,
        publish,
        publish_direct,
        publish_direct_raw,
        publish_raw,
        register_node,
        remove_presence,
        subscribe,
        unregister_node
    });
}

export {
    create_local_adapter
};
