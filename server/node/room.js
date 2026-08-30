/**
 * @fileoverview Room state container and local connection fanout.
 */

/**
 * Creates a room container.
 *
 * @param {string} name - Channel or room name.
 * @returns {Readonly<object>} Frozen room instance.
 */
function create_room(name) {
    const members = Object.create(null);

    function add_member(conn) {
        members[conn.id] = conn;
    }

    function remove_member(conn_id) {
        delete members[conn_id];
        return Object.keys(members).length === 0;
    }

    function emit_local(exclude_id, raw_msg) {
        Object.keys(members).forEach(function (id) {
            if (exclude_id === null || id !== exclude_id) {
                members[id].try_send(raw_msg);
            }
        });
    }

    function snapshot() {
        return Object.keys(members);
    }

    function is_empty() {
        return Object.keys(members).length === 0;
    }

    function len() {
        return Object.keys(members).length;
    }

    return Object.freeze({
        add_member,
        emit_local,
        is_empty,
        len,
        name,
        remove_member,
        snapshot
    });
}

export {
    create_room
};
