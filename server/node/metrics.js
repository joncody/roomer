/**
 * @fileoverview Telemetry and observability hooks.
 */

/**
 * Creates a no-op metrics collector.
 *
 * @returns {Readonly<object>} Frozen no-op metrics instance.
 */
function create_nop_metrics() {
    return Object.freeze({
        onClusterDropped: function () {},
        onClusterPublish: function () {},
        onClusterReceived: function () {},
        onConnect: function () {},
        onDisconnect: function () {},
        onMessageDropped: function () {},
        onMessageReceived: function () {},
        onMessageSent: function () {},
        onRoomCreated: function () {},
        onRoomDeleted: function () {}
    });
}

/**
 * Creates an in-memory atomic-style metrics tracker.
 *
 * @returns {Readonly<object>} Frozen in-memory metrics instance.
 */
function create_in_memory_metrics() {
    let active_connections = 0;
    let total_connections = 0;
    let active_rooms = 0;
    let total_rooms = 0;
    let messages_sent = 0;
    let messages_received = 0;
    let messages_dropped = 0;
    let bytes_sent = 0;
    let bytes_received = 0;
    let cluster_published = 0;
    let cluster_received = 0;

    function onConnect() {
        active_connections += 1;
        total_connections += 1;
    }

    function onDisconnect() {
        if (active_connections > 0) {
            active_connections -= 1;
        }
    }

    function onMessageSent(bytes) {
        messages_sent += 1;
        if (typeof bytes === "number") {
            bytes_sent += bytes;
        }
    }

    function onMessageReceived(bytes) {
        messages_received += 1;
        if (typeof bytes === "number") {
            bytes_received += bytes;
        }
    }

    function onMessageDropped() {
        messages_dropped += 1;
    }

    function onRoomCreated() {
        active_rooms += 1;
        total_rooms += 1;
    }

    function onRoomDeleted() {
        if (active_rooms > 0) {
            active_rooms -= 1;
        }
    }

    function onClusterPublish() {
        cluster_published += 1;
    }

    function onClusterReceived() {
        cluster_received += 1;
    }

    function onClusterDropped() {}

    function getStats() {
        return Object.freeze({
            active_connections,
            active_rooms,
            bytes_received,
            bytes_sent,
            cluster_published,
            cluster_received,
            messages_dropped,
            messages_received,
            messages_sent,
            total_connections,
            total_rooms
        });
    }

    return Object.freeze({
        getStats,
        onClusterDropped,
        onClusterPublish,
        onClusterReceived,
        onConnect,
        onDisconnect,
        onMessageDropped,
        onMessageReceived,
        onMessageSent,
        onRoomCreated,
        onRoomDeleted
    });
}

export {
    create_in_memory_metrics,
    create_nop_metrics
};
