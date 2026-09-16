# `roomer-server` – Node.js Server Implementation

[![npm version](https://img.shields.io/npm/v/roomer-server.svg?style=flat&color=CB3837&logo=npm)](https://www.npmjs.com/package/roomer-server)
[![Node Version](https://img.shields.io/badge/Node.js-18+-339933?style=flat&logo=node.js&logoColor=white)](https://nodejs.org/)
[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

High-performance, functional Node.js server implementation of the Roomer WebSocket framework with 12-byte zero-copy binary framing, Crockfordian functional encapsulation, pluggable Redis cluster SET presence with auto-expiration, token-bucket control-plane rate limiting, true wire-level unicast routing, explicit RFC 6455 close control, and native libuv backpressure control.

> 📖 **For Wire Protocol specifications and Client API documentation, see the [Root README](../../README.md).**

---

## 📦 Scope & Architecture

The `server/node` package provides the backend coordinator (`create_hub`), connection handles (`create_conn`), room registries, and distributed cluster adapters for Node.js environments.

```text
               +---------------------------------------------------+
               |             Node.js HTTP Server & ws              |
               |     (Nagle Bypassed, clientTracking Disabled)     |
               +-------------------------+-------------------------+
                                         |
               +-------------------------v-------------------------+
               |               Hub Coordinator                     |
               |  - Prototype-Free Dictionaries (Object.create)    |
               |  - Token-Bucket Control Rate Limiter              |
               +-------------------+-------------------+-----------+
                                   |                   |
                     +-------------v----+        +-----v-------------+
                     | Connection Handles|       | Room Registries   |
                     | (Kernel Drain)   |        | (Local Fanout)    |
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |        Pluggable Distributed Adapter (Redis)      |
               |  - Auto-Expiring SET Presence (SADD, SREM)        |
               |  - True Wire-Level Unicast (SUBSCRIBE prefix:node)|
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
```

---

## 🧠 Quick Start

```javascript
import http from "node:http";
import {
    create_hub,
    create_roomer_server,
    BACKPRESSURE
} from "./index.js";

const hub = create_hub();

// 1. Register custom event handlers
hub.register_handler("chat", function (conn, msg) {
    // Broadcast to all room members except sender
    conn.send_to_room(msg.room, msg.event, msg.payload);
});

// 2. Mount WebSocket handler on HTTP server
const server = http.createServer();
create_roomer_server(server, {
    hub,
    channel_capacity: 2048,
    backpressure: BACKPRESSURE.DROP_SLOW_CLIENT,
    control_rate_limit: 10.0,
    control_burst: 20,
    max_message_size: 16 * 1024 * 1024
});

// 3. Graceful Shutdown
function shutdown() {
    console.log("Shutting down server gracefully...");
    hub.shutdown().then(function () {
        server.close(function () {
            process.exit(0);
        });
    });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

server.listen(8080, function () {
    console.log("Roomer Node.js server running on ws://localhost:8080/ws");
});
```

---

## 📚 API Reference

### `Conn` Instance Methods
| Method | Description |
|---|---|
| `conn.id` | Unique UUID string assigned to connection. |
| `conn.claims` | Read-only object of authenticated claims extracted during handshake. |
| `conn.allow_control_event()` | Evaluates token-bucket rate limiter for control operations. |
| `conn.send_to_room(room, event, payload)` | Broadcasts message to room members **except sender** (local + cluster). |
| `conn.send_to_client(dst_id, event, payload)` | Sends direct message to client ID via isolated node unicast. |
| `conn.try_send(msg_buffer)` | Non-blocking frame transmission with backpressure policy. |
| `conn.close_with(code, reason)` | Sends an RFC 6455 close frame with status code and reason, then cleans up. |
| `conn.is_in_room(room)` | Checks if connection is currently tracked in a room. |
| `conn.joined_rooms()` | Returns an array copy of all joined room names. |
| `conn.cleanup()` | Safely removes connection from all rooms and terminates socket. |

### `Hub` Instance Methods
| Method | Description |
|---|---|
| `hub.register_handler(event, fn)` | Registers a custom message handler callback. |
| `hub.broadcast_room(exclude_id, msg)` | Broadcasts message to room members and cluster adapter. |
| `hub.get_cluster_presence(room)` | Retrieves all connection IDs in a room across the cluster using SMEMBERS. |
| `hub.send_direct_to_cluster(msg)` | Routes a direct message via isolated node unicast. |
| `hub.disconnect(conn_id, code, reason)` | Disconnects an active connection by ID with a custom close code and reason. |
| `hub.shutdown()` | Broadcasts `1001 Going Away` close frames and closes adapters. |

---

## 🧪 Testing & Benchmarks

```bash
npm test
```
