# `roomer-server` – Node.js Server Implementation

[![npm version](https://img.shields.io/npm/v/roomer-server.svg?style=flat&color=CB3837&logo=npm)](https://www.npmjs.com/package/roomer-server)
[![Node Version](https://img.shields.io/badge/Node.js-18+-339933?style=flat&logo=node.js&logoColor=white)](https://nodejs.org/)
[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

High-performance, functional Node.js server implementation of the Roomer WebSocket framework with zero-intermediate allocation binary framing, Crockfordian functional encapsulation, pluggable Redis cluster presence with heartbeat touching, true wire-level unicast routing, and native libuv backpressure control.

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
               |  - Clean Closure Scopes (Zero this / Zero class)  |
               +-------------------+-------------------+-----------+
                                   |                   |
                     +-------------v----+        +-----v-------------+
                     | Connection Handles|       | Room Registries   |
                     | (Kernel Drain)   |        | (Local Fanout)    |
                     +------------------+        +-------------------+
                                   |                   |
               +-------------------v-------------------v-----------+
               |        Pluggable Distributed Adapter (Redis)      |
               |  - Presence Sets (ZSET Heartbeat Touch on Pong)   |
               |  - True Wire-Level Unicast (SUBSCRIBE prefix:node)|
               |  - Loopback-Suppressed Broadcast (PUBLISH)        |
               +---------------------------------------------------+
```

- **Pure Functional Encapsulation**: Zero `class`, zero `this`, and zero prototype modification. Built with closure-based factory functions returning frozen interfaces (`Object.freeze(self)`).
- **True Wire-Level Unicast**: Direct node messages route via dedicated `SUBSCRIBE prefix:node:<nodeID>` channels, preventing bystander cluster nodes from receiving or parsing direct traffic over the wire.
- **Presence Heartbeat Touching**: WebSocket Pong frames refresh connection timestamps in Redis ZSET presence sets via batched pipelines every 54 seconds.
- **Configurable Backpressure**: Choose between `DROP_SLOW_CLIENT` (default memory protection), `DROP_OLDEST`, and `DROP_NEWEST`.
- **Zero Redis Memory Leaks**: Pure Pub/Sub routing keeps Redis completely stateless—no persistent stream radix trees, unread entry accumulation, or dead consumer groups.
- **Single-Allocation Binary Framing**: Uses `Buffer.byteLength()` and in-place `buf.write()` to pack binary frames in a **single heap allocation** without intermediate buffer copies.
- **Kernel-Level Stream Optimization**: Disables Nagle's algorithm (`setNoDelay(true)`) and disables redundant `ws` client tracking for minimal GC overhead.

---

## 🚀 Installation

```bash
cd server/node
npm install
npm install ioredis # Optional for multi-node clustering
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
    presence_touch_interval: 54000,
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

## 🌐 Distributed Clustering (Redis Adapter)

The Redis clustering adapter provides **loopback suppression**, **cluster presence synchronization with heartbeat touches**, and **wire-level isolated unicast routing** using binary buffer streaming (`publish` and `pmessageBuffer`/`messageBuffer`):

```javascript
import http from "node:http";
import Redis from "ioredis";
import {
    create_hub,
    create_redis_adapter,
    create_roomer_server
} from "./index.js";

const pub_client = new Redis("localhost:6379");
const sub_client = pub_client.duplicate();

const adapter = create_redis_adapter(pub_client, sub_client, {
    prefix: "roomer:demo:",
    presence_ttl: 180 // Prune inactive presence members after 3 minutes
});

const hub = create_hub();
await hub.configure(adapter);

const server = http.createServer();
create_roomer_server(server, { hub });

server.listen(8080, function () {
    console.log("Clustered Node.js node running on ws://localhost:8080/ws");
});
```

---

## 📚 API Reference

### `create_roomer_server(http_server, options)` Options
| Option | Default | Description |
|---|---|---|
| `hub` | `create_hub()` | Custom Hub coordinator instance. |
| `authorize` | `undefined` | Handshake function `async (req) => claims`. |
| `max_message_size` | `16 MB` | Maximum allowed WebSocket frame size in bytes. |
| `channel_capacity` | `8192` | Outbound message queue capacity factor before backpressure activates. |
| `backpressure` | `BACKPRESSURE.DROP_SLOW_CLIENT` | Backpressure policy: `DROP_SLOW_CLIENT`, `DROP_OLDEST`, `DROP_NEWEST`. |
| `ping_interval` | `54000` (54s) | Keep-alive heartbeat ping interval in milliseconds. |
| `presence_touch_interval` | `54000` (54s) | Minimum interval in ms between presence score updates on Pong frames. |

### `Conn` Instance Methods
| Method | Description |
|---|---|
| `conn.id` | Unique UUID string assigned to connection. |
| `conn.claims` | Read-only object of authenticated claims extracted during handshake. |
| `conn.send_to_room(room, event, payload)` | Broadcasts message to room members **except sender** (local + cluster). |
| `conn.send_to_client(dst_id, event, payload)` | Sends direct message to client ID via isolated node unicast. |
| `conn.try_send(msg_buffer)` | Non-blocking frame transmission with backpressure policy. |
| `conn.touch_presence(interval)` | Updates last-seen presence heartbeat score across joined rooms. |
| `conn.is_in_room(room)` | Checks if connection is currently tracked in a room. |
| `conn.joined_rooms()` | Returns an array copy of all joined room names. |
| `conn.cleanup()` | Safely removes connection from all rooms and terminates socket. |

### `Hub` Instance Methods
| Method | Description |
|---|---|
| `hub.register_handler(event, fn)` | Registers a custom message handler callback. |
| `hub.broadcast_room(exclude_id, msg)` | Broadcasts message to room members and cluster adapter. |
| `hub.get_cluster_presence(room)` | Retrieves all connection IDs in a room across the cluster. |
| `hub.send_direct_to_cluster(msg)` | Routes a direct message via isolated node unicast. |
| `hub.touch_presence(conn_id, rooms)` | Touches cluster presence score for a connection across rooms. |
| `hub.shutdown()` | Broadcasts `1001 Going Away` close frames and closes adapters. |

---

## 🧪 Testing & Benchmarks

### 1. Run Unit Tests & Integration Tests
```bash
npm test
```

### 2. Browser Test Suite & Interactive Demo
```bash
npm start
```
- **Interactive Chat Demo:** [http://localhost:8080/](http://localhost:8080/)
- **Automated Browser Test Suite:** [http://localhost:8080/tests/](http://localhost:8080/tests/)

---

## 📄 License

Roomer is open-source software licensed under the [MIT License](../../LICENSE).
