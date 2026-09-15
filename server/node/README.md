# `roomer-server` – Node.js Server Implementation

[![npm version](https://img.shields.io/npm/v/roomer-server.svg?style=flat&color=CB3837&logo=npm)](https://www.npmjs.com/package/roomer-server)
[![Node Version](https://img.shields.io/badge/Node.js-18+-339933?style=flat&logo=node.js&logoColor=white)](https://nodejs.org/)
[![JavaScript](https://img.shields.io/badge/JavaScript-ES6+-F7DF1E?style=flat&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](../../LICENSE)

High-performance, functional Node.js server implementation of the Roomer WebSocket framework with 12-byte zero-copy binary framing, Crockfordian functional encapsulation, pluggable Redis cluster SET presence with auto-expiration, token-bucket control-plane rate limiting, true wire-level unicast routing, and native libuv backpressure control.

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

- **Pure Functional Encapsulation**: Zero `class`, zero `this`, and zero prototype modification. Built with closure-based factory functions returning frozen interfaces (`Object.freeze(self)`).
- **12-Byte Binary Wire Framing**: Packets serialize directly with a 2-byte header and big-endian length prefixes, reducing header overhead to 12 bytes.
- **Token-Bucket Control Protection**: Enforces token-bucket rate limiting on `join` and `leave` control operations per connection to protect Redis from control storms.
- **True Wire-Level Unicast**: Direct node messages route via dedicated `SUBSCRIBE prefix:node:<nodeID>` channels, preventing bystander cluster nodes from receiving or parsing direct traffic over the wire.
- **Auto-Expiring SET Presence**: Redis plain SETs (`SADD`, `SREM`, `SMEMBERS`) with key expiration auto-evict abandoned rooms on node crashes without heartbeat touching.
- **Configurable Backpressure**: Choose between `DROP_SLOW_CLIENT` (default memory protection), `DROP_OLDEST`, and `DROP_NEWEST`.
- **Zero Redis Memory Leaks**: Pure Pub/Sub routing keeps Redis completely stateless—no persistent stream radix trees, unread entry accumulation, or dead consumer groups.
- **Early Size Guarding**: Max payload validation enforces frame size limits before allocating or decoding packet structures.

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

## 🌐 Distributed Clustering (Redis Adapter)

The Redis clustering adapter provides **loopback suppression**, **auto-expiring SET presence**, and **wire-level isolated unicast routing**:

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
    presence_ttl: 180 // Key expiration in seconds on AddPresence
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
| `control_rate_limit` | `10.0` | Token-bucket refill rate (tokens/sec) for join/leave events. |
| `control_burst` | `20` | Token-bucket max burst capacity for join/leave events. |
| `ping_interval` | `54000` (54s) | Keep-alive heartbeat ping interval in milliseconds. |

### `Conn` Instance Methods
| Method | Description |
|---|---|
| `conn.id` | Unique UUID string assigned to connection. |
| `conn.claims` | Read-only object of authenticated claims extracted during handshake. |
| `conn.allow_control_event()` | Evaluates token-bucket rate limiter for control operations. |
| `conn.send_to_room(room, event, payload)` | Broadcasts message to room members **except sender** (local + cluster). |
| `conn.send_to_client(dst_id, event, payload)` | Sends direct message to client ID via isolated node unicast. |
| `conn.try_send(msg_buffer)` | Non-blocking frame transmission with backpressure policy. |
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
| `hub.shutdown()` | Broadcasts `1001 Going Away` close frames and closes adapters. |

---

## 🧪 Testing & Benchmarks

```bash
npm test
```
