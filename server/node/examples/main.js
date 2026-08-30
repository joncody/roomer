/**
 * @fileoverview Standalone demonstration server serving static HTML/JS demo,
 * automated browser test suite, and clustered WebSocket endpoint.
 */

import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Redis from "ioredis";
import {
    create_hub,
    create_in_memory_metrics,
    create_redis_adapter,
    create_roomer_server
} from "../index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = Number(process.env.PORT) || 8080;
const REDIS_ADDR = process.env.REDIS_ADDR || "";
const REDIS_PREFIX = process.env.REDIS_PREFIX || "roomer:demo:";

function find_file(candidates) {
    let found = candidates[0];
    candidates.forEach(function (c) {
        if (fs.existsSync(c) === true) {
            found = c;
        }
    });
    return found;
}

const hub = create_hub();
const metrics = create_in_memory_metrics();
let redis_pub = null;
let redis_sub = null;

// 1. Configure Redis Adapter if REDIS_ADDR is present
if (REDIS_ADDR !== "") {
    let redis_url = REDIS_ADDR;
    if (
        redis_url.startsWith("redis://") === false &&
        redis_url.startsWith("rediss://") === false
    ) {
        redis_url = "redis://" + redis_url;
    }

    console.log("Connecting to Redis cluster at " + redis_url + "...");
    redis_pub = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 3 });
    redis_sub = new Redis(redis_url, { lazyConnect: true, maxRetriesPerRequest: 3 });

    try {
        await redis_pub.connect();
        await redis_sub.connect();
        const adapter = create_redis_adapter(redis_pub, redis_sub, {
            prefix: REDIS_PREFIX
        });
        await hub.configure(adapter, metrics);
        console.log("Connected to Redis cluster with node ID: " + adapter.node_id());
    } catch (err) {
        console.warn("Could not connect to Redis; running in standalone mode (" + err.message + ")");
        await hub.configure(undefined, metrics);
    }
} else {
    await hub.configure(undefined, metrics);
}

// 2. High-throughput "chat" message handler (no synchronous logging)
hub.register_handler("chat", function (conn, msg) {
    conn.send_to_room(msg.room, msg.event, msg.payload);
});

// 3. HTTP Server serving static files, browser test suite, and interactive demo
const server = http.createServer(function (req, res) {
    const url = req.url || "/";
    const base_path = path.resolve(__dirname, "../../..");
    let file_path;

    if (url === "/" || url === "/index.html") {
        file_path = find_file([
            path.join(base_path, "examples/index.html"),
            path.join(process.cwd(), "examples/index.html")
        ]);
    } else if (url.startsWith("/client/")) {
        file_path = path.join(base_path, url);
    } else if (url.startsWith("/static/")) {
        file_path = path.join(base_path, "examples", url);
    } else if (url.startsWith("/tests/")) {
        const sub = url.replace(/^\/tests\/?/, "") || "index.html";
        file_path = path.join(base_path, "tests", sub);
    } else {
        res.writeHead(404);
        res.end("Not Found");
        return;
    }

    fs.readFile(file_path, function (err, data) {
        if (err) {
            res.writeHead(404);
            res.end("Not Found");
            return;
        }

        if (file_path.endsWith(".html")) {
            res.setHeader("Content-Type", "text/html");
        } else if (file_path.endsWith(".js")) {
            res.setHeader("Content-Type", "application/javascript");
        } else if (file_path.endsWith(".css")) {
            res.setHeader("Content-Type", "text/css");
        }

        res.writeHead(200);
        res.end(data);
    });
});

// 4. Mount WebSocket Server with 8,192 Channel Capacity
create_roomer_server(server, {
    channel_capacity: 8192,
    hub,
    max_message_size: 16 * 1024 * 1024
});

// 5. Graceful Server Shutdown
async function graceful_shutdown() {
    console.log("Shutting down server gracefully (sending 1001 close frames)...");
    await hub.shutdown();
    server.close(function () {
        if (redis_pub !== null) redis_pub.disconnect();
        if (redis_sub !== null) redis_sub.disconnect();
        console.log("Server shutdown complete.");
        process.exit(0);
    });
}

process.on("SIGINT", graceful_shutdown);
process.on("SIGTERM", graceful_shutdown);

server.listen(PORT, function () {
    console.log("Roomer Node.js Server listening on http://localhost:" + PORT);
    console.log("Interactive Demo:    http://localhost:" + PORT + "/");
    console.log("Browser Test Suite:  http://localhost:" + PORT + "/tests/");
});
