// Package roomer provides a high-performance, room-based WebSocket framework
// for real-time bidirectional communication between clients.
//
// Core Concepts
//
//   - Connection (Conn): Represents a single authenticated WebSocket client.
//     Each has a unique UUID and optional authenticated claims (e.g., user ID, roles).
//
//   - Room: A named group of connections. Messages sent to a room are
//     broadcast to all members (excluding the sender). The "root" room
//     is auto-joined by every new connection. Empty rooms are garbage-collected
//     automatically.
//
//   - Hub: Manages all active connections and rooms across 32 lock-striped shards
//     using FNV-1a hashing to eliminate CPU core mutex contention.
//
//   - Cluster Adapter (Adapter): Pluggable interface for multi-node deployments
//     (e.g., Redis, NATS, Kafka). Outbound room broadcasts publish to the cluster,
//     and incoming cluster messages fan out locally without loopbacks.
//
//   - Observability (Metrics): Telemetry interface for instrumenting connections,
//     disconnections, room lifecycle, byte throughput, and dropped frame counts.
//
//   - Message Format: Binary, length-prefixed frames encoding room,
//     event, destination, source, and payload. This enables zero-copy
//     slice parsing and single-allocation serialization.
//
//   - Event Dispatch: Built-in events ("join", "leave") and custom
//     events handled via registered MessageHandlers.
//
// Usage
//
//  1. Register custom event handlers (optional):
//     roomer.RegisterHandler("chat", func(c *roomer.Conn, msg *roomer.Message) error {
//     c.SendToRoom(msg.Room, msg.Event, msg.Payload)
//     return nil
//     })
//
//  2. Mount the WebSocket handler with production options:
//     http.Handle("/ws", roomer.SocketHandlerWithOptions(
//     roomer.WithLogger(slog.Default()),
//     roomer.WithMetrics(myPrometheusCollector),
//     roomer.WithAdapter(myRedisAdapter),
//     roomer.WithAuthorize(func(r *http.Request) (map[string]string, error) {
//     // Extract JWT claims, session, etc.
//     return claims, nil
//     }),
//     roomer.WithMaxMessageSize(8 * 1024 * 1024), // 8 MB max
//     roomer.WithCheckOrigin(func(r *http.Request) bool { return true }),
//     ))
//
//  3. Graceful Server Shutdown:
//     // On SIGINT / SIGTERM:
//     ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//     defer cancel()
//     if err := roomer.Shutdown(ctx); err != nil {
//     log.Printf("Shutdown error: %v", err)
//     }
//
//  4. Message Structure and Construction
//
//     All messages (client→server and server→client) follow a binary,
//     length-prefixed format with these fields:
//     - Room (string): Target room name (use "root" for direct messages).
//     - Event (string): Event type (e.g., "join", "chat", "update").
//     - Dst (string): Optional destination client ID (for direct messages).
//     - Src (string): Source client ID (auto-set by server on send).
//     - Payload ([]byte): Arbitrary binary data (commonly JSON-encoded).
//
//     To construct a message on the server, use NewMessage:
//     msg := roomer.NewMessage(
//     room,    // e.g., "lobby"
//     event,   // e.g., "chat"
//     dst,     // e.g., "" for broadcast, or "abc123" for direct
//     src,     // typically c.ID (sender's ID)
//     payload, // e.g., []byte(`{"text":"hello"}`)
//     )
//     rawBytes := msg.Bytes() // serialize for sending
//
//     Built-in client→server events:
//     - "join":  { "event": "join", "room": "lobby" }
//     - "leave": { "event": "leave", "room": "lobby" }
//     To send a direct message from client, set "dst" to the recipient's ID.
//
//  5. Server-Side Messaging APIs
//
//     From within a MessageHandler or server logic, use:
//
//     - c.TrySend(msg []byte) bool
//     Sends raw binary message; returns false if dropped (slow or closed client).
//     Drops trigger asynchronous connection cleanup to prevent deadlocks.
//
//     - c.SendToRoom(room, event string, payload []byte)
//     Broadcasts to all members of a room (excluding sender) and publishes to cluster adapter.
//
//     - c.SendToClient(dstID, event string, payload []byte)
//     Sends a direct message to another client by ID.
//
// Concurrency & Safety
//
//   - All exported APIs are safe for concurrent use.
//   - Hub uses 32-shard lock striping for high-throughput parallel execution across CPU cores.
//   - Non-blocking sends: TrySend and internal messaging never block during broadcasts.
//   - Deadlock-free teardown: Slow client cleanup runs asynchronously.
//   - Connections auto-cleanup on disconnect, error, or write timeout.
//   - Empty rooms are garbage-collected automatically.
package roomer
