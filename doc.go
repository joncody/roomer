// Package roomer provides a scalable, room-based WebSocket server
// for real-time bidirectional communication between clients.
//
// Core Concepts
//
//   - Connection (Conn): Represents a single authenticated WebSocket client.
//     Each has a unique ID and optional claims (e.g., user ID, roles).
//
//   - Room: A named group of connections. Messages sent to a room are
//     broadcast to all members (excluding the sender). The "root" room
//     is auto-joined by every new connection.
//
//   - Message Format: Binary, length-prefixed frames encoding room,
//     event, destination, source, and payload. This enables efficient
//     parsing and low overhead.
//
//   - Event Dispatch: Built-in events ("join", "leave") and custom
//     events handled via registered MessageHandlers.
//
// Usage
//
//   1. Register custom event handlers (optional):
//        roomer.RegisterHandler("chat", func(c *roomer.Conn, msg *roomer.Message) error {
//            // Handle "chat" event
//            return nil
//        })
//
//   2. Mount the WebSocket handler with optional authentication and configuration options:
//        http.Handle("/ws", roomer.SocketHandlerWithOptions(
//            roomer.WithAuthorize(func(r *http.Request) (map[string]string, error) {
//                // Extract JWT claims, session, etc.
//                return claims, nil
//            }),
//            roomer.WithMaxMessageSize(8 * 1024 * 1024), // 8 MB max
//            roomer.WithCheckOrigin(func(r *http.Request) bool { return true }),
//        ))
//
//   3. Message Structure and Construction
//
//      All messages (client→server and server→client) follow a binary,
//      length-prefixed format with these fields:
//        - Room (string): Target room name (use "root" for direct messages).
//        - Event (string): Event type (e.g., "join", "chat", "update").
//        - Dst (string): Optional destination client ID (for direct messages).
//        - Src (string): Source client ID (auto-set by server on send).
//        - Payload ([]byte): Arbitrary binary data (commonly JSON-encoded).
//
//      To construct a message on the server, use NewMessage:
//        msg := roomer.NewMessage(
//            room,    // e.g., "lobby"
//            event,   // e.g., "chat"
//            dst,     // e.g., "" for broadcast, or "abc123" for direct
//            src,     // typically c.ID (sender's ID)
//            payload, // e.g., []byte(`{"text":"hello"}`)
//        )
//        rawBytes := msg.Bytes() // serialize for sending
//
//      Built-in client→server events:
//        - "join":  { "event": "join", "room": "lobby" }
//        - "leave": { "event": "leave", "room": "lobby" }
//      To send a direct message from client, set "dst" to the recipient's ID.
//
//   4. Server-Side Messaging APIs
//
//      From within a MessageHandler or server logic, use:
//
//        - c.TrySend(msg []byte) bool
//            Sends raw binary message; returns false if dropped (slow or closed client).
//            Typically used with msg := NewMessage(...); c.TrySend(msg.Bytes()).
//
//        - c.SendToRoom(room, event string, payload []byte)
//            Broadcasts to all members of a room (excluding sender).
//            Equivalent to: NewMessage(room, event, "", c.ID, payload) + room emit.
//
//        - c.SendToClient(dstID, event string, payload []byte)
//            Sends a direct message to another client by ID.
//            Equivalent to: NewMessage("root", event, dstID, c.ID, payload) + direct send.
//
//      Example:
//        func chatHandler(c *roomer.Conn, msg *roomer.Message) error {
//            // Echo message back as direct reply
//            reply := roomer.NewMessage(
//                "root", "echo", msg.Src, c.ID, []byte("ack"),
//            )
//            if !c.TrySend(reply.Bytes()) {
//                log.Printf("Failed to send reply to %s", msg.Src)
//            }
//            return nil
//        }
//
// Safety
//
//   - All exported APIs are safe for concurrent use.
//   - Connections auto-cleanup on disconnect, error, or write timeout.
//   - Empty rooms are garbage-collected automatically.
package roomer
