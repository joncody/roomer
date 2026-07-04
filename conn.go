package roomer

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Conn represents a single WebSocket connection with metadata and messaging channels.
type Conn struct {
	ID          string
	Claims      map[string]string // Authenticated claims (e.g., user ID, roles)
	send        chan []byte       // Outbound message queue
	socket      *websocket.Conn
	cleanupOnce sync.Once // Ensures cleanup happens only once
	done        chan struct{}
}

const (
	writeWait      = 10 * time.Second  // Time allowed to write a message to the peer
	pongWait       = 60 * time.Second  // Time allowed to read next pong before idle timeout
	pingPeriod     = pongWait * 9 / 10 // Send pings to peer with this period
	maxMessageSize = 65536             // Maximum message size allowed from peer
)

var (
	// upgrader configures the WebSocket handshake with permissive origin policy.
	upgrader = websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}
)

// TrySend attempts to send a message; drops it if the send buffer is full or closed.
func (c *Conn) TrySend(msg []byte) bool {
	select {
	case <-c.done:
		return false
	default:
		select {
		case c.send <- msg:
			return true
		default:
			log.Printf("Conn %s: dropped message (slow or closed)", c.ID)
			c.cleanup()
			return false
		}
	}
}

// SendToRoom broadcasts a message to all members of the specified room.
func (c *Conn) SendToRoom(roomName, event string, payload []byte) {
	msg := NewMessage(roomName, event, "", c.ID, payload)
	if room, ok := hub.getRoom(roomName); ok {
		room.emit(c, msg)
	}
}

// SendToClient sends a direct message to another client by ID.
func (c *Conn) SendToClient(dstID, event string, payload []byte) {
	msg := NewMessage("root", event, dstID, c.ID, payload)
	if dst, ok := hub.getConn(dstID); ok {
		dst.TrySend(msg.Bytes())
	}
}

// dispatch routes an incoming message to appropriate handlers or rooms.
func (c *Conn) dispatch(msg *Message) {
	switch msg.Event {
	case "join":
		hub.joinRoom(msg.Room, c)
		members := []byte("[]") // or fetch real snapshot if needed
		if room, ok := hub.getRoom(msg.Room); ok {
			if snap, err := json.Marshal(room.snapshot()); err == nil {
				members = snap
			}
		}
		ack := NewMessage(msg.Room, "join_ack", "", c.ID, members).Bytes()
		c.TrySend(ack)
	case "leave":
		hub.leaveRoom(msg.Room, c)
		ack := NewMessage(msg.Room, "leave_ack", "", c.ID, []byte(c.ID)).Bytes()
		c.TrySend(ack)
	default:
		if msg.Dst != "" {
			if dst, ok := hub.getConn(msg.Dst); ok {
				dst.TrySend(msg.Bytes())
			}
			return
		}
		if handler := getHandler(msg.Event); handler != nil {
			if err := handler(c, msg); err != nil {
				log.Printf("Handler error for event %q from conn %s: %v", msg.Event, c.ID, err)
			}
			return
		}
		if room, ok := hub.getRoom(msg.Room); ok {
			room.emit(c, msg)
		}
	}
}

// cleanup safely removes the connection from all rooms and closes resources.
func (c *Conn) cleanup() {
	c.cleanupOnce.Do(func() {
		hub.leaveAllRooms(c)
		hub.removeConn(c.ID)
		c.socket.Close()
		close(c.done)
	})
}

// readPump reads messages from the WebSocket and dispatches them.
func (c *Conn) readPump() {
	defer c.cleanup()
	c.socket.SetReadLimit(maxMessageSize)
	c.socket.SetReadDeadline(time.Now().Add(pongWait))
	c.socket.SetPongHandler(func(string) error {
		c.socket.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, data, err := c.socket.ReadMessage()
		if err != nil {
			return
		}
		msg := BytesToMessage(data)
		if msg == nil {
			log.Printf("Conn %s: malformed message", c.ID)
			return
		}
		c.dispatch(msg)
	}
}

// write writes a message with a specified WebSocket message type and deadline.
func (c *Conn) write(mt int, payload []byte) error {
	c.socket.SetWriteDeadline(time.Now().Add(writeWait))
	return c.socket.WriteMessage(mt, payload)
}

// writePump sends messages from the send channel and periodic pings.
func (c *Conn) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.cleanup()
	}()
	for {
		select {
		case msg := <-c.send:
			if err := c.write(websocket.BinaryMessage, msg); err != nil {
				return
			}
		case <-c.done:
			c.write(websocket.CloseMessage, []byte{})
			return
		case <-ticker.C:
			if err := c.write(websocket.PingMessage, []byte{}); err != nil {
				return
			}
		}
	}
}

// newConnection upgrades an HTTP request to a WebSocket and initializes a Conn.
func newConnection(w http.ResponseWriter, r *http.Request, claims map[string]string) *Conn {
	sock, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil
	}
	id, err := uuid.NewRandom()
	if err != nil {
		sock.Close()
		return nil
	}
	return &Conn{
		ID:     id.String(),
		Claims: claims,
		socket: sock,
		send:   make(chan []byte, 256),
		done:   make(chan struct{}),
	}
}
