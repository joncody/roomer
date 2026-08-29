package roomer

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Conn represents a single WebSocket connection with metadata and messaging channels.
type Conn struct {
	ID          string
	Claims      map[string]string // Authenticated claims (e.g., user ID, roles)
	hub         *Hub
	send        chan []byte // Outbound message queue
	socket      *websocket.Conn
	cleanupOnce sync.Once
	done        chan struct{}
	cleaningUp  int32
	roomsMu     sync.RWMutex
	rooms       map[string]struct{} // Set of joined room names
	config      Config
}

func (c *Conn) trackRoom(room string) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	select {
	case <-c.done:
		return false
	default:
		c.rooms[room] = struct{}{}
		return true
	}
}

func (c *Conn) untrackRoom(room string) {
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	delete(c.rooms, room)
}

func (c *Conn) joinedRooms() []string {
	c.roomsMu.RLock()
	defer c.roomsMu.RUnlock()
	rooms := make([]string, 0, len(c.rooms))
	for r := range c.rooms {
		rooms = append(rooms, r)
	}
	return rooms
}

// IsInRoom checks if this connection is currently tracked in a room.
func (c *Conn) IsInRoom(room string) bool {
	c.roomsMu.RLock()
	defer c.roomsMu.RUnlock()
	_, ok := c.rooms[room]
	return ok
}

// TrySend attempts to send a binary message; drops it and triggers asynchronous cleanup if send buffer is full.
func (c *Conn) TrySend(msg []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}

	select {
	case <-c.done:
		return false
	case c.send <- msg:
		if c.config.Metrics != nil {
			c.config.Metrics.OnMessageSent(len(msg))
		}
		return true
	default:
		if c.config.Metrics != nil {
			c.config.Metrics.OnMessageDropped()
		}
		if c.config.Logger != nil {
			c.config.Logger.Warn("Conn dropped message (buffer full or slow client)", "conn_id", c.ID)
		}

		// Trigger teardown once asynchronously without spawning duplicate goroutines
		if atomic.CompareAndSwapInt32(&c.cleaningUp, 0, 1) {
			go c.cleanup()
		}
		return false
	}
}

// SendToRoom broadcasts a message to all members of the specified room except self.
func (c *Conn) SendToRoom(roomName, event string, payload []byte) {
	msg := NewMessage(roomName, event, "", c.ID, payload)
	if room, ok := c.hub.getRoom(roomName); ok {
		room.emit(c, msg)
	}
}

// SendToClient sends a direct message to another client by connection ID.
func (c *Conn) SendToClient(dstID, event string, payload []byte) {
	msg := NewMessage("root", event, dstID, c.ID, payload)
	if dst, ok := c.hub.getConn(dstID); ok {
		dst.TrySend(msg.Bytes())
	}
}

// dispatch routes an incoming message to handlers or rooms.
func (c *Conn) dispatch(msg *Message) {
	select {
	case <-c.done:
		return
	default:
	}

	msg.Src = c.ID
	msg.SrcLength = len(c.ID)

	switch msg.Event {
	case "join":
		c.hub.joinRoom(msg.Room, c)
		members := []byte("[]")
		if room, ok := c.hub.getRoom(msg.Room); ok {
			if snap, err := json.Marshal(room.snapshot()); err == nil {
				members = snap
			}
		}
		ack := NewMessage(msg.Room, "join_ack", "", c.ID, members).Bytes()
		c.TrySend(ack)

	case "leave":
		c.hub.leaveRoom(msg.Room, c)
		ack := NewMessage(msg.Room, "leave_ack", "", c.ID, []byte(c.ID)).Bytes()
		c.TrySend(ack)

	default:
		// Direct targeted messaging
		if msg.Dst != "" {
			if dst, ok := c.hub.getConn(msg.Dst); ok {
				dst.TrySend(msg.Bytes())
			}
			return
		}

		// Custom handler dispatch
		if handler := getHandler(msg.Event); handler != nil {
			go func() {
				if err := handler(c, msg); err != nil {
					if c.config.Logger != nil {
						c.config.Logger.Error("Message handler execution error", "event", msg.Event, "conn_id", c.ID, "err", err)
					}
				}
			}()
			return
		}

		// Default room broadcast
		if room, ok := c.hub.getRoom(msg.Room); ok {
			room.emit(c, msg)
		}
	}
}

// cleanup safely removes the connection from all rooms and releases resources.
func (c *Conn) cleanup() {
	c.cleanupOnce.Do(func() {
		close(c.done)
		c.hub.leaveAllRooms(c)
		c.hub.removeConn(c.ID)
		if c.socket != nil {
			_ = c.socket.Close()
		}
	})
}

// readPump reads messages from the WebSocket and dispatches them.
func (c *Conn) readPump() {
	if c.socket == nil {
		return
	}
	defer c.cleanup()

	c.socket.SetReadLimit(c.config.MaxMessageSize)
	_ = c.socket.SetReadDeadline(time.Now().Add(c.config.PongWait))
	c.socket.SetPongHandler(func(string) error {
		_ = c.socket.SetReadDeadline(time.Now().Add(c.config.PongWait))
		return nil
	})

	for {
		_, data, err := c.socket.ReadMessage()
		if err != nil {
			return
		}
		if c.config.Metrics != nil {
			c.config.Metrics.OnMessageReceived(len(data))
		}
		msg := BytesToMessage(data)
		if msg == nil {
			if c.config.Logger != nil {
				c.config.Logger.Warn("Malformed binary packet received", "conn_id", c.ID)
			}
			return
		}
		c.dispatch(msg)
	}
}

// write writes a message with a specified WebSocket message type and deadline.
func (c *Conn) write(mt int, payload []byte) error {
	if c.socket == nil {
		return nil
	}
	_ = c.socket.SetWriteDeadline(time.Now().Add(c.config.WriteWait))
	return c.socket.WriteMessage(mt, payload)
}

// writePump sends messages from the send channel and periodic pings.
func (c *Conn) writePump() {
	ticker := time.NewTicker(c.config.PingPeriod)
	defer func() {
		ticker.Stop()
		c.cleanup()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				_ = c.write(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.write(websocket.BinaryMessage, msg); err != nil {
				return
			}
		case <-c.done:
			_ = c.write(websocket.CloseMessage, []byte{})
			return
		case <-ticker.C:
			if err := c.write(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// newConnection upgrades an HTTP request to a WebSocket and initializes a Conn.
func newConnection(w http.ResponseWriter, r *http.Request, claims map[string]string, cfg Config, h *Hub) *Conn {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  cfg.ReadBufferSize,
		WriteBufferSize: cfg.WriteBufferSize,
		CheckOrigin:     cfg.CheckOrigin,
	}
	sock, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil
	}
	id, err := uuid.NewRandom()
	if err != nil {
		_ = sock.Close()
		return nil
	}
	return &Conn{
		ID:     id.String(),
		Claims: claims,
		hub:    h,
		socket: sock,
		send:   make(chan []byte, cfg.ChannelCapacity),
		done:   make(chan struct{}),
		rooms:  make(map[string]struct{}),
		config: cfg,
	}
}
