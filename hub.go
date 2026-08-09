package roomer

import "sync"

// Hub manages all rooms and connections (singleton via global `hub`).
type Hub struct {
	mu    sync.RWMutex
	rooms map[string]*room
	conns map[string]*Conn
}

// Global hub instance
var hub = &Hub{
	rooms: make(map[string]*room),
	conns: make(map[string]*Conn),
}

// getConn returns a connection by ID, if it exists.
func (h *Hub) getConn(id string) (*Conn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	c, ok := h.conns[id]
	return c, ok
}

// addConn adds a new connection to the hub.
func (h *Hub) addConn(c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.conns[c.ID] = c
}

// removeConn removes a connection from the hub.
func (h *Hub) removeConn(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.conns, id)
}

// getRoom returns a room by name, if it exists.
func (h *Hub) getRoom(name string) (*room, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	r, ok := h.rooms[name]
	return r, ok
}

// removeRoom deletes a room from the hub (called when room becomes empty).
func (h *Hub) removeRoom(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.rooms, name)
}

// joinRoom adds a connection to a room, creating the room if needed.
func (h *Hub) joinRoom(name string, c *Conn) {
	select {
	case <-c.done:
		return
	default:
	}
	h.mu.Lock()
	room, ok := h.rooms[name]
	if !ok {
		room = newRoom(name)
		h.rooms[name] = room
	}
	h.mu.Unlock()
	if c.trackRoom(name) {
		room.join(c)
	}
}

// leaveRoom removes a connection from a specific room.
func (h *Hub) leaveRoom(name string, c *Conn) {
	c.untrackRoom(name)
	if room, ok := h.getRoom(name); ok {
		room.leave(c)
	}
}

// leaveAllRooms removes a connection from every room it's in.
func (h *Hub) leaveAllRooms(c *Conn) {
	for _, name := range c.joinedRooms() {
		h.leaveRoom(name, c)
	}
}
