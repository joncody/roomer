package roomer

import (
	"sync"
)

// roomMessage wraps a message with its sender for room broadcasting.
type roomMessage struct {
	sender *Conn
	data   []byte
}

// room manages a group of connections with concurrent-safe operations.
type room struct {
	Name       string
	members    map[string]*Conn
	register   chan *Conn        // Channel to join the room
	unregister chan *Conn        // Channel to leave the room
	send       chan *roomMessage // Channel to broadcast messages
	stop       chan struct{}     // Signal to terminate room if empty
	stopOnce   sync.Once         // Ensures stop is only closed once
	mu         sync.Mutex        // Protects member list
}

// join queues a connection to join the room.
func (r *room) join(c *Conn) {
	r.register <- c
}

// leave queues a connection to leave the room.
func (r *room) leave(c *Conn) {
	r.unregister <- c
}

// emit queues a message to be broadcast to all room members (except sender).
func (r *room) emit(c *Conn, msg *Message) {
	r.send <- &roomMessage{c, msg.Bytes()}
}

// snapshot returns a copy of current member IDs (for join_ack responses).
func (r *room) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.members))
	for id := range r.members {
		ids = append(ids, id)
	}
	return ids
}

// broadcast sends a message to all room members except the sender.
func (r *room) broadcast(msg *roomMessage) {
	r.mu.Lock()
	members := make([]*Conn, 0, len(r.members))
	for id, c := range r.members {
		if id != msg.sender.ID {
			members = append(members, c)
		}
	}
	r.mu.Unlock()
	for _, c := range members {
		c.TrySend(msg.data)
	}
}

// handleJoin adds a connection to the room and notifies others of new member.
func (r *room) handleJoin(c *Conn) {
	r.mu.Lock()
	r.members[c.ID] = c
	r.mu.Unlock()
	r.emit(c, NewMessage(r.Name, "new_member", "", "", []byte(c.ID)))
}

// handleLeave removes a connection and notifies others; stops room if empty.
func (r *room) handleLeave(c *Conn) {
	r.mu.Lock()
	delete(r.members, c.ID)
	empty := len(r.members) == 0
	r.mu.Unlock()
	r.emit(c, NewMessage(r.Name, "member_left", "", "", []byte(c.ID)))
	if empty {
		hub.mu.Lock()
		r.mu.Lock()
		if len(r.members) == 0 && len(r.register) == 0 {
			delete(hub.rooms, r.Name)
			r.stopOnce.Do(func() {
				close(r.stop)
			})
		}
		r.mu.Unlock()
		hub.mu.Unlock()
	}
}

// run is the room's main event loop processing register/unregister/send/stop.
func (r *room) run() {
	for {
		select {
		case c := <-r.register:
			r.handleJoin(c)
		case c := <-r.unregister:
			r.handleLeave(c)
		case msg := <-r.send:
			r.broadcast(msg)
		case <-r.stop:
			for {
				select {
				case msg := <-r.send:
					r.broadcast(msg)
				default:
					return
				}
			}
		}
	}
}

// newRoom creates and starts a new room.
func newRoom(name string) *room {
	r := &room{
		Name:       name,
		members:    make(map[string]*Conn),
		register:   make(chan *Conn, 16),
		unregister: make(chan *Conn, 16),
		send:       make(chan *roomMessage, 64),
		stop:       make(chan struct{}),
	}
	go r.run()
	return r
}
