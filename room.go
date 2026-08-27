package roomer

import (
	"sync"
)

// room manages a group of connections with concurrent-safe operations.
type room struct {
	Name    string
	members map[string]*Conn
	mu      sync.RWMutex // Protects member list
}

// addMember adds a connection to the room under write lock.
func (r *room) addMember(c *Conn) {
	r.mu.Lock()
	r.members[c.ID] = c
	r.mu.Unlock()
}

// removeMember removes a connection from the room and reports if the room became empty.
func (r *room) removeMember(c *Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.members, c.ID)
	return len(r.members) == 0
}

// join adds a connection to the room and notifies others of new member.
func (r *room) join(c *Conn) {
	r.addMember(c)
	r.emit(c, NewMessage(r.Name, "new_member", "", "", []byte(c.ID)))
}

// leave removes a connection and notifies others; removes room from hub if empty.
func (r *room) leave(c *Conn) {
	if r.removeMember(c) {
		hub.removeRoom(r)
	}
	r.emit(c, NewMessage(r.Name, "member_left", "", "", []byte(c.ID)))
}

// emit broadcasts a message to all room members (except sender) on-demand without intermediate queues.
func (r *room) emit(c *Conn, msg *Message) {
	data := msg.Bytes()
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, member := range r.members {
		if c == nil || id != c.ID {
			member.TrySend(data)
		}
	}
}

// snapshot returns a copy of current member IDs (for join_ack responses).
func (r *room) snapshot() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.members))
	for id := range r.members {
		ids = append(ids, id)
	}
	return ids
}

// isEmpty reports whether the room currently has no members.
func (r *room) isEmpty() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members) == 0
}

// newRoom creates a new room instance.
func newRoom(name string) *room {
	return &room{
		Name:    name,
		members: make(map[string]*Conn),
	}
}
