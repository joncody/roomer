package roomer

import (
	"sync"
)

// room manages a group of connections with concurrent-safe operations.
type room struct {
	Name    string
	hub     *Hub
	members map[string]*Conn
	mu      sync.RWMutex
}

// newRoom creates a new room instance linked to its owning hub.
func newRoom(name string, h *Hub) *room {
	return &room{
		Name:    name,
		hub:     h,
		members: make(map[string]*Conn),
	}
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

// emit broadcasts a message to local room members and publishes to the distributed cluster adapter.
func (r *room) emit(sender *Conn, msg *Message) {
	data := msg.Bytes()
	r.mu.RLock()
	for id, member := range r.members {
		if sender == nil || id != sender.ID {
			member.TrySend(data)
		}
	}
	r.mu.RUnlock()

	// Publish to cluster adapter asynchronously without blocking broadcast loop
	if r.hub != nil {
		r.hub.publishToCluster(r.Name, msg)
	}
}

// emitLocal broadcasts a message received from cluster subscribers to local members only.
func (r *room) emitLocal(msg *Message) {
	data := msg.Bytes()
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, member := range r.members {
		if id != msg.Src {
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

// len returns the member count.
func (r *room) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}
