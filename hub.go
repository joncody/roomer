package roomer

import "sync"

const shardCount = 32

type connShard struct {
	mu    sync.RWMutex
	conns map[string]*Conn
}

type roomShard struct {
	mu    sync.RWMutex
	rooms map[string]*room
}

// Hub manages all rooms and connections via lock-striped shards.
type Hub struct {
	connShards [shardCount]connShard
	roomShards [shardCount]roomShard
}

// getShardIndex computes an FNV-1a hash index for key partitioning.
func getShardIndex(key string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h % shardCount
}

func newHub() *Hub {
	h := &Hub{}
	for i := 0; i < shardCount; i++ {
		h.connShards[i].conns = make(map[string]*Conn)
		h.roomShards[i].rooms = make(map[string]*room)
	}
	return h
}

// Global hub instance
var hub = newHub()

// getConn returns a connection by ID, if it exists.
func (h *Hub) getConn(id string) (*Conn, bool) {
	shard := &h.connShards[getShardIndex(id)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	c, ok := shard.conns[id]
	return c, ok
}

// addConn adds a new connection to the hub.
func (h *Hub) addConn(c *Conn) {
	shard := &h.connShards[getShardIndex(c.ID)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.conns[c.ID] = c
}

// removeConn removes a connection from the hub.
func (h *Hub) removeConn(id string) {
	shard := &h.connShards[getShardIndex(id)]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.conns, id)
}

// getRoom returns a room by name, if it exists.
func (h *Hub) getRoom(name string) (*room, bool) {
	shard := &h.roomShards[getShardIndex(name)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	r, ok := shard.rooms[name]
	return r, ok
}

// removeRoom deletes a specific room instance from the hub if it remains empty.
func (h *Hub) removeRoom(r *room) {
	shard := &h.roomShards[getShardIndex(r.Name)]
	shard.mu.Lock()
	r.mu.Lock()
	if current, ok := shard.rooms[r.Name]; ok && current == r {
		if len(r.members) == 0 {
			delete(shard.rooms, r.Name)
		}
	}
	r.mu.Unlock()
	shard.mu.Unlock()
}

// joinRoom adds a connection to a room atomically, creating the room if needed.
func (h *Hub) joinRoom(name string, c *Conn) {
	select {
	case <-c.done:
		return
	default:
	}
	shard := &h.roomShards[getShardIndex(name)]
	shard.mu.Lock()
	r, ok := shard.rooms[name]
	if !ok {
		r = newRoom(name)
		shard.rooms[name] = r
	}
	if c.trackRoom(name) {
		r.addMember(c)
		shard.mu.Unlock()
		r.emit(c, NewMessage(r.Name, "new_member", "", "", []byte(c.ID)))
		return
	}
	shard.mu.Unlock()
}

// leaveRoom removes a connection from a specific room and cleans up empty rooms.
func (h *Hub) leaveRoom(name string, c *Conn) {
	c.untrackRoom(name)
	shard := &h.roomShards[getShardIndex(name)]
	shard.mu.RLock()
	r, ok := shard.rooms[name]
	shard.mu.RUnlock()
	if !ok {
		return
	}

	if r.removeMember(c) {
		h.removeRoom(r)
	}

	r.emit(c, NewMessage(r.Name, "member_left", "", "", []byte(c.ID)))
}

// leaveAllRooms removes a connection from every room it's in.
func (h *Hub) leaveAllRooms(c *Conn) {
	for _, name := range c.joinedRooms() {
		h.leaveRoom(name, c)
	}
}
