package roomer

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

const shardCount = 32

type connShard struct {
	mu    sync.RWMutex
	conns map[string]*Conn
}

type roomShard struct {
	mu    sync.RWMutex
	rooms map[string]*room
}

// Hub manages all rooms and connections via lock-striped shards and distributed adapters.
type Hub struct {
	connShards [shardCount]connShard
	roomShards [shardCount]roomShard
	adapter    Adapter
	metrics    Metrics
	logger     *slog.Logger
	cfgMu      sync.RWMutex
}

// getShardIndex computes an FNV-1a hash index for key partitioning across shards.
func getShardIndex(key string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h % shardCount
}

// NewHub constructs a new independent Hub coordinator.
func NewHub() *Hub {
	h := &Hub{
		adapter: newLocalAdapter(),
		metrics: NopMetrics{},
		logger:  slog.Default(),
	}
	for i := 0; i < shardCount; i++ {
		h.connShards[i].conns = make(map[string]*Conn)
		h.roomShards[i].rooms = make(map[string]*room)
	}
	return h
}

// Global default hub instance.
var defaultHub = NewHub()

// DefaultHub returns the singleton package-level Hub instance.
func DefaultHub() *Hub {
	return defaultHub
}

// Configure initializes telemetry, logger, and distributed adapter subscribers.
func (h *Hub) Configure(adapter Adapter, metrics Metrics, logger *slog.Logger) {
	h.cfgMu.Lock()
	defer h.cfgMu.Unlock()
	if adapter != nil {
		h.adapter = adapter
		_ = h.adapter.Subscribe(func(channelSuffix string, msg *Message) {
			// Targeted node unicast direct messaging: "node:node_UUID"
			if strings.HasPrefix(channelSuffix, "node:") {
				if msg.Dst != "" {
					if dst, ok := h.getConn(msg.Dst); ok {
						dst.TrySend(msg.Bytes())
					}
				}
				return
			}

			// Cross-node direct messaging via root broadcast
			if msg.Dst != "" {
				if dst, ok := h.getConn(msg.Dst); ok {
					dst.TrySend(msg.Bytes())
				}
				return
			}

			// Local room fanout
			if r, ok := h.getRoom(channelSuffix); ok {
				r.emitLocal(msg)
			}
		})
	}
	if metrics != nil {
		h.metrics = metrics
	}
	if logger != nil {
		h.logger = logger
	}
}

// getConn returns a connection by ID, if it exists.
func (h *Hub) getConn(id string) (*Conn, bool) {
	shard := &h.connShards[getShardIndex(id)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	c, ok := shard.conns[id]
	return c, ok
}

// addConn adds a new connection to the hub, registers with cluster node registry, and tracks metrics.
func (h *Hub) addConn(c *Conn) {
	shard := &h.connShards[getShardIndex(c.ID)]
	shard.mu.Lock()
	shard.conns[c.ID] = c
	shard.mu.Unlock()

	h.cfgMu.RLock()
	adapter := h.adapter
	h.cfgMu.RUnlock()
	if adapter != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = adapter.RegisterNode(ctx, c.ID)
		}()
	}

	if h.metrics != nil {
		h.metrics.OnConnect()
	}
}

// removeConn removes a connection from the hub, unregisters from cluster node registry, and tracks metrics.
func (h *Hub) removeConn(id string) {
	shard := &h.connShards[getShardIndex(id)]
	shard.mu.Lock()
	if _, ok := shard.conns[id]; ok {
		delete(shard.conns, id)
		shard.mu.Unlock()

		h.cfgMu.RLock()
		adapter := h.adapter
		h.cfgMu.RUnlock()
		if adapter != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = adapter.UnregisterNode(ctx, id)
			}()
		}

		if h.metrics != nil {
			h.metrics.OnDisconnect()
		}
		return
	}
	shard.mu.Unlock()
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
			if h.metrics != nil {
				h.metrics.OnRoomDeleted(r.Name)
			}
		}
	}
	r.mu.Unlock()
	shard.mu.Unlock()
}

// joinRoom adds a connection to a room atomically, tracking cluster presence.
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
		r = newRoom(name, h)
		shard.rooms[name] = r
		if h.metrics != nil {
			h.metrics.OnRoomCreated(name)
		}
	}

	if c.trackRoom(name) {
		r.addMember(c)
		shard.mu.Unlock()

		h.cfgMu.RLock()
		adapter := h.adapter
		h.cfgMu.RUnlock()
		if adapter != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = adapter.AddPresence(ctx, name, c.ID)
			cancel()
		}

		r.emit(c, NewMessage(r.Name, "new_member", "", "", []byte(c.ID)))
		return
	}
	shard.mu.Unlock()
}

// leaveRoom removes a connection from a specific room, cleaning up empty rooms and presence.
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

	h.cfgMu.RLock()
	adapter := h.adapter
	h.cfgMu.RUnlock()
	if adapter != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = adapter.RemovePresence(ctx, name, c.ID)
		}()
	}

	r.emit(c, NewMessage(r.Name, "member_left", "", "", []byte(c.ID)))
}

// leaveAllRooms removes a connection from every room it's in.
func (h *Hub) leaveAllRooms(c *Conn) {
	for _, name := range c.joinedRooms() {
		h.leaveRoom(name, c)
	}
}

// getClusterPresence retrieves all member IDs in a room across the entire cluster.
func (h *Hub) getClusterPresence(roomName string) []string {
	memberMap := make(map[string]struct{})

	if r, ok := h.getRoom(roomName); ok {
		for _, id := range r.snapshot() {
			memberMap[id] = struct{}{}
		}
	}

	h.cfgMu.RLock()
	adapter := h.adapter
	h.cfgMu.RUnlock()

	if adapter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if members, err := adapter.GetPresence(ctx, roomName); err == nil {
			for _, id := range members {
				memberMap[id] = struct{}{}
			}
		}
	}

	res := make([]string, 0, len(memberMap))
	for id := range memberMap {
		res = append(res, id)
	}
	return res
}

// sendDirectToCluster routes a direct message using targeted node unicast when target node is known.
func (h *Hub) sendDirectToCluster(dstID string, msg *Message) {
	h.cfgMu.RLock()
	adapter := h.adapter
	metrics := h.metrics
	h.cfgMu.RUnlock()

	if adapter != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if targetNode, err := adapter.GetNodeForConn(ctx, dstID); err == nil && targetNode != "" {
				if err := adapter.PublishDirect(ctx, targetNode, msg); err == nil {
					if metrics != nil {
						metrics.OnClusterPublish(len(msg.Payload))
					}
					return
				}
			}
			// Fallback to cluster-wide root broadcast if node mapping not resolved
			if err := adapter.Publish(ctx, "root", msg); err == nil {
				if metrics != nil {
					metrics.OnClusterPublish(len(msg.Payload))
				}
			}
		}()
	}
}

// publishToCluster publishes an outbound room message to the distributed cluster adapter.
func (h *Hub) publishToCluster(roomName string, msg *Message) {
	h.cfgMu.RLock()
	adapter := h.adapter
	metrics := h.metrics
	h.cfgMu.RUnlock()

	if adapter != nil {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := adapter.Publish(ctx, roomName, msg); err == nil {
				if metrics != nil {
					metrics.OnClusterPublish(len(msg.Payload))
				}
			}
		}()
	}
}

// Shutdown gracefully drains active connections and terminates adapters.
func (h *Hub) Shutdown(ctx context.Context) error {
	var conns []*Conn
	for i := 0; i < shardCount; i++ {
		shard := &h.connShards[i]
		shard.mu.RLock()
		for _, c := range shard.conns {
			conns = append(conns, c)
		}
		shard.mu.RUnlock()
	}

	for _, c := range conns {
		c.cleanup()
	}

	h.cfgMu.RLock()
	adapter := h.adapter
	h.cfgMu.RUnlock()
	if adapter != nil {
		return adapter.Close()
	}
	return nil
}
