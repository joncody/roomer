package roomer

import (
	"context"
	"sync"
)

// Adapter handles cross-server message distribution, cluster presence, and unicast routing for multi-node deployments.
type Adapter interface {
	// Publish broadcasts a message to external cluster instances for a given room.
	Publish(ctx context.Context, room string, msg *Message) error

	// PublishDirect sends a message directly to a specific cluster node hosting a target client.
	PublishDirect(ctx context.Context, targetNodeID string, msg *Message) error

	// Subscribe listens for messages published from other nodes and dispatches them locally.
	Subscribe(handler func(channel string, msg *Message)) error

	// AddPresence adds a connection ID to a room's cluster-wide presence set.
	AddPresence(ctx context.Context, room, connID string) error

	// RemovePresence removes a connection ID from a room's cluster-wide presence set.
	RemovePresence(ctx context.Context, room, connID string) error

	// GetPresence retrieves all connection IDs in a room across the entire cluster.
	GetPresence(ctx context.Context, room string) ([]string, error)

	// RegisterNode maps a connection ID to this node ID in the cluster registry.
	RegisterNode(ctx context.Context, connID string) error

	// UnregisterNode removes a connection ID mapping from the cluster registry.
	UnregisterNode(ctx context.Context, connID string) error

	// GetNodeForConn retrieves the node ID hosting a given connection ID.
	GetNodeForConn(ctx context.Context, connID string) (string, error)

	// NodeID returns the unique cluster identifier of this server instance.
	NodeID() string

	// Close terminates adapter connections and releases resources.
	Close() error
}

// localAdapter is the default in-memory adapter for single-node deployments.
type localAdapter struct {
	mu       sync.RWMutex
	presence map[string]map[string]struct{}
	nodeMap  map[string]string
	nodeID   string
}

// newLocalAdapter creates a new in-memory local adapter with presence and node tracking.
func newLocalAdapter() Adapter {
	return &localAdapter{
		presence: make(map[string]map[string]struct{}),
		nodeMap:  make(map[string]string),
		nodeID:   "local-node",
	}
}

func (a *localAdapter) Publish(ctx context.Context, room string, msg *Message) error {
	return nil
}

func (a *localAdapter) PublishDirect(ctx context.Context, targetNodeID string, msg *Message) error {
	return nil
}

func (a *localAdapter) Subscribe(handler func(channel string, msg *Message)) error {
	return nil
}

func (a *localAdapter) AddPresence(ctx context.Context, room, connID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.presence[room] == nil {
		a.presence[room] = make(map[string]struct{})
	}
	a.presence[room][connID] = struct{}{}
	return nil
}

func (a *localAdapter) RemovePresence(ctx context.Context, room, connID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.presence[room] != nil {
		delete(a.presence[room], connID)
		if len(a.presence[room]) == 0 {
			delete(a.presence, room)
		}
	}
	return nil
}

func (a *localAdapter) GetPresence(ctx context.Context, room string) ([]string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	set := a.presence[room]
	if set == nil {
		return []string{}, nil
	}
	members := make([]string, 0, len(set))
	for id := range set {
		members = append(members, id)
	}
	return members, nil
}

func (a *localAdapter) RegisterNode(ctx context.Context, connID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodeMap[connID] = a.nodeID
	return nil
}

func (a *localAdapter) UnregisterNode(ctx context.Context, connID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.nodeMap, connID)
	return nil
}

func (a *localAdapter) GetNodeForConn(ctx context.Context, connID string) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.nodeMap[connID], nil
}

func (a *localAdapter) NodeID() string {
	return a.nodeID
}

func (a *localAdapter) Close() error {
	return nil
}
