package roomer

import "context"

// Adapter handles cross-server message distribution for multi-node deployments.
type Adapter interface {
	// Publish broadcasts a message to external cluster instances for a given room.
	Publish(ctx context.Context, room string, msg *Message) error

	// Subscribe listens for messages published from other nodes and dispatches them locally.
	Subscribe(handler func(room string, msg *Message)) error

	// Close terminates adapter connections and releases resources.
	Close() error
}

// localAdapter is the default in-memory adapter for single-node deployments.
type localAdapter struct{}

// newLocalAdapter creates a new no-op local adapter.
func newLocalAdapter() Adapter {
	return &localAdapter{}
}

func (a *localAdapter) Publish(ctx context.Context, room string, msg *Message) error {
	return nil
}

func (a *localAdapter) Subscribe(handler func(room string, msg *Message)) error {
	return nil
}

func (a *localAdapter) Close() error {
	return nil
}
