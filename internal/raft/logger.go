package raft

// Event describes a notable state transition, emitted for observability and used
// by the failure-scenario demos to produce readable logs.
type Event struct {
	Kind string // e.g. "election_start", "become_leader", "step_down"
	Term uint64
	Role Role
	Peer string // optional: the other node involved
	Info string // optional: free-form detail
}

// Logger receives Events. Implementations must be safe for concurrent use.
type Logger interface {
	Event(nodeID string, e Event)
}

// nopLogger discards all events; it is the default when no logger is configured.
type nopLogger struct{}

func (nopLogger) Event(string, Event) {}
