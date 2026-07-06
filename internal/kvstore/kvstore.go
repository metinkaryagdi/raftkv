// Package kvstore is the replicated state machine that sits on top of Raft. It is
// a plain in-memory key-value map; the interesting property is not the data
// structure but that every replica applies the exact same committed command
// sequence and therefore ends up in the exact same state.
package kvstore

import (
	"encoding/json"
	"sync"
)

// Store is a concurrency-safe in-memory key-value map.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty Store.
func New() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply mutates the store according to a committed command. It is the single
// entry point the Raft apply loop calls, so it must be deterministic: the same
// command applied to the same prior state always yields the same result.
// Unknown/no-op ops (e.g. the leader's term marker) are ignored.
func (s *Store) Apply(op, key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op {
	case "set":
		s.data[key] = value
	case "delete":
		delete(s.data, key)
	}
}

// Get returns the value for key and whether it was present. Reads bypass the log;
// linearizable-read semantics (serving reads only from a leader that has
// confirmed leadership) are enforced by the caller, not the store.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Snapshot returns a copy of the entire map, for debugging, equivalence checks,
// and as the payload of a Raft log snapshot (see MarshalSnapshot).
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Restore replaces the store's entire state with data, which must be a
// complete map (not a merge) — this is how a node loads a snapshot installed
// by Raft (see MarshalSnapshot/UnmarshalSnapshot).
func (s *Store) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string, len(data))
	for k, v := range data {
		s.data[k] = v
	}
}

// MarshalSnapshot serializes the current state for handing to Raft as a
// snapshot payload. JSON keeps the payload human-inspectable (useful for the
// lab's debug views later) and needs no new dependency.
func (s *Store) MarshalSnapshot() ([]byte, error) {
	return json.Marshal(s.Snapshot())
}

// UnmarshalSnapshot loads a snapshot payload produced by MarshalSnapshot,
// replacing the store's entire state.
func (s *Store) UnmarshalSnapshot(data []byte) error {
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	s.Restore(m)
	return nil
}
