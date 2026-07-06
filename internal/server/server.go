// Package server binds a raft.Node to the key-value state machine and exposes
// client operations. It owns the apply loop: as Raft commits entries, the server
// applies them to the store in order and unblocks any client waiting for its
// write to become durable.
package server

import (
	"errors"
	"sync"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/kvstore"
	"github.com/metinkaryagdi/raftkv/internal/raft"
)

var (
	// ErrNotLeader is returned when a write (or a linearizable read) is attempted
	// on a non-leader. Clients should redirect to LeaderHint.
	ErrNotLeader = errors.New("not leader")
	// ErrLostLeadership means the entry this client submitted was overwritten by a
	// new leader before committing, so the write did not take effect.
	ErrLostLeadership = errors.New("leadership changed before commit")
	// ErrTimeout means the write was not committed within the deadline.
	ErrTimeout = errors.New("timed out waiting for commit")
)

// defaultSnapshotThreshold is how many entries the server applies between
// snapshots by default: large enough that ordinary test/demo traffic (tens to
// hundreds of commands) never spuriously triggers one, so tests exercising the
// snapshot path do so deliberately via SetSnapshotThreshold.
const defaultSnapshotThreshold = 1000

// Server is a single KV node: a Raft node plus the state machine it drives.
type Server struct {
	node  *raft.Node
	store *kvstore.Store

	commitTimeout     time.Duration
	snapshotThreshold uint64

	done chan struct{}

	// apply progress. appliedTerms[i] is the term of the entry applied at index i;
	// waitApplied polls these fields to learn when a proposal became durable.
	mu             sync.Mutex
	appliedIndex   uint64
	appliedTerms   []uint64
	lastSnapshotAt uint64 // logical index of the last entry folded into a snapshot
}

// New wraps a constructed (but not started) raft.Node.
func New(node *raft.Node) *Server {
	return &Server{
		node:              node,
		store:             kvstore.New(),
		commitTimeout:     2 * time.Second,
		snapshotThreshold: defaultSnapshotThreshold,
		done:              make(chan struct{}),
		appliedTerms:      []uint64{0}, // index 0 sentinel
	}
}

// SetCommitTimeout overrides how long a write waits for commit before returning
// ErrTimeout. Useful for tests and for tuning client-facing latency.
func (s *Server) SetCommitTimeout(d time.Duration) { s.commitTimeout = d }

// SetSnapshotThreshold overrides how many entries are applied between
// snapshots (default 1000). Tests lower this to exercise the snapshot/
// InstallSnapshot path deterministically without submitting thousands of
// commands.
func (s *Server) SetSnapshotThreshold(n int) { s.snapshotThreshold = uint64(n) }

// Node exposes the underlying raft node (status, lifecycle).
func (s *Server) Node() *raft.Node { return s.node }

// Store exposes the state machine (used for equivalence checks in tests).
func (s *Server) Store() *kvstore.Store { return s.store }

// Start begins the apply loop. The caller is responsible for starting the raft
// node's networking and calling node.Start().
func (s *Server) Start() {
	go s.applyLoop()
}

// Stop halts the apply loop. It does not stop the underlying raft node.
func (s *Server) Stop() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
}

// applyLoop consumes committed entries and applies them to the store in index
// order, recording progress so client writes can wait for durability. It also
// consumes installed snapshots (delivered when this node was too far behind and
// had to catch up via InstallSnapshot instead of normal replication) and, on the
// leader-applied side, triggers compaction once enough entries have piled up.
func (s *Server) applyLoop() {
	applyCh := s.node.ApplyCh()
	snapshotCh := s.node.SnapshotCh()
	for {
		select {
		case <-s.done:
			return
		case msg := <-snapshotCh:
			if err := s.store.UnmarshalSnapshot(msg.Data); err != nil {
				// The state machine could not load the snapshot; nothing sensible to
				// do but leave it as-is and let replication keep this node's applied
				// state as the source of truth until a future snapshot succeeds.
				continue
			}
			s.mu.Lock()
			s.appliedIndex = msg.LastIncludedIndex
			s.lastSnapshotAt = msg.LastIncludedIndex
			for uint64(len(s.appliedTerms)) <= msg.LastIncludedIndex {
				s.appliedTerms = append(s.appliedTerms, 0)
			}
			s.mu.Unlock()
		case msg := <-applyCh:
			s.store.Apply(msg.Command.Op, msg.Command.Key, msg.Command.Value)
			s.mu.Lock()
			s.appliedIndex = msg.Index
			for uint64(len(s.appliedTerms)) <= msg.Index {
				s.appliedTerms = append(s.appliedTerms, 0)
			}
			s.appliedTerms[msg.Index] = msg.Term
			due := msg.Index-s.lastSnapshotAt >= s.snapshotThreshold
			s.mu.Unlock()

			if due {
				s.maybeCompact(msg.Index)
			}
		}
	}
}

// maybeCompact takes a snapshot of the state machine and asks Raft to discard
// log entries up to upToIndex. Errors are non-fatal: compaction is an
// optimization, not a correctness requirement, so a failed attempt just means
// the log stays a bit longer and the next threshold crossing tries again.
func (s *Server) maybeCompact(upToIndex uint64) {
	data, err := s.store.MarshalSnapshot()
	if err != nil {
		return
	}
	if err := s.node.CompactLog(data, upToIndex); err != nil {
		return
	}
	s.mu.Lock()
	s.lastSnapshotAt = upToIndex
	s.mu.Unlock()
}

// Set proposes a key/value write and blocks until it is committed and applied, or
// fails. Only the leader can accept it.
func (s *Server) Set(key, value string) error {
	return s.propose(raft.Command{Op: "set", Key: key, Value: value})
}

// Delete proposes a key deletion, with the same semantics as Set.
func (s *Server) Delete(key string) error {
	return s.propose(raft.Command{Op: "delete", Key: key})
}

func (s *Server) propose(cmd raft.Command) error {
	index, term, isLeader := s.node.Submit(cmd)
	if !isLeader {
		return ErrNotLeader
	}
	return s.waitApplied(index, term, s.commitTimeout)
}

// Get performs a leader-only read. Serving reads exclusively from the leader
// gives read-your-writes/linearizable-ish behavior for this project; a fully
// linearizable read would add a ReadIndex heartbeat barrier (out of scope).
func (s *Server) Get(key string) (string, bool, error) {
	if !s.node.IsLeader() {
		return "", false, ErrNotLeader
	}
	v, ok := s.store.Get(key)
	return v, ok, nil
}

// LeaderHint returns the id of the leader this node last heard from, for client
// redirection. It may be empty during an election.
func (s *Server) LeaderHint() string {
	return s.node.Status().LeaderID
}

// waitApplied blocks until the entry at index is applied. If a different term was
// applied at that index, the original proposal was overwritten (leadership
// changed) and the write is reported as lost.
func (s *Server) waitApplied(index, term uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		if s.appliedIndex >= index {
			applied := s.appliedTerms[index]
			s.mu.Unlock()
			if applied != term {
				return ErrLostLeadership
			}
			return nil
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			return ErrTimeout
		}
		select {
		case <-s.done:
			return ErrTimeout
		case <-time.After(3 * time.Millisecond):
		}
	}
}
