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

// Server is a single KV node: a Raft node plus the state machine it drives.
type Server struct {
	node  *raft.Node
	store *kvstore.Store

	commitTimeout time.Duration

	done chan struct{}

	// apply progress. appliedTerms[i] is the term of the entry applied at index i;
	// waitApplied polls these fields to learn when a proposal became durable.
	mu           sync.Mutex
	appliedIndex uint64
	appliedTerms []uint64
}

// New wraps a constructed (but not started) raft.Node.
func New(node *raft.Node) *Server {
	return &Server{
		node:          node,
		store:         kvstore.New(),
		commitTimeout: 2 * time.Second,
		done:          make(chan struct{}),
		appliedTerms:  []uint64{0}, // index 0 sentinel
	}
}

// SetCommitTimeout overrides how long a write waits for commit before returning
// ErrTimeout. Useful for tests and for tuning client-facing latency.
func (s *Server) SetCommitTimeout(d time.Duration) { s.commitTimeout = d }

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
// order, recording progress so client writes can wait for durability.
func (s *Server) applyLoop() {
	ch := s.node.ApplyCh()
	for {
		select {
		case <-s.done:
			return
		case msg := <-ch:
			s.store.Apply(msg.Command.Op, msg.Command.Key, msg.Command.Value)
			s.mu.Lock()
			s.appliedIndex = msg.Index
			for uint64(len(s.appliedTerms)) <= msg.Index {
				s.appliedTerms = append(s.appliedTerms, 0)
			}
			s.appliedTerms[msg.Index] = msg.Term
			s.mu.Unlock()
		}
	}
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
