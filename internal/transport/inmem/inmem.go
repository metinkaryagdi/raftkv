// Package inmem provides an in-process implementation of raft.Transport. All
// nodes share one Network that routes RPCs by direct method calls. It exists to
// make the consensus logic testable deterministically and, crucially, to inject
// faults — crashes (isolation), network partitions and latency — which are the
// scenarios that exercise Raft's safety guarantees.
package inmem

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// ErrUnreachable is returned when the network cannot deliver an RPC because the
// sender or receiver is isolated or the two are in different partitions.
var ErrUnreachable = errors.New("inmem: peer unreachable")

// Network is a virtual, in-process network connecting a set of Raft nodes.
// It is safe for concurrent use.
type Network struct {
	mu       sync.RWMutex
	handlers map[string]raft.RPCHandler
	group    map[string]int  // partition id; peers deliver only within the same group
	isolated map[string]bool // fully cut off (simulates a crashed/unplugged node)

	minLatency time.Duration
	maxLatency time.Duration
	rng        *rand.Rand
}

// NewNetwork creates an empty network with no latency.
func NewNetwork() *Network {
	return &Network{
		handlers: make(map[string]raft.RPCHandler),
		group:    make(map[string]int),
		isolated: make(map[string]bool),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register attaches a node's RPC handler under its id. All nodes start in the
// same partition (group 0).
func (nw *Network) Register(id string, h raft.RPCHandler) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.handlers[id] = h
	nw.group[id] = 0
}

// Endpoint returns a raft.Transport bound to the given node id.
func (nw *Network) Endpoint(id string) raft.Transport {
	return &endpoint{net: nw, from: id}
}

// SetLatency configures a uniform random per-RPC delay in [min, max]. Zero
// disables added latency (the default), keeping unit tests fast and
// deterministic.
func (nw *Network) SetLatency(min, max time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.minLatency, nw.maxLatency = min, max
}

// Isolate cuts a node off from every other node in both directions. It models a
// crash or an unplugged network cable: the node's loop may keep running, but no
// RPCs reach it and none of its RPCs land.
func (nw *Network) Isolate(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.isolated[id] = true
}

// Heal reconnects a previously isolated node.
func (nw *Network) Heal(id string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.isolated, id)
}

// Partition splits the cluster into disjoint groups; RPCs are delivered only
// between nodes in the same group. Any registered node not named in groups is
// placed in its own catch-all group and can talk to no one.
func (nw *Network) Partition(groups ...[]string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for id := range nw.group {
		nw.group[id] = -1 // catch-all: unnamed nodes are isolated by partition
	}
	for gi, members := range groups {
		for _, id := range members {
			nw.group[id] = gi
		}
	}
}

// HealPartitions merges everyone back into a single partition and clears all
// isolation.
func (nw *Network) HealPartitions() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	for id := range nw.group {
		nw.group[id] = 0
	}
	nw.isolated = make(map[string]bool)
}

// deliverable reports whether an RPC from -> to can be delivered, and the delay
// to apply. It takes the read lock itself.
func (nw *Network) deliverable(from, to string) (raft.RPCHandler, time.Duration, bool) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	if nw.isolated[from] || nw.isolated[to] {
		return nil, 0, false
	}
	if nw.group[from] != nw.group[to] {
		return nil, 0, false
	}
	h, ok := nw.handlers[to]
	if !ok {
		return nil, 0, false
	}
	var delay time.Duration
	if nw.maxLatency > 0 {
		span := nw.maxLatency - nw.minLatency
		delay = nw.minLatency
		if span > 0 {
			delay += time.Duration(nw.rng.Int63n(int64(span)))
		}
	}
	return h, delay, true
}

// endpoint is the per-node view of the network; it implements raft.Transport.
type endpoint struct {
	net  *Network
	from string
}

func (e *endpoint) SendRequestVote(target string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, delay, ok := e.net.deliverable(e.from, target)
	if !ok {
		return nil, ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return h.HandleRequestVote(args), nil
}

func (e *endpoint) SendAppendEntries(target string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, delay, ok := e.net.deliverable(e.from, target)
	if !ok {
		return nil, ErrUnreachable
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return h.HandleAppendEntries(args), nil
}
