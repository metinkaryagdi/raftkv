package raft

// Status is an immutable snapshot of a node's observable state, used by tests and
// tooling.
type Status struct {
	ID          string
	Role        Role
	Term        uint64
	LeaderID    string
	CommitIndex uint64
	LastApplied uint64
	// LogLength is the logical number of entries that have ever existed
	// (compacted-away entries plus retained ones), not just what's in memory —
	// otherwise this number would inexplicably shrink after every compaction,
	// which would look like data loss to a caller such as the lab dashboard.
	LogLength int
	// LastIncludedIndex is the highest index folded into the most recent
	// snapshot (0 if none yet). Combine with LogCopy(), whose entries start
	// right after this index, to interpret log positions correctly.
	LastIncludedIndex uint64
}

// Status returns a consistent snapshot of the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:                n.id,
		Role:              n.role,
		Term:              n.currentTerm,
		LeaderID:          n.leaderID,
		CommitIndex:       n.commitIndex,
		LastApplied:       n.lastApplied,
		LogLength:         int(n.lastIncludedIndex) + len(n.log) - 1, // exclude sentinel
		LastIncludedIndex: n.lastIncludedIndex,
	}
}

// IsLeader reports whether the node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// Term returns the node's current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// CommitIndex returns the node's current commit index.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// LogCopy returns a copy of the node's log excluding the index-0 sentinel. It is
// used by tests to assert that logs converge across the cluster.
func (n *Node) LogCopy() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]LogEntry, len(n.log)-1)
	copy(out, n.log[1:])
	return out
}
