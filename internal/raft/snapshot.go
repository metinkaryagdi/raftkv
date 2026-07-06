package raft

import "fmt"

// SnapshotMsg is delivered on SnapshotCh whenever this node installs a snapshot
// received from a leader. The state machine must replace its current state with
// Data entirely (not merge it).
type SnapshotMsg struct {
	Data              []byte
	LastIncludedIndex uint64
}

// CompactLog discards all log entries up to and including upToIndex, replacing
// them with a snapshot of the state machine at that point. upToIndex must not
// exceed lastApplied: snapshotting ahead of what the state machine has actually
// applied would let the applier goroutine (apply.go) read data that no longer
// exists. data is the serialized state machine snapshot; raft treats it as
// opaque and only hands it back out via SnapshotCh on a future InstallSnapshot.
func (n *Node) CompactLog(data []byte, upToIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if upToIndex > n.lastApplied {
		return fmt.Errorf("raft: cannot compact up to index %d, only %d entries have been applied", upToIndex, n.lastApplied)
	}
	if upToIndex <= n.lastIncludedIndex {
		return nil // already compacted at least this far; nothing to do
	}

	term := n.termAtLocked(upToIndex)
	offset := n.offsetLocked(upToIndex)
	retained := make([]LogEntry, len(n.log)-offset)
	copy(retained, n.log[offset:])
	retained[0] = LogEntry{Term: term, Index: upToIndex} // the new movable sentinel

	n.log = retained
	n.lastIncludedIndex = upToIndex
	n.lastIncludedTerm = term
	n.snapshotData = data
	return nil
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC (§7): it
// performs the same term arbitration as HandleAppendEntries, then replaces (or
// trims) this node's log to start from the snapshot's coverage and hands the
// snapshot data to the state machine via SnapshotCh.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()

	reply := &InstallSnapshotReply{}

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		n.mu.Unlock()
		return reply
	}
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.role != Follower {
		n.becomeFollowerLocked(args.Term)
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Term = n.currentTerm

	// Stale or duplicate (InstallSnapshot may be retried): we already cover this
	// range, so there's nothing to install. Idempotent no-op.
	if args.LastIncludedIndex <= n.lastIncludedIndex {
		n.mu.Unlock()
		return reply
	}

	// If we happen to already retain an entry exactly at LastIncludedIndex with a
	// matching term, we can keep everything after it (it's already consistent).
	// Otherwise the snapshot supersedes our entire log.
	lastIdx, _ := n.lastLogInfoLocked()
	if args.LastIncludedIndex <= lastIdx && n.termAtLocked(args.LastIncludedIndex) == args.LastIncludedTerm {
		offset := n.offsetLocked(args.LastIncludedIndex)
		retained := make([]LogEntry, len(n.log)-offset)
		copy(retained, n.log[offset:])
		n.log = retained
	} else {
		n.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}
	n.lastIncludedIndex = args.LastIncludedIndex
	n.lastIncludedTerm = args.LastIncludedTerm
	n.snapshotData = args.Data

	if n.commitIndex < args.LastIncludedIndex {
		n.commitIndex = args.LastIncludedIndex
	}
	if n.lastApplied < args.LastIncludedIndex {
		n.lastApplied = args.LastIncludedIndex
	}
	n.logger.Event(n.id, Event{Kind: "install_snapshot_applied", Term: n.currentTerm, Role: n.role,
		Peer: args.LeaderID, Info: "lastIncludedIndex=" + itoa(args.LastIncludedIndex)})
	n.mu.Unlock()

	// Deliver the snapshot to the state machine exactly like a committed entry,
	// outside the lock (matches applier()'s existing convention for applyCh).
	select {
	case n.snapshotCh <- SnapshotMsg{Data: args.Data, LastIncludedIndex: args.LastIncludedIndex}:
	case <-n.stopCh:
	}
	return reply
}

// sendSnapshotToPeer sends the leader's current snapshot to peer when its
// nextIndex has fallen at or before lastIncludedIndex, meaning the entries it
// needs have already been compacted away. On success it advances match/next so
// the normal replication loop (replication.go) takes over from there.
func (n *Node) sendSnapshotToPeer(peer string, term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		Data:              n.snapshotData,
	}
	n.logger.Event(n.id, Event{Kind: "install_snapshot_sent", Term: term, Role: Leader,
		Peer: peer, Info: "lastIncludedIndex=" + itoa(args.LastIncludedIndex)})
	n.mu.Unlock()

	reply, err := n.transport.SendInstallSnapshot(peer, args)
	if err != nil || reply == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		n.resetElectionTimerLocked()
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = args.LastIncludedIndex + 1
}
