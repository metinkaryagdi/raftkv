package raft

// broadcastAppendEntries replicates the leader's log to every peer in parallel.
// It is called right after Submit and on every heartbeat tick; a peer that has
// nothing new to receive simply gets an empty (heartbeat) AppendEntries, which
// still refreshes its election timer and carries the leader's commit index.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, p := range peers {
		go n.replicateToPeer(p, term)
	}
}

// replicateToPeer sends one AppendEntries to peer, tailored to that peer's
// nextIndex, and processes the reply: on success it advances match/next and may
// commit; on failure it backs nextIndex up using the conflict hints so the next
// round converges quickly (§5.3).
func (n *Node) replicateToPeer(peer string, term uint64) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	nextIdx := n.nextIndex[peer]
	if nextIdx < 1 {
		nextIdx = 1
	}
	// The peer needs entries this leader has already compacted away: it must
	// catch up via InstallSnapshot instead of AppendEntries.
	if nextIdx <= n.lastIncludedIndex {
		n.mu.Unlock()
		n.sendSnapshotToPeer(peer, term)
		return
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm := n.termAtLocked(prevLogIndex)

	// Defensive copy so the leader can keep appending while this RPC is in flight.
	tail := n.log[n.offsetLocked(nextIdx):]
	entries := make([]LogEntry, len(tail))
	copy(entries, tail)

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.transport.SendAppendEntries(peer, args)
	if err != nil || reply == nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// Step down if we discover a higher term.
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		n.resetElectionTimerLocked()
		return
	}
	// Ignore stale replies (term changed or we are no longer leader).
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		matched := prevLogIndex + uint64(len(entries))
		if matched > n.matchIndex[peer] {
			n.matchIndex[peer] = matched
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeAdvanceCommitLocked()
		return
	}

	// Failure: back nextIndex up toward the conflict point.
	n.nextIndex[peer] = n.backoffNextIndexLocked(reply)
}

// backoffNextIndexLocked computes the next value of nextIndex after a rejected
// AppendEntries, using the fast-backtracking hints (ConflictIndex/ConflictTerm)
// so the leader need not decrement one index at a time. Caller must hold mu.
func (n *Node) backoffNextIndexLocked(reply *AppendEntriesReply) uint64 {
	if reply.ConflictTerm == 0 {
		// Follower's log is shorter than PrevLogIndex; jump straight to its end.
		if reply.ConflictIndex < 1 {
			return 1
		}
		return reply.ConflictIndex
	}
	// Look for the last entry the leader has in ConflictTerm. Terms increase along
	// the log, so we can stop once we drop below the conflict term. i is a slice
	// offset (never the sentinel at offset 0); translate back to a logical index
	// on return. If the leader has already compacted past the entries it would
	// need to check, the loop simply finds nothing and falls through — the
	// caller's next round then sees nextIndex <= lastIncludedIndex and correctly
	// switches to InstallSnapshot.
	for i := len(n.log) - 1; i >= 1; i-- {
		if n.log[i].Term == reply.ConflictTerm {
			return uint64(i) + n.lastIncludedIndex + 1
		}
		if n.log[i].Term < reply.ConflictTerm {
			break
		}
	}
	// Leader has no entry in that term: skip the whole term on the follower.
	if reply.ConflictIndex < 1 {
		return 1
	}
	return reply.ConflictIndex
}
