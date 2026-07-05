package raft

// Submit appends a command to the leader's log and kicks off replication. It
// returns the index and term the command was assigned and whether this node is
// the leader. A successful return does NOT mean the command is committed — the
// caller learns that when the entry surfaces on ApplyCh at the returned index.
func (n *Node) Submit(cmd Command) (index uint64, term uint64, isLeader bool) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, false
	}
	term = n.currentTerm
	lastIdx, _ := n.lastLogInfoLocked()
	index = lastIdx + 1
	n.log = append(n.log, LogEntry{Term: term, Index: index, Command: cmd})
	n.logger.Event(n.id, Event{Kind: "submit", Term: term, Role: Leader,
		Info: cmd.Op + " " + cmd.Key})
	n.mu.Unlock()

	// Replicate immediately rather than waiting for the next heartbeat tick.
	n.broadcastAppendEntries()
	return index, term, true
}

// maybeAdvanceCommitLocked advances commitIndex to the highest index replicated
// on a majority, subject to the current-term restriction of §5.4.2: a leader only
// commits an entry from its own term directly (earlier-term entries are then
// committed transitively). Caller must hold mu.
func (n *Node) maybeAdvanceCommitLocked() {
	lastIdx, _ := n.lastLogInfoLocked()
	quorum := n.quorum()
	for N := lastIdx; N > n.commitIndex; N-- {
		if n.log[N].Term != n.currentTerm {
			// Terms only increase along the log, so no lower index can be in the
			// current term either; stop scanning.
			break
		}
		count := 1 // the leader itself has entry N
		for _, p := range n.peers {
			if n.matchIndex[p] >= N {
				count++
			}
		}
		if count >= quorum {
			n.commitIndex = N
			n.logger.Event(n.id, Event{Kind: "commit", Term: n.currentTerm, Role: Leader,
				Info: "commitIndex=" + itoa(N)})
			n.signalApplyLocked()
			return
		}
	}
}

// signalApplyLocked nudges the applier goroutine without blocking. Caller holds mu.
func (n *Node) signalApplyLocked() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

// applier delivers newly committed entries to applyCh in index order, advancing
// lastApplied. It runs on every node (leaders and followers alike).
func (n *Node) applier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applySignal:
		}
		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			n.lastApplied++
			entry := n.log[n.lastApplied]
			n.mu.Unlock()

			msg := ApplyMsg{Index: entry.Index, Term: entry.Term, Command: entry.Command}
			select {
			case n.applyCh <- msg:
			case <-n.stopCh:
				return
			}
		}
	}
}
