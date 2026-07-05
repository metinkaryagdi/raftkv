package raft

// HandleRequestVote processes an incoming RequestVote RPC (§5.2, §5.4.1). It is
// invoked by the transport on the receiving node.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &RequestVoteReply{}

	// Reply false if the candidate's term is stale.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return reply
	}

	// If we see a newer term, adopt it and revert to follower (which clears our
	// vote so we may grant one below).
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}

	// Grant the vote if we have not voted for anyone else this term and the
	// candidate's log is at least as up-to-date as ours.
	canVote := n.votedFor == "" || n.votedFor == args.CandidateID
	if canVote && n.isCandidateUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		// Granting a vote counts as hearing from the cluster: reset our timer so
		// we do not immediately start a competing election.
		n.resetElectionTimerLocked()
		n.logger.Event(n.id, Event{Kind: "vote_granted", Term: n.currentTerm, Role: n.role, Peer: args.CandidateID})
	}

	reply.Term = n.currentTerm
	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC (§5.3): it performs
// term arbitration, the log-consistency check at PrevLogIndex/PrevLogTerm, appends
// or reconciles entries, and advances the follower's commit index.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{}

	// Reject stale leaders.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return reply
	}

	// A valid AppendEntries at term >= ours means there is a current leader. Adopt
	// the term if newer, and (even at an equal term) step down from candidate.
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	} else if n.role != Follower {
		n.becomeFollowerLocked(args.Term)
	}

	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Term = n.currentTerm

	lastIdx, _ := n.lastLogInfoLocked()

	// Consistency check 1: we must actually have an entry at PrevLogIndex.
	if args.PrevLogIndex > lastIdx {
		reply.Success = false
		reply.ConflictIndex = lastIdx + 1 // tell the leader where our log ends
		reply.ConflictTerm = 0
		return reply
	}

	// Consistency check 2: the terms at PrevLogIndex must match. If not, report
	// the first index of our conflicting term so the leader can back up a whole
	// term at once.
	if got := n.termAtLocked(args.PrevLogIndex); got != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = got
		ci := args.PrevLogIndex
		for ci > 1 && n.termAtLocked(ci-1) == got {
			ci--
		}
		reply.ConflictIndex = ci
		return reply
	}

	// Append any new entries, reconciling conflicts. An existing entry that
	// disagrees on term (and everything after it) is truncated (§5.3). Entries we
	// already have with a matching term are left untouched so that a delayed or
	// duplicated RPC can never delete committed entries.
	for i := range args.Entries {
		entry := args.Entries[i]
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Advance commit index. We cannot commit past what we actually hold.
	if args.LeaderCommit > n.commitIndex {
		newLast, _ := n.lastLogInfoLocked()
		n.commitIndex = min64(args.LeaderCommit, newLast)
		n.signalApplyLocked()
	}

	reply.Success = true
	return reply
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
