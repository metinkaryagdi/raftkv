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

// HandleAppendEntries processes an incoming AppendEntries RPC. In Phase 1 this
// covers term arbitration and the leader-recognition/heartbeat path; the log
// consistency check and entry appending are added in Phase 2.
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
	reply.Success = true
	return reply
}
