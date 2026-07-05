package raft

import (
	"sync"
	"time"
)

// startElection transitions to candidate for a new term and solicits votes from
// all peers in parallel (§5.2). It runs in its own goroutine (spawned from tick)
// so the internal loop is never blocked on the network.
func (n *Node) startElection() {
	n.mu.Lock()
	// Guard against a role change that happened between tick() unlocking and us
	// acquiring the lock (e.g. an AppendEntries arrived).
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.resetElectionTimerLocked()

	term := n.currentTerm
	lastIdx, lastTerm := n.lastLogInfoLocked()
	peers := append([]string(nil), n.peers...)
	n.logger.Event(n.id, Event{Kind: "election_start", Term: term, Role: Candidate})
	n.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	// Collect vote results on a buffered channel; count them in this goroutine so
	// the tally is single-threaded and needs no extra synchronization.
	type voteResult struct {
		reply *RequestVoteReply
		err   error
	}
	results := make(chan voteResult, len(peers))
	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			reply, err := n.transport.SendRequestVote(target, args)
			results <- voteResult{reply: reply, err: err}
		}(p)
	}
	go func() { wg.Wait(); close(results) }()

	votes := 1 // vote for self
	needed := n.quorum()
	for res := range results {
		if res.err != nil || res.reply == nil {
			continue
		}
		reply := res.reply

		n.mu.Lock()
		// A higher term anywhere means we are stale: revert to follower.
		if reply.Term > n.currentTerm {
			n.becomeFollowerLocked(reply.Term)
			n.resetElectionTimerLocked()
			n.mu.Unlock()
			return
		}
		// If we are no longer the candidate for this term, abandon the count.
		if n.role != Candidate || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		granted := reply.VoteGranted
		if granted {
			votes++
		}
		if votes >= needed {
			n.becomeLeaderLocked()
			n.mu.Unlock()
			// Assert leadership immediately with a round of heartbeats so peers
			// stop their own election timers.
			n.broadcastAppendEntries()
			return
		}
		n.mu.Unlock()
	}
}

// becomeLeaderLocked promotes this node to leader and initializes per-peer
// replication bookkeeping (§5.3). Caller must hold mu and have just confirmed a
// won election.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	lastIdx, _ := n.lastLogInfoLocked()
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}
	// Force the next tick to broadcast immediately.
	n.lastHeartbeat = time.Time{}
	n.logger.Event(n.id, Event{Kind: "become_leader", Term: n.currentTerm, Role: Leader})
}
