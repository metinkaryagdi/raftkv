package raft

// broadcastAppendEntries sends an AppendEntries RPC to every peer in parallel.
// In Phase 1 these are pure heartbeats (empty Entries) whose only job is to
// suppress follower election timers and to detect a higher term. Phase 2 extends
// this to carry log entries and advance the commit index.
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	leaderCommit := n.commitIndex
	prevLogIndex, prevLogTerm := n.lastLogInfoLocked()
	peers := append([]string(nil), n.peers...)
	n.mu.Unlock()

	for _, p := range peers {
		go func(target string) {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: leaderCommit,
			}
			reply, err := n.transport.SendAppendEntries(target, args)
			if err != nil || reply == nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				n.resetElectionTimerLocked()
			}
			n.mu.Unlock()
		}(p)
	}
}
