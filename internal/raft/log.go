package raft

// This file holds helpers for reasoning about the in-memory log. Because log[0]
// is a term-0 sentinel and snapshotting is out of scope, a log entry's Index is
// always equal to its position in the slice. That invariant keeps index math
// trivial throughout the rest of the package.

// lastLogInfoLocked returns the index and term of the last log entry. Caller must
// hold mu.
func (n *Node) lastLogInfoLocked() (index, term uint64) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// termAtLocked returns the term of the entry at the given index. Index 0 is the
// sentinel (term 0). Caller must hold mu.
func (n *Node) termAtLocked(index uint64) uint64 {
	if index >= uint64(len(n.log)) {
		return 0
	}
	return n.log[index].Term
}

// isCandidateUpToDateLocked implements the election restriction of §5.4.1: a
// candidate's log is at least as up-to-date as ours if its last entry has a
// higher term, or the same term and an index >= ours. Caller must hold mu.
func (n *Node) isCandidateUpToDateLocked(lastLogIndex, lastLogTerm uint64) bool {
	myIndex, myTerm := n.lastLogInfoLocked()
	if lastLogTerm != myTerm {
		return lastLogTerm > myTerm
	}
	return lastLogIndex >= myIndex
}
