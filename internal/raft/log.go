package raft

// This file holds helpers for reasoning about the retained log. log[0] is a
// movable sentinel representing (lastIncludedIndex, lastIncludedTerm) — the
// last entry folded into the most recent snapshot (0,0 if none yet). A logical
// log index therefore does not equal its slice position; every direct access
// must translate via offsetLocked first.

// offsetLocked translates a logical log index into a slice offset into n.log.
// The result is only a valid slice index if it falls in [0, len(n.log)-1];
// offset 0 always denotes the sentinel. Caller must hold mu.
func (n *Node) offsetLocked(index uint64) int {
	return int(index - n.lastIncludedIndex)
}

// lastLogInfoLocked returns the index and term of the last log entry. Caller must
// hold mu.
func (n *Node) lastLogInfoLocked() (index, term uint64) {
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// termAtLocked returns the term of the entry at the given logical index, or 0 if
// this node does not (yet) hold that index. An index before lastIncludedIndex
// has been compacted away; term information for it is only meaningful at
// exactly lastIncludedIndex (answered directly by the sentinel). Caller must
// hold mu.
func (n *Node) termAtLocked(index uint64) uint64 {
	if index < n.lastIncludedIndex {
		return 0
	}
	offset := n.offsetLocked(index)
	if offset >= len(n.log) {
		return 0
	}
	return n.log[offset].Term
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
