package raft

import "testing"

// TestBackoffFallsThroughPastCompactedPrefix is a white-box test (package raft,
// not raft_test) of backoffNextIndexLocked's edge case: when the conflicting
// term the follower reports predates everything the leader still retains (it
// was already folded into a snapshot), the backward scan cannot find a match
// and must fall through cleanly to a value at or below lastIncludedIndex —
// which is exactly what makes replicateToPeer's `nextIdx <= lastIncludedIndex`
// check correctly switch to InstallSnapshot on the very next round, rather than
// panicking or looping forever.
func TestBackoffFallsThroughPastCompactedPrefix(t *testing.T) {
	n := &Node{
		lastIncludedIndex: 50,
		lastIncludedTerm:  3,
		log: []LogEntry{
			{Term: 3, Index: 50}, // sentinel
			{Term: 5, Index: 51},
			{Term: 5, Index: 52},
		},
	}

	// The follower's conflicting term (2) predates the leader's retained log
	// entirely (whose oldest retained real entry is term 5, and the sentinel
	// itself is term 3) — the leader has no way to find where term 2 ended.
	reply := &AppendEntriesReply{ConflictTerm: 2, ConflictIndex: 30}
	got := n.backoffNextIndexLocked(reply)

	if got > n.lastIncludedIndex {
		t.Fatalf("backoffNextIndexLocked returned %d, want <= lastIncludedIndex (%d) so the next round installs a snapshot",
			got, n.lastIncludedIndex)
	}

	// Simulate the caller: nextIndex[peer] = got. Confirm this value would
	// indeed take the InstallSnapshot branch in replicateToPeer, not a normal
	// AppendEntries slice (which would panic on an out-of-range offset if it
	// somehow fell below the retained log's start).
	if got > n.lastIncludedIndex {
		t.Fatalf("value %d would not trigger the InstallSnapshot branch", got)
	}
}

// TestBackoffFindsMatchWithinRetainedLog is the companion happy-path check: when
// the conflicting term *is* still present in the leader's retained log, the
// scan must find it and return the logical index right after it (translated
// correctly from the internal slice offset via lastIncludedIndex).
func TestBackoffFindsMatchWithinRetainedLog(t *testing.T) {
	n := &Node{
		lastIncludedIndex: 50,
		lastIncludedTerm:  3,
		log: []LogEntry{
			{Term: 3, Index: 50}, // sentinel
			{Term: 5, Index: 51},
			{Term: 5, Index: 52},
			{Term: 6, Index: 53},
		},
	}

	reply := &AppendEntriesReply{ConflictTerm: 5, ConflictIndex: 51}
	got := n.backoffNextIndexLocked(reply)

	// Term 5's last entry is at logical index 52 (slice offset 2); the leader
	// should back up to just after it: logical index 53.
	const want = 53
	if got != want {
		t.Fatalf("backoffNextIndexLocked = %d, want %d", got, want)
	}
}
