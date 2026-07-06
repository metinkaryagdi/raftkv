package raft

import "testing"

// TestApplyConfigChangeNeverAddsSelf is a white-box regression test for a real
// bug found during Phase 10 design review: a conf_change entry replicates to
// (and is processed by) every member, including the node it names. Without an
// explicit self-exclusion check, a newly added node processing the very entry
// that added it would append its own id to its own n.peers — which means "the
// *other* nodes" — corrupting quorum() for the rest of that node's lifetime.
func TestApplyConfigChangeNeverAddsSelf(t *testing.T) {
	n := &Node{
		id:         "n6",
		peers:      nil,
		nextIndex:  make(map[string]uint64),
		matchIndex: make(map[string]uint64),
	}
	n.applyConfigChangeLocked(1, Command{Op: "conf_change", ConfigOp: "add", Key: "n6", Value: "addr"})

	if containsString(n.peers, "n6") {
		t.Fatalf("n.peers = %v; must never contain the node's own id", n.peers)
	}
	if len(n.peers) != 0 {
		t.Fatalf("n.peers = %v; want empty (the only entry processed named this node itself)", n.peers)
	}
}

// TestRecomputeConfigFromLogNeverAddsSelf is the equivalent regression test for
// the truncation-reversion path.
func TestRecomputeConfigFromLogNeverAddsSelf(t *testing.T) {
	n := &Node{
		id:          "n6",
		baseConfig:  nil,
		commitIndex: 1,
		log: []LogEntry{
			{Term: 1, Index: 0},
			{Term: 1, Index: 1, Command: Command{Op: "conf_change", ConfigOp: "add", Key: "n6", Value: "addr"}},
		},
	}
	n.recomputeConfigFromLogLocked()

	if containsString(n.peers, "n6") {
		t.Fatalf("n.peers = %v; must never contain the node's own id", n.peers)
	}
}
