package logfmt_test

import (
	"testing"

	"github.com/metinkaryagdi/raftkv/internal/logfmt"
	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// TestFormatParseRoundTrip is the executable contract between Format (what
// cmd/raftkv's stderrLogger prints) and Parse (what the lab reads back): for
// every event shape the codebase actually emits, formatting then parsing must
// reproduce the original nodeID/Event exactly. This is a stronger guarantee
// than a comment cross-referencing the two functions, since a future format
// change that breaks the pairing fails this test immediately.
func TestFormatParseRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		nodeID string
		event  raft.Event
	}{
		{"start", "n1", raft.Event{Kind: "start", Term: 0, Role: raft.Follower}},
		{"election_start", "n2", raft.Event{Kind: "election_start", Term: 3, Role: raft.Candidate}},
		{"become_leader", "n3", raft.Event{Kind: "become_leader", Term: 5, Role: raft.Leader}},
		{"step_down", "n1", raft.Event{Kind: "step_down", Term: 6, Role: raft.Follower}},
		{"vote_granted_with_peer", "n2", raft.Event{Kind: "vote_granted", Term: 4, Role: raft.Follower, Peer: "n5"}},
		{"submit_with_info", "n4", raft.Event{Kind: "submit", Term: 2, Role: raft.Leader, Info: "set city"}},
		{"commit_with_info", "n4", raft.Event{Kind: "commit", Term: 2, Role: raft.Leader, Info: "commitIndex=7"}},
		{"install_snapshot_sent", "n1", raft.Event{Kind: "install_snapshot_sent", Term: 9, Role: raft.Leader, Peer: "n6", Info: "lastIncludedIndex=100"}},
		{"conf_change_proposed", "n1", raft.Event{Kind: "conf_change_proposed", Term: 1, Role: raft.Leader, Peer: "n6", Info: "add"}},
		{"peer_and_info_together", "raftkv-3", raft.Event{Kind: "install_snapshot_applied", Term: 12, Role: raft.Follower, Peer: "raftkv-0", Info: "lastIncludedIndex=42"}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			line := logfmt.Format(c.nodeID, c.event)
			gotID, gotEvent, ok := logfmt.Parse(line)
			if !ok {
				t.Fatalf("Parse(%q) failed to match", line)
			}
			if gotID != c.nodeID {
				t.Errorf("nodeID = %q, want %q", gotID, c.nodeID)
			}
			if gotEvent != c.event {
				t.Errorf("event = %+v, want %+v (line: %q)", gotEvent, c.event, line)
			}
		})
	}
}

// TestParseHandlesRealLogLoggerPrefix is a regression test for a real bug
// found by testing against a live `docker logs -f` stream: cmd/raftkv prints
// this line through a standard log.Logger configured with
// log.LstdFlags|log.Lmicroseconds, which prepends a timestamp — so the actual
// bytes on the wire are NEVER just Format's bare output. The in-isolation
// round-trip test above (TestFormatParseRoundTrip) calls Format then Parse
// directly and completely missed this, since it never exercises log.Logger at
// all. Every real line has this prefix, so Parse must handle it.
func TestParseHandlesRealLogLoggerPrefix(t *testing.T) {
	line := "2026/07/06 04:47:33.944321 [n1] start term=0 role=Follower"
	nodeID, e, ok := logfmt.Parse(line)
	if !ok {
		t.Fatalf("Parse(%q) failed to match a real, timestamp-prefixed log line", line)
	}
	if nodeID != "n1" || e.Kind != "start" || e.Term != 0 || e.Role != raft.Follower {
		t.Fatalf("Parse(%q) = %q, %+v; want n1, {start 0 Follower}", line, nodeID, e)
	}
}

// TestParseIgnoresUnrelatedLines confirms Parse safely reports ok=false (not
// an error, not a panic) for lines a node's log stream legitimately contains
// that aren't Event lines at all, e.g. the startup banner or a Fatal error.
func TestParseIgnoresUnrelatedLines(t *testing.T) {
	lines := []string{
		"",
		"node n1: raft(gRPC)=127.0.0.1:9001 http=127.0.0.1:8001 peers=[n2 n3]",
		"2026/07/06 04:18:19 some unrelated log line",
		"[n1] unknown_kind term=abc role=Leader",     // non-numeric term
		"[n1] some_kind term=1 role=TotallyNotARole", // unknown role
	}
	for _, line := range lines {
		if _, _, ok := logfmt.Parse(line); ok {
			t.Errorf("Parse(%q) unexpectedly matched", line)
		}
	}
}
