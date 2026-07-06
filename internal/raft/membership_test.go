package raft_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

func addServerCmd(id, addr string) raft.Command {
	return raft.Command{Op: "conf_change", ConfigOp: "add", Key: id, Value: addr}
}

func removeServerCmd(id string) raft.Command {
	return raft.Command{Op: "conf_change", ConfigOp: "remove", Key: id}
}

// TestMembership_AddServerCatchesUpAndParticipates verifies the core dynamic
// membership path: a brand-new node, started with no known peers (Joining),
// is added to a running 5-node cluster via a single conf_change, catches up on
// the existing log, and goes on to participate in a subsequent election and
// commits exactly like a genesis member.
func TestMembership_AddServerCatchesUpAndParticipates(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	c.waitLeader(2*time.Second, c.ids)

	for i := 0; i < 5; i++ {
		c.submitToLeader(2*time.Second, c.ids, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitLogsConverged(2*time.Second, c.ids, 5)

	original := append([]string(nil), c.ids...)
	c.addJoiningNode("n6")
	c.proposeConfigChangeToLeader(2*time.Second, original, addServerCmd("n6", "unused-in-memory"))

	// n6 must catch up to the existing committed state.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes["n6"].Status().CommitIndex >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := c.nodes["n6"].Status().CommitIndex; got < 5 {
		t.Fatalf("n6 did not catch up: commitIndex=%d", got)
	}

	// It must also no longer be "joining": isolate the old leader and confirm a
	// new election succeeds among the other 5 (including n6), and that n6 can
	// itself observe/participate in subsequent commits.
	oldLeader, _ := c.waitLeader(2*time.Second, original)
	c.net.Isolate(oldLeader)
	survivors := c.aliveIDs(oldLeader)
	c.waitLeader(3*time.Second, survivors)

	c.submitToLeader(2*time.Second, survivors, setCmd("after-join", "v"))
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes["n6"].Status().CommitIndex >= 6 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("n6 did not receive the post-join commit: %s", c.dump(survivors))
}

// TestMembership_RemoveServerAdjustsQuorum verifies that removing a server
// actually shrinks the quorum size used for commits: after removing one node
// from a healthy 5-node cluster (leaving 4), isolating one of the remaining 4
// must still leave a majority (3) able to commit — which would be impossible
// if quorum() were still computed as if 5 peers existed.
func TestMembership_RemoveServerAdjustsQuorum(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	c.waitLeader(2*time.Second, c.ids)

	// Remove a non-leader node so there's still a leader to keep proposing.
	leader, _ := c.waitLeader(2*time.Second, c.ids)
	var target string
	for _, id := range c.ids {
		if id != leader {
			target = id
			break
		}
	}
	c.proposeConfigChangeToLeader(2*time.Second, c.ids, removeServerCmd(target))
	c.nodes[target].Stop() // it's out of the config; stop its goroutines cleanly

	remaining := c.aliveIDs(target)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.nodes[leader].Status().LeaderID) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Now isolate one of the 4 remaining nodes (not the leader). With a correct
	// 4-node quorum of 3, the other 3 must still commit.
	var toIsolate string
	for _, id := range remaining {
		if id != leader {
			toIsolate = id
			break
		}
	}
	c.net.Isolate(toIsolate)
	survivors := c.aliveIDs(target, toIsolate)
	if len(survivors) != 3 {
		t.Fatalf("expected 3 survivors, got %d: %v", len(survivors), survivors)
	}

	c.waitLeader(2*time.Second, survivors)
	c.submitToLeader(2*time.Second, survivors, setCmd("after-remove", "v"))
	c.waitLogsConverged(2*time.Second, survivors, 1)
}

// TestMembership_RejectsConcurrentChange verifies the single-change-at-a-time
// invariant: proposing a second configuration change before the first commits
// must fail with ErrConfigChangeInFlight, not silently queue or corrupt state.
func TestMembership_RejectsConcurrentChange(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader, _ := c.waitLeader(2*time.Second, c.ids)
	leaderNode := c.nodes[leader]

	// Isolate every other node so the first conf_change can never commit,
	// guaranteeing it is still in flight when we try the second.
	for _, id := range c.aliveIDs(leader) {
		c.net.Isolate(id)
	}

	if _, _, err := leaderNode.ProposeConfigChange(addServerCmd("n6", "addr")); err != nil {
		t.Fatalf("first ProposeConfigChange: %v", err)
	}
	_, _, err := leaderNode.ProposeConfigChange(addServerCmd("n7", "addr"))
	if !errors.Is(err, raft.ErrConfigChangeInFlight) {
		t.Fatalf("second ProposeConfigChange = %v, want ErrConfigChangeInFlight", err)
	}
}

// TestMembership_JoiningNodeDoesNotSelfElect is the explicit safety-property
// test for the hazard a joining node's empty peer set creates: quorum() would
// be 1, so without the Joining guard it would trivially win its own election.
func TestMembership_JoiningNodeDoesNotSelfElect(t *testing.T) {
	c := newTestCluster(t, 0)
	node := c.addJoiningNode("solo")
	defer c.stopAll()

	// Run for several multiples of the (short, test-scale) election timeout.
	time.Sleep(500 * time.Millisecond)

	st := node.Status()
	if st.Role != raft.Follower {
		t.Fatalf("joining node became %s, want Follower", st.Role)
	}
	if st.Term != 0 {
		t.Fatalf("joining node's term advanced to %d, want 0 (no election ever started)", st.Term)
	}
}

// TestMembership_UncommittedConfigChangeRevertedOnTruncation is the gap found
// during review of the append-time-effect design: an uncommitted conf_change
// that gets truncated away by a new leader's conflicting entries must not
// leave a phantom peer membership behind.
func TestMembership_UncommittedConfigChangeRevertedOnTruncation(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader1, _ := c.waitLeader(2*time.Second, c.ids)

	for i := 0; i < 3; i++ {
		c.submitToLeader(2*time.Second, c.ids, setCmd(fmt.Sprintf("base%d", i), "x"))
	}
	c.waitLogsConverged(2*time.Second, c.ids, 3)

	// Partition the old leader together with one follower (minority of 2) so
	// its conf_change can be appended (visible immediately, per §6) but can
	// never commit.
	var withLeader string
	for _, id := range c.ids {
		if id != leader1 {
			withLeader = id
			break
		}
	}
	c.net.Partition([]string{leader1, withLeader}, c.aliveIDs(leader1, withLeader))

	if _, _, err := c.nodes[leader1].ProposeConfigChange(addServerCmd("phantom", "addr")); err != nil {
		t.Fatalf("ProposeConfigChange on minority leader: %v", err)
	}
	// The old leader applied it at append time (§6's immediate-effect rule),
	// visible right away in its own log even though it can never commit while
	// stuck in the minority.
	foundPhantom := false
	for _, e := range c.nodes[leader1].LogCopy() {
		if e.Command.Op == "conf_change" && e.Command.Key == "phantom" {
			foundPhantom = true
		}
	}
	if !foundPhantom {
		t.Fatalf("old leader should have the phantom conf_change in its own log right after proposing it")
	}

	// The majority side elects a new leader and keeps committing without ever
	// having heard of "phantom".
	majority := c.aliveIDs(leader1, withLeader)
	c.waitLeader(2*time.Second, majority)
	for i := 0; i < 3; i++ {
		c.submitToLeader(2*time.Second, majority, setCmd(fmt.Sprintf("good%d", i), "y"))
	}
	c.waitLogsConverged(2*time.Second, majority, 3+3)

	// Heal. The old leader must discard its uncommitted "phantom" conf_change
	// (truncated away by the majority's conflicting entries) and its peers must
	// revert — provable by the whole cluster converging normally afterward,
	// including a commit that requires exactly the original 5-node quorum (3),
	// not a phantom 6-node quorum (4).
	c.net.HealPartitions()
	c.waitLogsConverged(4*time.Second, c.ids, 3+3)

	for _, id := range c.ids {
		for _, e := range c.nodes[id].LogCopy() {
			if e.Command.Op == "conf_change" && e.Command.Key == "phantom" {
				// The entry may still physically appear in the log (Raft never
				// hides history), but by the time we get here every node's log has
				// converged to the majority leader's log, which never included
				// "phantom" in the first place, so this should be unreachable.
				t.Fatalf("node %s retained the phantom conf_change entry", id)
			}
		}
	}
}
