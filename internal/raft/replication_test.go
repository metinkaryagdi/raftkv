package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

func setCmd(k, v string) raft.Command { return raft.Command{Op: "set", Key: k, Value: v} }

// TestLogReplicationBasic verifies the happy path: commands submitted to the
// leader are replicated to all followers, committed once a majority stores them,
// and applied in the same order everywhere.
func TestLogReplicationBasic(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	c.waitLeader(2*time.Second, c.ids)

	const n = 10
	for i := 0; i < n; i++ {
		c.submitToLeader(2*time.Second, c.ids, setCmd(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}

	c.waitLogsConverged(2*time.Second, c.ids, n)

	// Every node must have applied the identical command sequence.
	ref := c.appliedOf(c.ids[0])
	if len(ref) < n {
		t.Fatalf("expected >=%d applied commands, got %d", n, len(ref))
	}
	for _, id := range c.ids {
		got := c.appliedOf(id)
		if len(got) < n {
			t.Fatalf("node %s applied %d commands, want >=%d", id, len(got), n)
		}
		for i := 0; i < n; i++ {
			if got[i].Command != ref[i].Command || got[i].Index != ref[i].Index {
				t.Fatalf("node %s applied[%d]=%+v, want %+v", id, i, got[i], ref[i])
			}
		}
	}
}

// TestFollowerCatchUp verifies that a follower which was unreachable while
// commands were committed catches up automatically once it reconnects — the
// leader backs up nextIndex and streams the missing suffix.
func TestFollowerCatchUp(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader, _ := c.waitLeader(2*time.Second, c.ids)

	// Pick a follower and cut it off.
	var laggard string
	for _, id := range c.ids {
		if id != leader {
			laggard = id
			break
		}
	}
	c.net.Isolate(laggard)

	// The remaining 4 nodes are still a majority and keep committing.
	const n = 8
	for i := 0; i < n; i++ {
		c.submitToLeader(2*time.Second, c.aliveIDs(laggard), setCmd(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}
	c.waitLogsConverged(2*time.Second, c.aliveIDs(laggard), n)

	// Reconnect the laggard; it must catch up to the full log.
	c.net.Heal(laggard)
	c.waitLogsConverged(2*time.Second, c.ids, n)
}

// TestLogReconciliationAfterLeaderChange is the key safety test for §5.3: a leader
// change plus divergent tails must reconcile so that all committed logs are
// identical. We isolate the leader after it has extra uncommitted entries, elect
// a new leader that commits its own entries, then heal the old leader and require
// it to discard its conflicting tail and converge.
func TestLogReconciliationAfterLeaderChange(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader1, _ := c.waitLeader(2*time.Second, c.ids)

	// Commit a few entries everyone agrees on.
	for i := 0; i < 3; i++ {
		c.submitToLeader(2*time.Second, c.ids, setCmd(fmt.Sprintf("base%d", i), "x"))
	}
	c.waitLogsConverged(2*time.Second, c.ids, 3)

	// Isolate the leader together with one follower (minority of 2). The leader
	// keeps accepting writes locally, but they can never commit.
	var withLeader string
	for _, id := range c.ids {
		if id != leader1 {
			withLeader = id
			break
		}
	}
	c.net.Partition([]string{leader1, withLeader}, c.aliveIDs(leader1, withLeader))
	for i := 0; i < 4; i++ {
		// These go to the old leader (still leader in its minority) and stall.
		c.nodes[leader1].Submit(setCmd(fmt.Sprintf("orphan%d", i), "y"))
	}

	// The majority side (3 nodes) elects a new leader and makes progress.
	majority := c.aliveIDs(leader1, withLeader)
	c.waitLeader(2*time.Second, majority)
	for i := 0; i < 5; i++ {
		c.submitToLeader(2*time.Second, majority, setCmd(fmt.Sprintf("good%d", i), "z"))
	}
	c.waitLogsConverged(2*time.Second, majority, 3+5)

	// Heal the partition. The old leader must step down, drop its 4 orphan
	// entries, and adopt the majority's log. Everyone converges.
	c.net.HealPartitions()
	c.waitLogsConverged(4*time.Second, c.ids, 3+5)

	// Sanity: no node's committed log contains an "orphan" command.
	for _, id := range c.ids {
		for _, e := range c.nodes[id].LogCopy() {
			if len(e.Command.Key) >= 6 && e.Command.Key[:6] == "orphan" {
				t.Fatalf("node %s retained orphaned entry %+v", id, e)
			}
		}
	}
}

// TestNoCommitWithoutMajority verifies that a leader confined to a minority
// partition cannot advance its commit index, no matter how many entries it
// appends locally — the guard against split-brain data loss.
func TestNoCommitWithoutMajority(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader, _ := c.waitLeader(2*time.Second, c.ids)

	// Commit one entry cluster-wide first.
	c.submitToLeader(2*time.Second, c.ids, setCmd("k0", "v0"))
	c.waitLogsConverged(2*time.Second, c.ids, 1)
	baseCommit := c.nodes[leader].CommitIndex()

	// Trap the leader in a 2-node minority.
	var buddy string
	for _, id := range c.ids {
		if id != leader {
			buddy = id
			break
		}
	}
	c.net.Partition([]string{leader, buddy}, c.aliveIDs(leader, buddy))

	// Append entries to the isolated leader; they must NOT commit.
	for i := 0; i < 5; i++ {
		c.nodes[leader].Submit(setCmd(fmt.Sprintf("blocked%d", i), "y"))
	}
	time.Sleep(500 * time.Millisecond)

	if got := c.nodes[leader].CommitIndex(); got != baseCommit {
		t.Fatalf("minority leader advanced commitIndex from %d to %d without a majority", baseCommit, got)
	}
}
