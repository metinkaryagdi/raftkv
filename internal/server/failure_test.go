package server_test

import (
	"testing"
	"time"
)

// TestClusterAvailableAcrossLeaderCrash is the headline availability scenario:
// the cluster serves a write, its leader crashes, and it elects a new leader and
// keeps serving new writes — with no data loss. This is the "the system keeps
// going" demonstration.
func TestClusterAvailableAcrossLeaderCrash(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(2 * time.Second)
	leaderID := leader.Node().ID()

	// Write before the crash.
	c.setToLeader(2*time.Second, c.ids, "before", "1")

	// Crash the leader.
	c.net.Isolate(leaderID)
	survivors := c.aliveIDs(leaderID)

	// The survivors elect a new leader and accept a fresh write.
	c.leaderAmong(2*time.Second, survivors)
	c.setToLeader(2*time.Second, survivors, "after", "2")

	// Both the pre-crash and post-crash writes are present on every survivor.
	c.waitConvergedAmong(2*time.Second, survivors, map[string]string{
		"before": "1",
		"after":  "2",
	})
}

// TestPartitionSplitBrainPrevention demonstrates that a network partition cannot
// cause split-brain: only the majority side accepts writes, the minority side
// cannot commit, and after the partition heals the minority discards its
// uncommitted writes and adopts the majority's state.
func TestPartitionSplitBrainPrevention(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.setCommitTimeout(400 * time.Millisecond) // so minority writes fail fast
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(2 * time.Second)
	leaderID := leader.Node().ID()

	// Commit an agreed baseline.
	c.setToLeader(2*time.Second, c.ids, "k", "v0")

	// Split: put the old leader with one buddy (minority of 2) against the other
	// three (majority).
	buddy := c.aliveIDs(leaderID)[0]
	minority := []string{leaderID, buddy}
	majority := c.aliveIDs(leaderID, buddy)
	c.net.Partition(minority, majority)

	// Majority elects a leader and keeps serving.
	c.leaderAmong(2*time.Second, majority)
	c.setToLeader(2*time.Second, majority, "k", "v-majority")
	c.setToLeader(2*time.Second, majority, "only-majority", "yes")

	// The minority's old leader cannot commit a write: it must fail (timeout),
	// never silently succeed.
	if err := c.servers[leaderID].Set("k", "v-minority"); err == nil {
		t.Fatal("minority leader committed a write without a quorum (split-brain!)")
	}

	// Heal. Everyone must converge on the majority's state; the minority write is
	// gone.
	c.net.HealPartitions()
	c.leaderAmong(3*time.Second, c.ids)
	c.waitConverged(4*time.Second, map[string]string{
		"k":             "v-majority",
		"only-majority": "yes",
	})
}
