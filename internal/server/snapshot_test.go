package server_test

import (
	"fmt"
	"testing"
	"time"
)

// TestSnapshotEndToEndThroughServer proves the full wired chain works, not just
// the raw-bytes raft-level tests: Server.applyLoop notices enough entries have
// been applied, calls kvstore.MarshalSnapshot + raft.Node.CompactLog on the
// leader, and a follower that fell behind past the compacted point installs the
// snapshot and calls kvstore.UnmarshalSnapshot to restore the exact same
// key-value state — all triggered automatically by crossing a (lowered, for the
// test) SnapshotThreshold, not by the test calling CompactLog directly.
func TestSnapshotEndToEndThroughServer(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.setSnapshotThreshold(5) // low enough that ~10 writes force a compaction
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	var laggard string
	for _, id := range c.ids {
		if c.servers[id] != leader {
			laggard = id
			break
		}
	}
	c.net.Isolate(laggard)

	survivors := c.aliveIDs(laggard)
	want := map[string]string{}
	for i := 0; i < 12; i++ {
		k, v := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		c.setToLeader(2*time.Second, survivors, k, v)
		want[k] = v
	}
	c.waitConvergedAmong(2*time.Second, survivors, want)

	// Give the leader's apply loop a moment to notice it crossed the threshold
	// and actually compact (this happens asynchronously after each apply).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if leader.Node().Status().LastIncludedIndex > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader.Node().Status().LastIncludedIndex == 0 {
		t.Fatal("leader never compacted its log despite crossing SnapshotThreshold")
	}

	c.net.Heal(laggard)

	// The laggard must catch up to the exact same key-value state, and it must
	// have gotten there via a real InstallSnapshot (LastIncludedIndex > 0),
	// proving MarshalSnapshot -> CompactLog -> InstallSnapshot -> UnmarshalSnapshot
	// all worked together correctly.
	c.waitConverged(3*time.Second, want)
	if c.servers[laggard].Node().Status().LastIncludedIndex == 0 {
		t.Fatalf("laggard %s converged without installing a snapshot", laggard)
	}
}
