package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// TestSnapshot_LeaderCompactsAndFollowerCatchesUp verifies the core snapshotting
// path end to end: a follower falls far enough behind that the leader compacts
// past what it needs, and once reconnected it must catch up via InstallSnapshot
// rather than normal AppendEntries (proven by it ending up with a nonzero
// LastIncludedIndex of its own, which only happens by installing a snapshot).
func TestSnapshot_LeaderCompactsAndFollowerCatchesUp(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader, _ := c.waitLeader(2*time.Second, c.ids)

	for i := 0; i < 10; i++ {
		c.submitToLeader(2*time.Second, c.ids, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitLogsConverged(2*time.Second, c.ids, 10)

	var laggard string
	for _, id := range c.ids {
		if id != leader {
			laggard = id
			break
		}
	}
	c.net.Isolate(laggard)

	survivors := c.aliveIDs(laggard)
	for i := 10; i < 20; i++ {
		c.submitToLeader(2*time.Second, survivors, setCmd(fmt.Sprintf("k%d", i), "v"))
	}
	c.waitLogsConverged(2*time.Second, survivors, 20)

	leaderNode := c.nodes[leader]
	st := leaderNode.Status()
	if err := leaderNode.CompactLog([]byte("fake-snapshot"), st.LastApplied); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	if leaderNode.Status().LastIncludedIndex == 0 {
		t.Fatalf("leader's LastIncludedIndex should be > 0 after compaction")
	}

	c.net.Heal(laggard)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[laggard].Status().CommitIndex >= 20 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	lst := c.nodes[laggard].Status()
	if lst.CommitIndex < 20 {
		t.Fatalf("laggard did not catch up: %+v", lst)
	}
	if lst.LastIncludedIndex == 0 {
		t.Fatalf("laggard should have installed a snapshot (LastIncludedIndex > 0), got %+v", lst)
	}
}

// committedPrefix returns only the entries of lg with Index <= upTo: the part
// of the log the Log Matching Property actually guarantees is identical across
// nodes at any instant. An uncommitted tail can legitimately and harmlessly
// differ by a heartbeat or two between nodes, so comparing the full retained
// log (rather than just this prefix) is too strict for a concurrent test.
func committedPrefix(lg []raft.LogEntry, upTo uint64) []raft.LogEntry {
	out := lg[:0:0]
	for _, e := range lg {
		if e.Index <= upTo {
			out = append(out, e)
		}
	}
	return out
}

// waitCommitConverged blocks until every node in ids reports the same
// commitIndex, which is at least wantMin. Unlike the shared waitLogsConverged
// helper, this does NOT compare LogCopy() across nodes — necessary here since a
// compacted leader's LogCopy() is legitimately shorter than an uncompacted
// follower's.
func (c *testCluster) waitCommitConverged(within time.Duration, ids []string, wantMin uint64) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		first := c.nodes[ids[0]].CommitIndex()
		if first >= wantMin {
			allEqual := true
			for _, id := range ids[1:] {
				if c.nodes[id].CommitIndex() != first {
					allEqual = false
					break
				}
			}
			if allEqual {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("commit index did not converge (want >=%d) within %v: %s", wantMin, within, c.dump(ids))
}

// TestSnapshot_InterleavesWithNormalReplication verifies that repeatedly
// compacting one specific node's log while all nodes stay healthy and continue
// to replicate normally causes no regression. The compacted node (n1, fixed
// regardless of role — real Raft allows any node to independently snapshot its
// own applied state, not just the leader) is deliberately excluded from the
// cross-node log comparison, since its LogCopy() is legitimately shorter after
// compaction; the other four must still converge on byte-identical logs with
// each other, and every node's commit index must keep advancing together. n1 is
// fixed rather than "whichever node is leader" so the test is not disrupted by
// an incidental leadership change under heavy concurrent test-suite load.
func TestSnapshot_InterleavesWithNormalReplication(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	c.waitLeader(2*time.Second, c.ids)

	const compacted = "n1"
	compactedNode := c.nodes[compacted]
	var others []string
	for _, id := range c.ids {
		if id != compacted {
			others = append(others, id)
		}
	}

	for round := 0; round < 4; round++ {
		for i := 0; i < 5; i++ {
			c.submitToLeader(2*time.Second, c.ids, setCmd(fmt.Sprintf("r%d-k%d", round, i), "v"))
		}
		// Only wait for commit-index convergence here, not the shared
		// waitLogsConverged helper: that helper also compares LogCopy() across
		// every id, but the compacted node's LogCopy() is deliberately shorter
		// once it has been compacted (checked separately below).
		want := uint64((round + 1) * 5)
		c.waitCommitConverged(2*time.Second, c.ids, want)

		st := compactedNode.Status()
		if st.LastApplied > 0 {
			if err := compactedNode.CompactLog([]byte(fmt.Sprintf("snap-round-%d", round)), st.LastApplied); err != nil {
				t.Fatalf("round %d: CompactLog: %v", round, err)
			}
		}

		// The other four nodes were never compacted; their COMMITTED prefix (the
		// only part the Log Matching Property actually guarantees is identical
		// at any given instant — the uncommitted tail can transiently differ by a
		// heartbeat or two in the background) must agree with each other
		// bit-for-bit. The compacted node's LogCopy is shorter now, so it is
		// deliberately excluded from this comparison.
		var ref string
		for i, id := range others {
			lg := committedPrefix(c.nodes[id].LogCopy(), want)
			b := fmt.Sprintf("%+v", lg)
			if i == 0 {
				ref = b
			} else if b != ref {
				t.Fatalf("round %d: node %s committed log diverged from %s", round, id, others[0])
			}
		}
	}

	// Every node, including the compacted one, must agree on commit progress.
	c.waitCommitConverged(2*time.Second, c.ids, compactedNode.Status().CommitIndex)
}

// TestSnapshot_ConcurrentCompactionRaceFree hammers Submit from multiple
// goroutines while repeatedly compacting the leader's log, all under the race
// detector: the point is that concurrent proposals and compaction never
// corrupt shared state, and the cluster still converges once the traffic stops.
func TestSnapshot_ConcurrentCompactionRaceFree(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	c.waitLeader(2*time.Second, c.ids)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers: hammer Submit via whichever node currently answers as leader.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, id := range c.ids {
					if idx, _, ok := c.nodes[id].Submit(setCmd(fmt.Sprintf("w%d-%d", w, i), "v")); ok {
						_ = idx
						break
					}
				}
				i++
			}
		}(w)
	}

	// Compactor: periodically compact whichever node currently is leader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(15 * time.Millisecond):
			}
			for _, id := range c.ids {
				n := c.nodes[id]
				if !n.IsLeader() {
					continue
				}
				st := n.Status()
				if st.LastApplied > 0 {
					_ = n.CompactLog([]byte("snap"), st.LastApplied)
				}
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// After the storm, the cluster must still converge to a single commit index.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		vals := map[uint64]bool{}
		for _, id := range c.ids {
			vals[c.nodes[id].Status().CommitIndex] = true
		}
		if len(vals) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge on a single commitIndex after concurrent load: %s", c.dump(c.ids))
}
