package raft_test

import (
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// TestElectSingleLeader verifies the core liveness+safety property of leader
// election: from a cold start, a 5-node cluster converges on exactly one leader,
// and every follower recognizes that leader in the same term.
func TestElectSingleLeader(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()

	leader, term := c.waitLeader(2*time.Second, c.ids)

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("expected exactly 1 leader, got %d: %s", got, c.dump(c.ids))
	}
	for _, id := range c.ids {
		if id == leader {
			continue
		}
		st := c.nodes[id].Status()
		if st.Role != raft.Follower {
			t.Errorf("node %s expected Follower, got %s", id, st.Role)
		}
		if st.Term != term {
			t.Errorf("node %s term=%d, want %d", id, st.Term, term)
		}
		if st.LeaderID != leader {
			t.Errorf("node %s leaderID=%q, want %q", id, st.LeaderID, leader)
		}
	}
}

// TestReElectionAfterLeaderFailure verifies that when the leader crashes, the
// remaining nodes elect a new leader in a higher term and the cluster keeps
// making progress.
func TestReElectionAfterLeaderFailure(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()

	oldLeader, oldTerm := c.waitLeader(2*time.Second, c.ids)

	// Crash the leader: isolate it from the network so its heartbeats stop
	// landing and it can neither win nor influence future elections.
	c.net.Isolate(oldLeader)

	survivors := c.aliveIDs(oldLeader)
	newLeader, newTerm := c.waitLeader(2*time.Second, survivors)

	if newLeader == oldLeader {
		t.Fatalf("new leader %s should differ from crashed leader %s", newLeader, oldLeader)
	}
	if newTerm <= oldTerm {
		t.Fatalf("new term %d should exceed old term %d", newTerm, oldTerm)
	}

	// Exactly one leader among the survivors.
	var leaderCount int
	for _, id := range survivors {
		if c.nodes[id].IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader among survivors, got %d: %s", leaderCount, c.dump(survivors))
	}
}

// TestRejoinedLeaderStepsDown verifies that an old leader that was partitioned
// away rejoins as a follower once it observes the newer term, rather than
// causing a split brain.
func TestRejoinedLeaderStepsDown(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()

	oldLeader, oldTerm := c.waitLeader(2*time.Second, c.ids)

	c.net.Isolate(oldLeader)
	survivors := c.aliveIDs(oldLeader)
	_, newTerm := c.waitLeader(2*time.Second, survivors)
	if newTerm <= oldTerm {
		t.Fatalf("expected higher term after re-election")
	}

	// Reconnect the old leader; it should discover the newer term and step down.
	c.net.Heal(oldLeader)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := c.nodes[oldLeader].Status()
		if st.Role == raft.Follower && st.Term >= newTerm {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := c.nodes[oldLeader].Status()
	if st.Role != raft.Follower {
		t.Fatalf("rejoined old leader should be Follower, got %s (term %d)", st.Role, st.Term)
	}
	// And the cluster still has exactly one leader overall.
	if got := len(c.leaders()); got != 1 {
		t.Fatalf("expected exactly 1 leader after heal, got %d: %s", got, c.dump(c.ids))
	}
}

// TestLeaderStabilityNoSpuriousElections verifies that a healthy leader's
// heartbeats suppress follower election timers: over a multi-timeout window the
// term must not keep climbing.
func TestLeaderStabilityNoSpuriousElections(t *testing.T) {
	c := newTestCluster(t, 5)
	c.startAll()
	defer c.stopAll()

	_, term := c.waitLeader(2*time.Second, c.ids)

	// Observe for ~10 election timeouts. A stable leader means at most a tiny
	// amount of term churn (ideally none).
	time.Sleep(800 * time.Millisecond)

	_, finalTerm, ok := c.checkOneLeader(c.ids)
	if !ok {
		t.Fatalf("expected a single stable leader: %s", c.dump(c.ids))
	}
	if finalTerm != term {
		t.Errorf("term advanced from %d to %d while leader was healthy (spurious elections)", term, finalTerm)
	}
}
