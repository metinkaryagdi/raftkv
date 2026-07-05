package server_test

import (
	"errors"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/server"
)

// TestSetGetThroughLeader is the end-to-end happy path: a write accepted by the
// leader is committed, applied, and immediately readable from the leader.
func TestSetGetThroughLeader(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	if err := leader.Set("city", "istanbul"); err != nil {
		t.Fatalf("Set on leader failed: %v", err)
	}
	v, ok, err := leader.Get("city")
	if err != nil || !ok || v != "istanbul" {
		t.Fatalf("Get(city)=%q,%v,%v; want istanbul,true,nil", v, ok, err)
	}
}

// TestWritesRejectedOnFollower verifies that followers refuse writes and reads
// and point the client at the leader, rather than serving stale data.
func TestWritesRejectedOnFollower(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	for _, f := range c.followers(leader) {
		if err := f.Set("k", "v"); !errors.Is(err, server.ErrNotLeader) {
			t.Fatalf("follower Set: got %v, want ErrNotLeader", err)
		}
		if _, _, err := f.Get("k"); !errors.Is(err, server.ErrNotLeader) {
			t.Fatalf("follower Get: got %v, want ErrNotLeader", err)
		}
		if hint := f.LeaderHint(); hint == "" {
			t.Errorf("follower should report a leader hint")
		}
	}
}

// TestStateMachineConvergence writes a batch through the leader and asserts every
// replica's key-value store ends up identical — the whole point of the exercise.
func TestStateMachineConvergence(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	want := map[string]string{}
	for _, kv := range [][2]string{
		{"lang", "go"}, {"algo", "raft"}, {"nodes", "5"}, {"tmp", "x"},
	} {
		if err := leader.Set(kv[0], kv[1]); err != nil {
			t.Fatalf("Set(%s): %v", kv[0], err)
		}
		want[kv[0]] = kv[1]
	}
	if err := leader.Delete("tmp"); err != nil {
		t.Fatalf("Delete(tmp): %v", err)
	}
	delete(want, "tmp")

	c.waitConverged(2*time.Second, want)
}

// TestWriteSurvivesLeaderChange writes, kills the leader, and confirms the data
// is still readable from the newly elected leader — committed writes are durable
// across failover.
func TestWriteSurvivesLeaderChange(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	if err := leader.Set("persisted", "yes"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	leaderID := leader.Node().ID()
	c.net.Isolate(leaderID)

	// A new leader emerges among the survivors; the committed key is still there.
	var newLeader *server.Server
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			if id != leaderID && c.servers[id].Node().IsLeader() {
				newLeader = c.servers[id]
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader after failover")
	}
	// A freshly elected leader must commit its term's no-op before carried-over
	// entries become applicable, so a client retries the read briefly (as any
	// real client would) until the value surfaces.
	readDeadline := time.Now().Add(2 * time.Second)
	for {
		v, ok, err := newLeader.Get("persisted")
		if err == nil && ok && v == "yes" {
			break
		}
		if time.Now().After(readDeadline) {
			t.Fatalf("after failover Get(persisted)=%q,%v,%v; want yes,true,nil", v, ok, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
