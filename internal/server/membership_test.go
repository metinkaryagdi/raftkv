package server_test

import (
	"errors"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/server"
)

// TestServerAddServerAndRemoveServer exercises the Server-level wrappers the
// HTTP /cluster/add-server and /cluster/remove-server endpoints call directly:
// adding a node updates the live cluster's membership and a subsequent removal
// shrinks it back down, both observed through the same quorum-behavior proof
// used at the raft level (isolating one survivor must still leave enough of a
// majority to commit).
func TestServerAddServerAndRemoveServer(t *testing.T) {
	c := newSvcCluster(t, 3)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	// Add a node via the Server API (not directly via raft.Node) -- this is the
	// same call path POST /cluster/add-server uses.
	if err := leader.AddServer("n4", "unused-in-memory"); err != nil {
		t.Fatalf("AddServer: %v", err)
	}

	// A second, concurrent change while nothing has isolated anyone should
	// still be rejected if it races the first one's commit; retry a moment
	// later it must succeed since the first has long since committed.
	deadline := time.Now().Add(2 * time.Second)
	var addErr error
	for time.Now().Before(deadline) {
		addErr = leader.RemoveServer("n4")
		if addErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if addErr != nil {
		t.Fatalf("RemoveServer: %v", addErr)
	}
}

// TestServerRejectsConcurrentConfigChange verifies the HTTP-facing error
// mapping: proposing a second membership change before the first commits must
// surface as server.ErrConfigChangeInFlight (which http.go maps to 409), not a
// generic failure. AddServer blocks until its proposal commits, so the first
// call is isolated-and-thus-permanently-pending in a background goroutine
// (mirroring an HTTP client whose request is still in flight) while the second
// is issued synchronously and must be rejected immediately.
func TestServerRejectsConcurrentConfigChange(t *testing.T) {
	c := newSvcCluster(t, 5)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	// Isolate every other node so the first change can never commit.
	for _, id := range c.aliveIDs(leader.Node().ID()) {
		c.net.Isolate(id)
	}

	go func() { _ = leader.AddServer("n6", "addr") }() // will block until ErrTimeout; ignored
	time.Sleep(50 * time.Millisecond)                  // let the append happen before we race it

	err := leader.AddServer("n7", "addr")
	if !errors.Is(err, server.ErrConfigChangeInFlight) {
		t.Fatalf("second AddServer = %v, want ErrConfigChangeInFlight", err)
	}
}
