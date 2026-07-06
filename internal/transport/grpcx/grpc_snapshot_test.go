package grpcx_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/transport/grpcx"
)

// waitLeaderAmong is like grpcCluster.waitLeader but restricted to a subset of
// ids — needed here because the laggard node isn't started yet and must be
// excluded from the leader search.
func (c *grpcCluster) waitLeaderAmong(within time.Duration, ids []string) *raft.Node {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leader *raft.Node
		count := 0
		for _, id := range ids {
			if c.nodes[id].IsLeader() {
				leader = c.nodes[id]
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(15 * time.Millisecond)
	}
	c.t.Fatal("no leader elected over gRPC in time")
	return nil
}

// commitConvergedAmong reports whether every node in ids has a commitIndex of
// at least want.
func (c *grpcCluster) commitConvergedAmong(ids []string, want uint64) bool {
	for _, id := range ids {
		if c.nodes[id].CommitIndex() < want {
			return false
		}
	}
	return true
}

// TestGRPCInstallSnapshotCatchUp proves InstallSnapshot works over the real
// gRPC/TCP transport, not just in-memory: a node that never received the early
// log entries (its gRPC server and raft.Node are only started after the leader
// has already compacted past what that node needs) must catch up purely via a
// real InstallSnapshot RPC — dialed, serialized, and applied across a real
// socket — since normal AppendEntries could no longer help it.
func TestGRPCInstallSnapshotCatchUp(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping gRPC e2e test in -short mode")
	}
	c := newGRPCCluster(t, 5)
	defer c.stopAll()

	laggard := c.ids[len(c.ids)-1] // n5: started late, on purpose
	var early []string
	for _, id := range c.ids {
		if id != laggard {
			early = append(early, id)
		}
	}

	// Start only the first 4 nodes. The 5th's gRPC server and raft.Node stay
	// down, so it neither serves nor initiates any RPCs yet.
	for _, id := range early {
		go func(s *grpcx.Server) { _ = s.Serve() }(c.servers[id])
		c.nodes[id].Start()
	}

	leader := c.waitLeaderAmong(3*time.Second, early)

	const n = 10
	for i := 0; i < n; i++ {
		cmd := raft.Command{Op: "set", Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)}
		if _, _, ok := leader.Submit(cmd); !ok {
			t.Fatalf("submit %d: leader lost leadership", i)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !c.commitConvergedAmong(early, n) {
		time.Sleep(20 * time.Millisecond)
	}
	if !c.commitConvergedAmong(early, n) {
		t.Fatalf("early nodes did not converge to commit %d", n)
	}

	st := leader.Status()
	if err := leader.CompactLog([]byte("grpc-snapshot"), st.LastApplied); err != nil {
		t.Fatalf("CompactLog: %v", err)
	}
	if leader.Status().LastIncludedIndex == 0 {
		t.Fatalf("leader should have a nonzero LastIncludedIndex after compaction")
	}

	// Now bring the laggard online. Its only path to catching up is a real
	// InstallSnapshot RPC, since the leader no longer retains the early entries.
	go func(s *grpcx.Server) { _ = s.Serve() }(c.servers[laggard])
	c.nodes[laggard].Start()

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		lst := c.nodes[laggard].Status()
		if lst.CommitIndex >= n && lst.LastIncludedIndex > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	lst := c.nodes[laggard].Status()
	t.Fatalf("laggard did not catch up via InstallSnapshot over gRPC: %+v", lst)
}
