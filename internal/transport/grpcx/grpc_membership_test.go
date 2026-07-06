package grpcx_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/transport/grpcx"
)

// TestGRPCAddServerCatchesUpOverRealNetwork proves the PeerManager wiring works
// end to end over a real gRPC/TCP transport, not just in-memory: a brand-new
// node — with no address known to anyone until this test dials it — is added
// to a running 4-node gRPC cluster via ProposeConfigChange. The leader's
// grpcx.Transport.AddPeer must register its address and the existing
// broadcast/replication loop must then dial and catch it up with zero new
// replication code, exactly as the design intends.
func TestGRPCAddServerCatchesUpOverRealNetwork(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping gRPC e2e test in -short mode")
	}
	c := newGRPCCluster(t, 4)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(3 * time.Second)

	for i := 0; i < 5; i++ {
		cmd := raft.Command{Op: "set", Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)}
		if _, _, ok := leader.Submit(cmd); !ok {
			t.Fatalf("submit %d: leader lost leadership", i)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !c.commitConvergedAmong(c.ids, 5) {
		time.Sleep(20 * time.Millisecond)
	}
	if !c.commitConvergedAmong(c.ids, 5) {
		t.Fatalf("initial 4 nodes did not converge to commit 5")
	}

	// Construct the 5th node completely independently: its own listener, its
	// own empty-peers Joining transport, no prior knowledge in any existing
	// node's peer map.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	newID := "n5"
	newAddr := lis.Addr().String()
	newTransport := grpcx.NewTransport(newID, map[string]string{})
	newNode := raft.NewNode(raft.Config{
		ID:                 newID,
		Peers:              nil,
		Joining:            true,
		Transport:          newTransport,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  40 * time.Millisecond,
	})
	newServer := grpcx.NewServerListener(newNode, lis)
	go func() { _ = newServer.Serve() }()
	newNode.Start()
	defer func() {
		newNode.Stop()
		newServer.Stop()
		newTransport.Close()
	}()

	if _, _, err := leader.ProposeConfigChange(raft.Command{
		Op: "conf_change", ConfigOp: "add", Key: newID, Value: newAddr,
	}); err != nil {
		t.Fatalf("ProposeConfigChange: %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if newNode.Status().CommitIndex >= 5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("new node did not catch up over real gRPC: %+v", newNode.Status())
}
