package grpcx_test

import (
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/transport/grpcx"
)

// grpcCluster runs N raft nodes talking over real gRPC/TCP on loopback. Unlike
// the in-memory tests, this exercises the actual wire path: serialization,
// sockets, and connection handling.
type grpcCluster struct {
	t       *testing.T
	ids     []string
	nodes   map[string]*raft.Node
	servers map[string]*grpcx.Server
	trans   map[string]*grpcx.Transport
}

func newGRPCCluster(t *testing.T, n int) *grpcCluster {
	t.Helper()
	c := &grpcCluster{
		t:       t,
		nodes:   make(map[string]*raft.Node),
		servers: make(map[string]*grpcx.Server),
		trans:   make(map[string]*grpcx.Transport),
	}

	// Reserve a loopback port per node first so every peer address is known
	// before we build the nodes.
	listeners := make(map[string]net.Listener)
	peers := make(map[string]string)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i+1)
		c.ids = append(c.ids, id)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[id] = lis
		peers[id] = lis.Addr().String()
	}

	for _, id := range c.ids {
		var raftPeers []string
		for _, other := range c.ids {
			if other != id {
				raftPeers = append(raftPeers, other)
			}
		}
		transport := grpcx.NewTransport(id, peers)
		node := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              raftPeers,
			Transport:          transport,
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  40 * time.Millisecond,
		})
		srv := grpcx.NewServerListener(node, listeners[id])
		c.nodes[id] = node
		c.servers[id] = srv
		c.trans[id] = transport
	}
	return c
}

func (c *grpcCluster) startAll() {
	for _, id := range c.ids {
		go func(s *grpcx.Server) { _ = s.Serve() }(c.servers[id])
		c.nodes[id].Start()
	}
}

func (c *grpcCluster) stopAll() {
	for _, id := range c.ids {
		c.nodes[id].Stop()
		c.servers[id].Stop()
		c.trans[id].Close()
	}
}

func (c *grpcCluster) waitLeader(within time.Duration) *raft.Node {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leader *raft.Node
		count := 0
		for _, id := range c.ids {
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

// TestGRPCElectionAndReplication is the real-network smoke test: a 5-node gRPC
// cluster elects a leader, replicates commands submitted to it, and every node's
// log converges — proving the transport, not just the algorithm, works.
func TestGRPCElectionAndReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping gRPC e2e test in -short mode")
	}
	c := newGRPCCluster(t, 5)
	c.startAll()
	defer c.stopAll()

	leader := c.waitLeader(3 * time.Second)

	const n = 6
	for i := 0; i < n; i++ {
		cmd := raft.Command{Op: "set", Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)}
		if _, _, ok := leader.Submit(cmd); !ok {
			t.Fatalf("submit %d: leader lost leadership", i)
		}
	}

	// Wait for all logs to converge.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.logsConverged(n) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("gRPC cluster logs did not converge to %d entries", n)
}

func (c *grpcCluster) logsConverged(want uint64) bool {
	var ref []raft.LogEntry
	for i, id := range c.ids {
		if c.nodes[id].CommitIndex() < want {
			return false
		}
		lg := c.nodes[id].LogCopy()
		if i == 0 {
			ref = lg
		} else if !reflect.DeepEqual(ref, lg) {
			return false
		}
	}
	return true
}
