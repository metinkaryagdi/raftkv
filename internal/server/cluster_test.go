package server_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/server"
	"github.com/metinkaryagdi/raftkv/internal/transport/inmem"
)

// svcCluster is N key-value servers wired over one in-memory network — the same
// fault-injectable network the raft tests use, so server-level tests can also
// exercise leader changes and partitions.
type svcCluster struct {
	t       *testing.T
	net     *inmem.Network
	servers map[string]*server.Server
	ids     []string
}

func newSvcCluster(t *testing.T, n int) *svcCluster {
	t.Helper()
	net := inmem.NewNetwork()
	c := &svcCluster{t: t, net: net, servers: make(map[string]*server.Server)}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i+1))
	}
	for _, id := range c.ids {
		var peers []string
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          net.Endpoint(id),
			ElectionTimeoutMin: 80 * time.Millisecond,
			ElectionTimeoutMax: 160 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
		})
		net.Register(id, node)
		c.servers[id] = server.New(node)
	}
	return c
}

func (c *svcCluster) setCommitTimeout(d time.Duration) {
	for _, id := range c.ids {
		c.servers[id].SetCommitTimeout(d)
	}
}

// aliveIDs returns all node ids except the excluded ones.
func (c *svcCluster) aliveIDs(exclude ...string) []string {
	skip := map[string]bool{}
	for _, id := range exclude {
		skip[id] = true
	}
	var out []string
	for _, id := range c.ids {
		if !skip[id] {
			out = append(out, id)
		}
	}
	return out
}

// leaderAmong waits for a single leader within the given id set.
func (c *svcCluster) leaderAmong(within time.Duration, ids []string) *server.Server {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leader *server.Server
		count := 0
		for _, id := range ids {
			if c.servers[id].Node().IsLeader() {
				leader = c.servers[id]
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader among %v in time", ids)
	return nil
}

// setToLeader writes k=v via whichever node in ids is currently leader, retrying
// across leadership changes until committed or the deadline passes.
func (c *svcCluster) setToLeader(within time.Duration, ids []string, k, v string) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, id := range ids {
			err := c.servers[id].Set(k, v)
			if err == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("could not set %s=%s via any leader in %v within %v", k, v, ids, within)
}

// waitConvergedAmong is waitConverged restricted to a subset of nodes.
func (c *svcCluster) waitConvergedAmong(within time.Duration, ids []string, want map[string]string) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			snap := c.servers[id].Store().Snapshot()
			if len(snap) != len(want) {
				ok = false
				break
			}
			for k, v := range want {
				if snap[k] != v {
					ok = false
					break
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("nodes %v did not converge to %v in time", ids, want)
}

func (c *svcCluster) startAll() {
	for _, id := range c.ids {
		c.servers[id].Start()
		c.servers[id].Node().Start()
	}
}

func (c *svcCluster) stopAll() {
	for _, id := range c.ids {
		c.servers[id].Stop()
		c.servers[id].Node().Stop()
	}
}

func (c *svcCluster) waitLeader(within time.Duration) *server.Server {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var leader *server.Server
		count := 0
		for _, id := range c.ids {
			if c.servers[id].Node().IsLeader() {
				leader = c.servers[id]
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected in time")
	return nil
}

func (c *svcCluster) followers(leader *server.Server) []*server.Server {
	var out []*server.Server
	for _, id := range c.ids {
		if c.servers[id] != leader {
			out = append(out, c.servers[id])
		}
	}
	return out
}

// waitConverged blocks until every server's state machine holds want.
func (c *svcCluster) waitConverged(within time.Duration, want map[string]string) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c.allEqual(want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("state machines did not converge to %v in time", want)
}

func (c *svcCluster) allEqual(want map[string]string) bool {
	for _, id := range c.ids {
		snap := c.servers[id].Store().Snapshot()
		if len(snap) != len(want) {
			return false
		}
		for k, v := range want {
			if snap[k] != v {
				return false
			}
		}
	}
	return true
}
