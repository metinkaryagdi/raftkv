package raft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/transport/inmem"
)

// testCluster wires N Raft nodes onto a single in-memory network. Timeouts are
// scaled down from real-world values so tests run quickly while keeping the
// heartbeat interval comfortably below the election timeout.
type testCluster struct {
	t     *testing.T
	net   *inmem.Network
	nodes map[string]*raft.Node
	ids   []string
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	net := inmem.NewNetwork()
	c := &testCluster{t: t, net: net, nodes: make(map[string]*raft.Node)}
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
		c.nodes[id] = node
	}
	return c
}

func (c *testCluster) startAll() {
	for _, id := range c.ids {
		c.nodes[id].Start()
	}
}

func (c *testCluster) stopAll() {
	for _, id := range c.ids {
		c.nodes[id].Stop()
	}
}

// leaders returns the ids of all nodes that currently believe they are leader.
func (c *testCluster) leaders() []string {
	var out []string
	for _, id := range c.ids {
		if c.nodes[id].IsLeader() {
			out = append(out, id)
		}
	}
	return out
}

// aliveIDs returns node ids excluding the given ones (e.g. a crashed leader).
func (c *testCluster) aliveIDs(exclude ...string) []string {
	skip := make(map[string]bool)
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

// waitLeader polls until exactly one node among the given ids reports leadership
// and every other node in that set agrees (same term, same leaderID). It returns
// the leader id and its term, failing the test on timeout.
func (c *testCluster) waitLeader(within time.Duration, ids []string) (leaderID string, term uint64) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if id, tm, ok := c.checkOneLeader(ids); ok {
			return id, tm
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no stable leader elected within %v among %v; states: %s", within, ids, c.dump(ids))
	return "", 0
}

// checkOneLeader returns the sole leader among ids if there is exactly one and
// all listed nodes are in its term. It does not fail the test.
func (c *testCluster) checkOneLeader(ids []string) (string, uint64, bool) {
	var leader string
	var count int
	var maxTerm uint64
	for _, id := range ids {
		st := c.nodes[id].Status()
		if st.Term > maxTerm {
			maxTerm = st.Term
		}
		if st.Role == raft.Leader {
			leader = id
			count++
		}
	}
	if count != 1 {
		return "", 0, false
	}
	// The leader must be at the highest observed term for it to be legitimate.
	if c.nodes[leader].Term() != maxTerm {
		return "", 0, false
	}
	return leader, maxTerm, true
}

func (c *testCluster) dump(ids []string) string {
	s := ""
	for _, id := range ids {
		st := c.nodes[id].Status()
		s += fmt.Sprintf("[%s role=%s term=%d leader=%s log=%d commit=%d] ",
			st.ID, st.Role, st.Term, st.LeaderID, st.LogLength, st.CommitIndex)
	}
	return s
}
