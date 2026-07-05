// Command raftkv runs a single node of the replicated key-value store. Launch
// five of them (see scripts/run-cluster.ps1) pointing at each other to form a
// cluster: Raft RPCs flow node-to-node over gRPC, while clients talk to any node
// over the HTTP API (writes are redirected to the leader).
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/server"
	"github.com/metinkaryagdi/raftkv/internal/transport/grpcx"
)

func main() {
	var (
		id       = flag.String("id", "", "this node's id (e.g. n1)")
		raftAddr = flag.String("raft-addr", "", "host:port for inter-node gRPC (defaults to this node's entry in -peers)")
		httpAddr = flag.String("http-addr", "127.0.0.1:8001", "host:port for the client HTTP API")
		peersStr = flag.String("peers", "", "comma-separated id=host:port list for ALL nodes, including this one")
	)
	flag.Parse()

	if *id == "" || *peersStr == "" {
		log.Fatal("both -id and -peers are required")
	}
	peers, err := parsePeers(*peersStr)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}
	selfAddr := *raftAddr
	if selfAddr == "" {
		selfAddr = peers[*id]
	}
	if selfAddr == "" {
		log.Fatalf("node %q not found in -peers and no -raft-addr given", *id)
	}

	// raft peers = every node except ourselves.
	var raftPeers []string
	for pid := range peers {
		if pid != *id {
			raftPeers = append(raftPeers, pid)
		}
	}

	transport := grpcx.NewTransport(*id, peers)
	node := raft.NewNode(raft.Config{
		ID:                 *id,
		Peers:              raftPeers,
		Transport:          transport,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		Logger:             stderrLogger{},
	})

	grpcServer, err := grpcx.NewServer(node, selfAddr)
	if err != nil {
		log.Fatalf("start gRPC server: %v", err)
	}
	go func() {
		if err := grpcServer.Serve(); err != nil {
			log.Fatalf("gRPC serve: %v", err)
		}
	}()

	kv := server.New(node)
	kv.Start()
	node.Start()

	log.Printf("node %s: raft(gRPC)=%s http=%s peers=%v", *id, selfAddr, *httpAddr, raftPeers)
	if err := http.ListenAndServe(*httpAddr, kv.HTTPHandler()); err != nil {
		log.Fatalf("http serve: %v", err)
	}
}

func parsePeers(s string) (map[string]string, error) {
	out := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errBadPeer(part)
		}
		out[strings.TrimSpace(id)] = strings.TrimSpace(addr)
	}
	return out, nil
}

type errBadPeer string

func (e errBadPeer) Error() string { return "expected id=host:port, got " + string(e) }

// stderrLogger prints Raft state transitions, one line each, to stderr.
type stderrLogger struct{}

func (stderrLogger) Event(nodeID string, e raft.Event) {
	line := "[" + nodeID + "] " + e.Kind + " term=" + itoa(e.Term) + " role=" + e.Role.String()
	if e.Peer != "" {
		line += " peer=" + e.Peer
	}
	if e.Info != "" {
		line += " (" + e.Info + ")"
	}
	log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds).Println(line)
}

func itoa(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
