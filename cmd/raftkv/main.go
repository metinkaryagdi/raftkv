// Command raftkv runs a single node of the replicated key-value store. Launch
// five of them (see scripts/run-cluster.ps1) pointing at each other to form a
// cluster: Raft RPCs flow node-to-node over gRPC, while clients talk to any node
// over the HTTP API (writes are redirected to the leader).
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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
		httpAddr = flag.String("http-addr", "", "host:port for the client HTTP API (default 127.0.0.1:8001)")
		peersStr = flag.String("peers", "", "comma-separated id=host:port list for ALL nodes, including this one")
	)
	flag.Parse()

	// Flags win; otherwise fall back to environment variables so the same image
	// can be configured declaratively (e.g. from docker-compose).
	*id = orEnv(*id, "RAFTKV_ID")
	*raftAddr = orEnv(*raftAddr, "RAFTKV_RAFT_ADDR")
	*httpAddr = orEnv(*httpAddr, "RAFTKV_HTTP_ADDR")
	*peersStr = orEnv(*peersStr, "RAFTKV_PEERS")
	if *httpAddr == "" {
		*httpAddr = "127.0.0.1:8001"
	}
	// Kubernetes StatefulSet mode: when no explicit peer list is given but a
	// replica count is, derive the peer list from stable pod DNS names so every
	// pod can share one identical spec (its id comes from the pod hostname).
	if *peersStr == "" {
		if generated := k8sPeersFromEnv(*id); generated != "" {
			*peersStr = generated
		}
	}

	if *id == "" || *peersStr == "" {
		log.Fatal("id and peers are required (flags -id/-peers or env RAFTKV_ID/RAFTKV_PEERS)")
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
		ID:        *id,
		Peers:     raftPeers,
		Transport: transport,
		// Timeouts are generous relative to the in-memory tests: real gRPC
		// connections are dialed lazily on first use, so the election timeout must
		// leave room for a cold connection to establish before a node gives up and
		// starts a competing election (otherwise terms inflate at startup).
		ElectionTimeoutMin: 600 * time.Millisecond,
		ElectionTimeoutMax: 1200 * time.Millisecond,
		HeartbeatInterval:  120 * time.Millisecond,
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

	transport.Warmup() // start dialing peers before the first election
	kv := server.New(node)
	kv.Start()
	node.Start()

	log.Printf("node %s: raft(gRPC)=%s http=%s peers=%v", *id, selfAddr, *httpAddr, raftPeers)
	if err := http.ListenAndServe(*httpAddr, kv.HTTPHandler()); err != nil {
		log.Fatalf("http serve: %v", err)
	}
}

// orEnv returns val if non-empty, otherwise the named environment variable.
func orEnv(val, envKey string) string {
	if val != "" {
		return val
	}
	return os.Getenv(envKey)
}

// k8sPeersFromEnv builds the peer list for a StatefulSet deployment from the
// replica count and the headless service name, using the stable pod DNS that a
// StatefulSet guarantees: pod <name>-<ordinal> is reachable at
// <name>-<ordinal>.<service>:<port>. It returns "" if RAFTKV_REPLICAS is unset.
//
// Env:
//
//	RAFTKV_REPLICAS   number of pods (e.g. "5")
//	RAFTKV_SERVICE    headless service name (default: pod basename)
//	RAFTKV_PEER_PORT  gRPC port (default: "9001")
func k8sPeersFromEnv(id string) string {
	replicasStr := os.Getenv("RAFTKV_REPLICAS")
	if replicasStr == "" {
		return ""
	}
	replicas, err := strconv.Atoi(replicasStr)
	if err != nil || replicas < 1 {
		log.Fatalf("invalid RAFTKV_REPLICAS %q", replicasStr)
	}
	// Pod names are "<basename>-<ordinal>"; strip the ordinal to get the basename.
	basename := id
	if i := strings.LastIndex(id, "-"); i > 0 {
		basename = id[:i]
	}
	service := os.Getenv("RAFTKV_SERVICE")
	if service == "" {
		service = basename
	}
	port := os.Getenv("RAFTKV_PEER_PORT")
	if port == "" {
		port = "9001"
	}
	parts := make([]string, 0, replicas)
	for i := 0; i < replicas; i++ {
		pod := fmt.Sprintf("%s-%d", basename, i)
		parts = append(parts, fmt.Sprintf("%s=%s.%s:%s", pod, pod, service, port))
	}
	return strings.Join(parts, ",")
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
