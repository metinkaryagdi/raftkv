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

	"github.com/metinkaryagdi/raftkv/internal/logfmt"
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
		join     = flag.Bool("join", false, "start as a node awaiting a dynamic membership change (see POST /cluster/add-server) rather than a genesis cluster member; suppresses election-starting and vote-granting until a real leader makes contact")
	)
	flag.Parse()

	// Flags win; otherwise fall back to environment variables so the same image
	// can be configured declaratively (e.g. from docker-compose).
	*id = orEnv(*id, "RAFTKV_ID")
	*raftAddr = orEnv(*raftAddr, "RAFTKV_RAFT_ADDR")
	*httpAddr = orEnv(*httpAddr, "RAFTKV_HTTP_ADDR")
	*peersStr = orEnv(*peersStr, "RAFTKV_PEERS")
	if !*join {
		*join = os.Getenv("RAFTKV_JOIN") != ""
	}
	if !*join {
		*join = k8sPastGenesis(*id)
	}
	if *httpAddr == "" {
		*httpAddr = "127.0.0.1:8001"
	}
	// Kubernetes StatefulSet mode: when no explicit peer list is given but a
	// replica count is, derive the peer list from stable pod DNS names so every
	// pod can share one identical spec (its id comes from the pod hostname). This
	// applies whether or not -join is set: a scaled-up pod still benefits from
	// knowing the full current membership up front rather than discovering peers
	// one at a time (see k8sPeersFromEnv's ordinal check).
	if *peersStr == "" {
		if generated := k8sPeersFromEnv(*id); generated != "" {
			*peersStr = generated
		}
	}

	if *id == "" {
		log.Fatal("-id is required (flag or RAFTKV_ID)")
	}
	if *peersStr == "" && !*join {
		log.Fatal("peers are required (flag -peers, env RAFTKV_PEERS, RAFTKV_REPLICAS-derived, or -join/RAFTKV_JOIN for a node awaiting a dynamic membership change)")
	}

	// peers may legitimately be empty here only when -join is set and no static
	// list nor RAFTKV_REPLICAS applies (e.g. a Docker Compose node started with
	// no prior knowledge at all, relying entirely on being told about the
	// cluster via a future AppendEntries once /cluster/add-server admits it).
	peers := map[string]string{}
	if *peersStr != "" {
		var err error
		peers, err = parsePeers(*peersStr)
		if err != nil {
			log.Fatalf("invalid -peers: %v", err)
		}
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
		Joining:   *join,
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
// <name>-<ordinal>.<service>:<port>. It returns "" if RAFTKV_REPLICAS is unset
// OR if this pod's own ordinal is >= RAFTKV_REPLICAS.
//
// The ordinal check matters for dynamic scale-up: RAFTKV_REPLICAS in the
// StatefulSet template is meant to be bumped by the operator (e.g. via
// `kubectl set env`) to the new total *before* scaling, so a scaled-up pod
// (ordinal >= the OLD count, < the NEW one) gets a full peer list covering
// every existing member automatically, with no separate discovery step. A pod
// whose own ordinal is still >= the (possibly-bumped) count falls through to
// -join/RAFTKV_JOIN instead — this only happens if an operator scales up
// without bumping RAFTKV_REPLICAS first, a documented prerequisite rather than
// something this function can detect and correct on its own.
//
// Env:
//
//	RAFTKV_REPLICAS   number of pods this deployment currently declares (e.g. "5")
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
	// Pod names are "<basename>-<ordinal>"; split them apart.
	basename := id
	ordinal := -1
	if i := strings.LastIndex(id, "-"); i > 0 {
		basename = id[:i]
		if n, err := strconv.Atoi(id[i+1:]); err == nil {
			ordinal = n
		}
	}
	if ordinal < 0 || ordinal >= replicas {
		return ""
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

// k8sPastGenesis reports whether this pod's ordinal is at or past
// RAFTKV_GENESIS_SIZE — the cluster's original size, frozen forever at
// authoring time and never bumped (unlike RAFTKV_REPLICAS, which k8s-add-
// node.sh deliberately bumps before each scale-up so a new pod's
// k8sPeersFromEnv already covers every existing member). This is a separate
// env var precisely because the two must diverge after the first scale-up: a
// scaled-up pod needs a *full* peer list (from the bumped RAFTKV_REPLICAS) to
// function well once admitted, but it must still start in Joining mode (to
// avoid disrupting the real cluster with a premature election, per the
// paper's "disruptive server" concern) even though it isn't missing any peer
// addresses. Returns false if RAFTKV_GENESIS_SIZE is unset (e.g. Docker
// Compose, or plain local runs) — those deployments rely solely on the
// explicit -join/RAFTKV_JOIN flag instead.
func k8sPastGenesis(id string) bool {
	genesisStr := os.Getenv("RAFTKV_GENESIS_SIZE")
	if genesisStr == "" {
		return false
	}
	genesisSize, err := strconv.Atoi(genesisStr)
	if err != nil || genesisSize < 1 {
		log.Fatalf("invalid RAFTKV_GENESIS_SIZE %q", genesisStr)
	}
	i := strings.LastIndex(id, "-")
	if i <= 0 {
		return false
	}
	ordinal, err := strconv.Atoi(id[i+1:])
	if err != nil {
		return false
	}
	return ordinal >= genesisSize
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

// stderrLogger prints Raft state transitions, one line each, to stderr, in the
// exact format internal/logfmt.Parse expects (the lab tails and parses this
// output — see that package's doc comment for why the format lives there and
// not here).
type stderrLogger struct{}

func (stderrLogger) Event(nodeID string, e raft.Event) {
	log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds).Println(logfmt.Format(nodeID, e))
}
