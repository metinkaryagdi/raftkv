// Package orchestrator abstracts the "make real infrastructure changes"
// half of the lab: killing a node in the dashboard must actually kill a real
// container or pod, not a simulated one. It mirrors internal/transport's
// one-interface-two-implementations pattern (inmem/grpcx there,
// compose/k8s here) so the lab's control-plane server and frontend are
// written once against the interface and work against either target via a
// single flag.
package orchestrator

import "io"

// NodeRef is what the orchestrator knows about one cluster member.
type NodeRef struct {
	ID    string // e.g. "n1" (compose) or "raftkv-0" (k8s)
	Addr  string // client-facing HTTP address reachable from wherever the lab runs
	Ready bool
}

// Orchestrator is implemented once per deployment target (Docker Compose,
// Kubernetes). Every method operates on real infrastructure; there is no
// simulated/dry-run mode. Implementations must be safe for concurrent use.
type Orchestrator interface {
	// ListNodes returns every node currently known to the deployment.
	ListNodes() ([]NodeRef, error)

	// KillNode forcibly terminates the node's process (docker kill / kubectl
	// delete pod --grace-period=0 --force) — a sudden-crash fault, the same
	// scenario the existing failover demo scripts exercise.
	KillNode(id string) error

	// IsolateNode cuts the node off from the rest of the network without
	// killing its process (docker network disconnect / a scoped deny-all
	// NetworkPolicy) — a partition fault, distinct from a crash.
	IsolateNode(id string) error

	// HealNode reverses IsolateNode.
	HealNode(id string) error

	// AddNode brings up a new node with the given id and admits it into the
	// running Raft cluster via a dynamic membership change (wraps
	// scripts/compose-add-node.sh or scripts/k8s-add-node.sh, the single
	// source of truth for the orchestration steps involved).
	AddNode(id string) error

	// RemoveNode proposes removing id from the cluster's configuration and
	// then tears down its container/pod, in that order (wraps
	// scripts/compose-remove-node.sh or scripts/k8s-remove-node.sh).
	RemoveNode(id string) error

	// Logs returns a stream of the node's stdout/stderr (docker logs -f /
	// kubectl logs -f) for the lab to tail and parse (see internal/logfmt).
	// The caller must Close the returned reader when done.
	Logs(id string) (io.ReadCloser, error)
}
