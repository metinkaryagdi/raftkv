package raft

// Transport is the outbound side of a node's networking: it lets a node send RPCs
// to its peers. It is an interface so the core algorithm is fully decoupled from
// the wire protocol. The project ships two implementations:
//
//   - an in-memory transport (internal/transport/inmem) used by the deterministic,
//     race-tested unit tests and for simulating latency and network partitions;
//   - a gRPC transport (internal/transport/grpcx) used to run real multi-process
//     clusters.
//
// Implementations must be safe for concurrent use: a node fans out RPCs to all
// peers from separate goroutines.
type Transport interface {
	// SendRequestVote sends a RequestVote RPC to the peer identified by target.
	// It returns an error if the peer is unreachable (e.g. crashed or partitioned).
	SendRequestVote(target string, args *RequestVoteArgs) (*RequestVoteReply, error)

	// SendAppendEntries sends an AppendEntries RPC to the peer identified by target.
	SendAppendEntries(target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)

	// SendInstallSnapshot sends an InstallSnapshot RPC to the peer identified by
	// target, used when the peer needs log entries the leader has already
	// compacted away.
	SendInstallSnapshot(target string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler is the inbound side: it is implemented by *Node so a transport can
// deliver incoming RPCs to it. Handlers must be safe for concurrent use.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}

// PeerManager is implemented by transports that need explicit address
// bookkeeping to add or remove peers at runtime (required for dynamic
// membership changes). inmem's transport needs no such bookkeeping — routing
// there is already keyed by node id via the shared Network — so it does not
// implement this; grpcx.Transport does, since it must know an address to dial.
// raft.Node type-asserts its transport against this interface and calls it only
// if the concrete transport implements it.
type PeerManager interface {
	AddPeer(id, addr string)
	RemovePeer(id string)
}
