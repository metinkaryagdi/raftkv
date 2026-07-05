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
}

// RPCHandler is the inbound side: it is implemented by *Node so a transport can
// deliver incoming RPCs to it. Handlers must be safe for concurrent use.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}
