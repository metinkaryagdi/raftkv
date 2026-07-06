// Package grpcx is the real-network implementation of raft.Transport, used to run
// a cluster as separate OS processes talking over gRPC (mirroring how systems
// like etcd wire their nodes together). It is a thin adapter: it converts between
// the package-internal raft types and the generated protobuf messages and
// delegates all logic to *raft.Node via the raft.RPCHandler interface.
package grpcx

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/metinkaryagdi/raftkv/internal/raft"
	"github.com/metinkaryagdi/raftkv/internal/transport/grpcx/raftpb"
)

// callTimeout bounds each outbound RPC so a dead peer fails fast instead of
// stalling an election or a replication round.
const callTimeout = 300 * time.Millisecond

// snapshotCallTimeout is longer than callTimeout: InstallSnapshot carries the
// full serialized state machine, which can take meaningfully longer to
// transfer than the small, fixed-size RequestVote/AppendEntries messages.
const snapshotCallTimeout = 5 * time.Second

// --- Server: adapts an inbound gRPC call to a raft.RPCHandler ---

// Server exposes a node's RPCHandler over gRPC on a TCP listener.
type Server struct {
	raftpb.UnimplementedRaftServer
	handler raft.RPCHandler
	grpc    *grpc.Server
	lis     net.Listener
}

// NewServer binds a gRPC server to listenAddr (host:port) delivering RPCs to h.
func NewServer(h raft.RPCHandler, listenAddr string) (*Server, error) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("grpcx: listen %s: %w", listenAddr, err)
	}
	return NewServerListener(h, lis), nil
}

// NewServerListener is like NewServer but serves on an already-open listener.
// Tests use it to reserve ports up front (listen on :0) so peer addresses are
// known before any node is constructed.
func NewServerListener(h raft.RPCHandler, lis net.Listener) *Server {
	s := &Server{handler: h, grpc: grpc.NewServer(), lis: lis}
	raftpb.RegisterRaftServer(s.grpc, s)
	return s
}

// Addr returns the actual listen address (useful when listenAddr used port 0).
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Serve blocks serving RPCs until Stop is called.
func (s *Server) Serve() error { return s.grpc.Serve(s.lis) }

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() { s.grpc.GracefulStop() }

func (s *Server) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteReply, error) {
	reply := s.handler.HandleRequestVote(&raft.RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  req.CandidateId,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	return &raftpb.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}, nil
}

func (s *Server) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesReply, error) {
	reply := s.handler.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     req.LeaderId,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      fromPBEntries(req.Entries),
		LeaderCommit: req.LeaderCommit,
	})
	return &raftpb.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func (s *Server) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotReply, error) {
	reply := s.handler.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              req.Term,
		LeaderID:          req.LeaderId,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	return &raftpb.InstallSnapshotReply{Term: reply.Term}, nil
}

// --- Transport: adapts raft.Transport to outbound gRPC calls ---

// Transport dials peers lazily and reuses connections. It implements
// raft.Transport and is safe for concurrent use.
type Transport struct {
	self  string
	peers map[string]string // peer id -> host:port

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewTransport creates a transport for the node self, given a peer id->address map.
func NewTransport(self string, peers map[string]string) *Transport {
	return &Transport{
		self:  self,
		peers: peers,
		conns: make(map[string]*grpc.ClientConn),
	}
}

func (t *Transport) client(target string) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[target]; ok {
		return raftpb.NewRaftClient(conn), nil
	}
	addr, ok := t.peers[target]
	if !ok {
		return nil, fmt.Errorf("grpcx: unknown peer %q", target)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpcx: dial %s: %w", addr, err)
	}
	t.conns[target] = conn
	return raftpb.NewRaftClient(conn), nil
}

// Warmup eagerly creates and starts dialing every peer connection so that the
// first real RPC (a vote request at startup) does not also pay connection-setup
// latency. Connections are non-blocking and retry in the background, so it is
// safe to call before peers are listening.
func (t *Transport) Warmup() {
	for id := range t.peers {
		_, _ = t.client(id) // populates t.conns
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		c.Connect()
	}
}

// Close tears down all peer connections.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
}

func (t *Transport) SendRequestVote(target string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	cli, err := t.client(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	resp, err := cli.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  args.CandidateID,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}
	return &raft.RequestVoteReply{Term: resp.Term, VoteGranted: resp.VoteGranted}, nil
}

func (t *Transport) SendAppendEntries(target string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	cli, err := t.client(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	resp, err := cli.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      toPBEntries(args.Entries),
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}
	return &raft.AppendEntriesReply{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

func (t *Transport) SendInstallSnapshot(target string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	cli, err := t.client(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), snapshotCallTimeout)
	defer cancel()
	resp, err := cli.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              args.Term,
		LeaderId:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &raft.InstallSnapshotReply{Term: resp.Term}, nil
}

// --- conversions ---

func toPBEntries(in []raft.LogEntry) []*raftpb.LogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*raftpb.LogEntry, len(in))
	for i, e := range in {
		out[i] = &raftpb.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Op:    e.Command.Op,
			Key:   e.Command.Key,
			Value: e.Command.Value,
		}
	}
	return out
}

func fromPBEntries(in []*raftpb.LogEntry) []raft.LogEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, len(in))
	for i, e := range in {
		out[i] = raft.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: raft.Command{Op: e.Op, Key: e.Key, Value: e.Value},
		}
	}
	return out
}
