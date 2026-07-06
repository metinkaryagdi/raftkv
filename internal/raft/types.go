// Package raft implements the core of the Raft consensus algorithm as described
// in "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout):
// leader election, log replication, log compaction via snapshotting (§7), and
// single-server cluster membership changes (§6).
package raft

// Role is the current role of a node in the Raft state machine.
//
//	               times out,          receives votes from
//	               starts election     majority of servers
//	┌──────────┐  ───────────────►  ┌───────────┐  ─────────────►  ┌────────┐
//	│ Follower │                    │ Candidate │                  │ Leader │
//	└──────────┘  ◄───────────────  └───────────┘                  └────────┘
//	               discovers current    discovers server
//	               leader or new term   with higher term
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single record in the replicated log. Each entry carries the term
// in which it was created (used for the log-matching safety property) and the
// command to be applied to the state machine once the entry is committed.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command Command
}

// Command is an opaque instruction handed to the state machine. For this project
// the state machine is an in-memory key-value store, so Op is one of
// "set"/"delete" (and "noop" for the leader's no-op entry). Reads are served
// outside the log and are not represented here.
//
// Op "conf_change" is handled specially by raft itself rather than the state
// machine (which ignores it, the same way it already ignores "noop"): ConfigOp
// is "add" or "remove", Key is the target node's id, and Value is the target's
// network address (meaningful only for "add").
type Command struct {
	Op       string // "set", "delete", "noop", "conf_change"
	Key      string
	Value    string
	ConfigOp string // "add" or "remove"; only set when Op == "conf_change"
}

// RequestVoteArgs are the arguments for the RequestVote RPC (§5.2, §5.4).
type RequestVoteArgs struct {
	Term         uint64 // candidate's term
	CandidateID  string // candidate requesting vote
	LastLogIndex uint64 // index of candidate's last log entry
	LastLogTerm  uint64 // term of candidate's last log entry
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
}

// AppendEntriesArgs are the arguments for the AppendEntries RPC (§5.3). An empty
// Entries slice is a heartbeat.
type AppendEntriesArgs struct {
	Term         uint64     // leader's term
	LeaderID     string     // so follower can redirect clients
	PrevLogIndex uint64     // index of log entry immediately preceding new ones
	PrevLogTerm  uint64     // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit uint64     // leader's commitIndex
}

// AppendEntriesReply is the response to an AppendEntries RPC. ConflictIndex and
// ConflictTerm implement the fast log-backtracking optimization described at the
// end of §5.3, so the leader does not have to decrement nextIndex one at a time.
type AppendEntriesReply struct {
	Term    uint64 // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching PrevLogIndex/Term

	ConflictIndex uint64 // first index of the conflicting term (optimization)
	ConflictTerm  uint64 // the conflicting term itself (optimization)
}

// InstallSnapshotArgs are the arguments for the InstallSnapshot RPC (§7), sent
// by a leader to a follower whose required log entries have already been
// compacted away. Unlike the paper's chunked design (meant for state machines
// too large to fit in one RPC message), this project sends the whole snapshot
// in a single, non-chunked call: the state machine is a small map[string]string,
// so the offset/done streaming fields the paper uses would add complexity
// without solving any problem this project actually has.
type InstallSnapshotArgs struct {
	Term              uint64 // leader's term
	LeaderID          string
	LastIncludedIndex uint64 // the snapshot replaces all entries up to and including this index
	LastIncludedTerm  uint64 // term of LastIncludedIndex
	Data              []byte // serialized state machine snapshot (opaque to raft)
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64 // currentTerm, for leader to update itself
}
