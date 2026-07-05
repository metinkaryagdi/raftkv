// Package raft implements the core of the Raft consensus algorithm as described
// in "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout).
//
// The scope is deliberately limited to the core protocol: leader election and
// log replication driving a replicated state machine. Log compaction/snapshots
// and dynamic cluster membership changes are intentionally out of scope.
package raft

// Role is the current role of a node in the Raft state machine.
//
//	                times out,          receives votes from
//	                starts election     majority of servers
//	 ┌──────────┐  ───────────────►  ┌───────────┐  ─────────────►  ┌────────┐
//	 │ Follower │                    │ Candidate │                  │ Leader │
//	 └──────────┘  ◄───────────────  └───────────┘                  └────────┘
//	                discovers current    discovers server
//	                leader or new term   with higher term
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
type Command struct {
	Op    string // "set", "delete", "noop"
	Key   string
	Value string
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
