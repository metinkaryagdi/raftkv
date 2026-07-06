package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Config holds the parameters needed to construct a Node.
type Config struct {
	// ID uniquely identifies this node within the cluster.
	ID string
	// Peers are the IDs of the *other* nodes in the cluster (excluding ID).
	Peers []string
	// Transport is used to send RPCs to peers.
	Transport Transport

	// ElectionTimeoutMin/Max bound the randomized election timeout. Randomization
	// across nodes is what breaks split votes (§5.2). Max must be > Min.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	// HeartbeatInterval is how often a leader sends AppendEntries. It must be
	// comfortably smaller than ElectionTimeoutMin.
	HeartbeatInterval time.Duration

	// Logger, if non-nil, receives structured state-transition events. Optional.
	Logger Logger

	// Joining marks a node that is starting with no known peers because it is
	// about to be added to an existing cluster via a dynamic membership change
	// (see ProposeConfigChange), rather than being a genesis member of a
	// brand-new cluster. A joining node never starts an election or grants a
	// vote on its own — with Peers empty, quorum() would trivially be 1, and it
	// would otherwise win its own election and become a rogue single-node
	// "leader" that never learns the real cluster. It stops joining and behaves
	// normally the first time a real leader successfully sends it AppendEntries.
	Joining bool

	// tickInterval controls how often the internal loop checks its timers. Zero
	// selects a sensible default. Exposed only for tests that need fast ticks.
	tickInterval time.Duration
}

// Node is a single Raft server. It is safe for concurrent use; all mutable state
// is guarded by mu. Network RPCs are never issued while holding mu.
type Node struct {
	id        string
	peers     []string
	transport Transport
	logger    Logger

	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration
	heartbeatInterval  time.Duration
	tickInterval       time.Duration

	mu sync.Mutex // guards everything below

	// --- Persistent state on all servers (would be persisted before responding
	// to RPCs; persistence is out of scope so it lives in memory). ---
	currentTerm uint64
	votedFor    string     // candidateID that received vote in currentTerm ("" = none)
	log         []LogEntry // retained log entries; log[0] is a movable sentinel

	// lastIncludedIndex/Term describe the sentinel at log[0]: the logical index
	// and term of the last entry folded into the most recent snapshot. Before any
	// compaction these are (0, 0), identical to the original always-zero
	// sentinel, so the no-snapshot case is unchanged. A logical index translates
	// to a slice offset via `offset := logicalIndex - lastIncludedIndex`; offset 0
	// is always the sentinel.
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotData      []byte // last snapshot payload, opaque to raft; sent via InstallSnapshot

	// --- Volatile state on all servers. ---
	role        Role
	leaderID    string // last known leader, for client redirection
	commitIndex uint64 // highest log entry known to be committed
	lastApplied uint64 // highest log entry applied to the state machine

	// --- Volatile state on leaders (reinitialized after election). ---
	nextIndex  map[string]uint64 // for each peer, next log index to send
	matchIndex map[string]uint64 // for each peer, highest replicated index

	// baseConfig is the cluster's original bootstrap peer set (Config.Peers at
	// construction), never mutated afterward. It is the fallback membership when
	// reverting after a truncated, never-committed configuration change (see
	// recomputeConfigFromLogLocked in membership.go).
	baseConfig []string

	// joining suppresses election-starting and vote-granting until this node has
	// been contacted by a real leader (see Config.Joining).
	joining bool

	// --- Election / heartbeat timers. ---
	lastHeard       time.Time     // last time we heard from a valid leader or granted a vote
	electionTimeout time.Duration // current randomized timeout
	lastHeartbeat   time.Time     // last time (as leader) we broadcast heartbeats

	rng *rand.Rand

	// --- Lifecycle. ---
	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup

	// applyCh receives commands as they are committed; consumed by the state
	// machine. applySignal wakes the applier goroutine when commitIndex advances.
	applyCh     chan ApplyMsg
	applySignal chan struct{}

	// snapshotCh delivers a state-machine snapshot whenever this node installs
	// one received from a leader (see HandleInstallSnapshot in snapshot.go).
	// Consumed by the state machine exactly like applyCh.
	snapshotCh chan SnapshotMsg
}

// ApplyMsg is delivered on the apply channel for each newly committed entry.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command Command
}

const defaultTickInterval = 10 * time.Millisecond

// NewNode constructs a Node from cfg. It does not start any goroutines; call
// Start to begin participating in the cluster.
func NewNode(cfg Config) *Node {
	tick := cfg.tickInterval
	if tick == 0 {
		tick = defaultTickInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = nopLogger{}
	}
	n := &Node{
		id:                 cfg.ID,
		peers:              append([]string(nil), cfg.Peers...),
		baseConfig:         append([]string(nil), cfg.Peers...),
		transport:          cfg.Transport,
		logger:             logger,
		electionTimeoutMin: cfg.ElectionTimeoutMin,
		electionTimeoutMax: cfg.ElectionTimeoutMax,
		heartbeatInterval:  cfg.HeartbeatInterval,
		tickInterval:       tick,
		role:               Follower,
		joining:            cfg.Joining,
		// A single sentinel entry at index 0 removes the special-casing of an
		// empty log: the "previous" entry always exists.
		log:         []LogEntry{{Term: 0, Index: 0}},
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() ^ hashID(cfg.ID))),
		stopCh:      make(chan struct{}),
		applyCh:     make(chan ApplyMsg, 256),
		applySignal: make(chan struct{}, 1),
		snapshotCh:  make(chan SnapshotMsg, 4),
	}
	n.resetElectionTimerLocked()
	return n
}

// ApplyCh returns the channel on which committed commands are delivered. It is
// consumed by the state machine.
func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }

// SnapshotCh returns the channel on which installed snapshots are delivered
// (see HandleInstallSnapshot). The state machine must load the snapshot's data,
// replacing its current state, whenever a message arrives here.
func (n *Node) SnapshotCh() <-chan SnapshotMsg { return n.snapshotCh }

// ID returns the node's identifier.
func (n *Node) ID() string { return n.id }

// Start launches the node's internal loop.
func (n *Node) Start() {
	n.wg.Add(2)
	go n.run()
	go n.applier()
	n.logger.Event(n.id, Event{Kind: "start", Term: 0, Role: Follower})
}

// Stop halts the node's internal loop and blocks until it has exited.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.mu.Unlock()
	n.wg.Wait()
}

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

// tick advances timers and, when they expire, triggers an election (as a
// follower/candidate) or a heartbeat broadcast (as a leader). Network fan-out is
// always performed without holding the lock.
func (n *Node) tick() {
	n.mu.Lock()
	switch n.role {
	case Leader:
		if time.Since(n.lastHeartbeat) >= n.heartbeatInterval {
			n.lastHeartbeat = time.Now()
			n.mu.Unlock()
			n.broadcastAppendEntries()
			return
		}
	case Follower, Candidate:
		// A joining node (empty peer set, about to be added via a dynamic
		// membership change) must never start an election: quorum() would
		// trivially be 1, so it would win instantly and become a rogue
		// single-node "leader" that never learns the real cluster.
		if n.joining {
			break
		}
		if time.Since(n.lastHeard) >= n.electionTimeout {
			n.mu.Unlock()
			n.startElection()
			return
		}
	}
	n.mu.Unlock()
}

// --- Role transitions (all require the caller to hold mu) ---

// becomeFollowerLocked reverts to follower. votedFor is cleared only when moving
// to a strictly higher term, so a candidate that steps down for a same-term
// leader keeps its recorded vote (§5.1).
func (n *Node) becomeFollowerLocked(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	if n.role != Follower {
		n.logger.Event(n.id, Event{Kind: "step_down", Term: n.currentTerm, Role: Follower})
	}
	n.role = Follower
}

func (n *Node) resetElectionTimerLocked() {
	n.lastHeard = time.Now()
	span := n.electionTimeoutMax - n.electionTimeoutMin
	if span <= 0 {
		n.electionTimeout = n.electionTimeoutMin
		return
	}
	n.electionTimeout = n.electionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)))
}

// quorum is the number of nodes required for a majority.
func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}

// hashID gives a stable per-node contribution to the RNG seed so that nodes
// started within the same nanosecond still diverge.
func hashID(s string) int64 {
	var h int64 = 1469598103934665603 // FNV-1a offset basis (64-bit)
	for i := 0; i < len(s); i++ {
		h ^= int64(s[i])
		h *= 1099511628211
	}
	return h
}
