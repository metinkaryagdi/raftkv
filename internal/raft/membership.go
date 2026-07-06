package raft

import "errors"

// Single-server membership changes (§6): the cluster's configuration is changed
// one node at a time, carried as an ordinary "conf_change" log entry, which
// guarantees the old and new configurations' quorums always overlap without
// needing joint consensus. The critical subtlety (and the reason this file
// exists separately from apply.go) is that a configuration change takes effect
// as soon as it is *appended* to a node's log — not when it commits — both for
// the leader appending it and for any follower receiving it via
// HandleAppendEntries; see applyConfigChangeLocked.

var (
	// ErrNotLeader is returned by ProposeConfigChange when called on a non-leader.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrConfigChangeInFlight is returned when a configuration change is proposed
	// while a previous one is still uncommitted. Only one change may be in
	// flight at a time — this is what guarantees old/new quorums overlap.
	ErrConfigChangeInFlight = errors.New("raft: a configuration change is already in flight")
)

// hasUncommittedConfigChangeLocked reports whether the log's uncommitted suffix
// (commitIndex+1 through the tail) contains a conf_change entry. This is
// checked fresh from the log itself, rather than tracked as a separate flag, so
// it can never drift out of sync with reality across a leadership change: a new
// leader inherits the log and sees exactly the same answer any other node
// would. Caller must hold mu.
func (n *Node) hasUncommittedConfigChangeLocked() bool {
	lastIdx, _ := n.lastLogInfoLocked()
	for idx := n.commitIndex + 1; idx <= lastIdx; idx++ {
		offset := n.offsetLocked(idx)
		if offset < 0 || offset >= len(n.log) {
			continue
		}
		if n.log[offset].Command.Op == "conf_change" {
			return true
		}
	}
	return false
}

// ProposeConfigChange appends a single-server membership change to the leader's
// log. cmd.ConfigOp must be "add" or "remove"; cmd.Key is the target node's id;
// cmd.Value is the target's network address (used only for "add"). It returns
// ErrNotLeader if this node isn't the leader, or ErrConfigChangeInFlight if a
// previous change hasn't committed yet. Like Submit, a successful return means
// the entry was appended, not that it has committed — the caller learns that
// when the entry surfaces on ApplyCh (the state machine ignores conf_change
// entries, the same way it already ignores "noop").
func (n *Node) ProposeConfigChange(cmd Command) (index uint64, term uint64, err error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	if n.hasUncommittedConfigChangeLocked() {
		n.mu.Unlock()
		return 0, 0, ErrConfigChangeInFlight
	}
	term = n.currentTerm
	lastIdx, _ := n.lastLogInfoLocked()
	index = lastIdx + 1
	entry := LogEntry{Term: term, Index: index, Command: cmd}
	n.log = append(n.log, entry)
	pending := n.applyConfigChangeLocked(index, cmd)
	n.logger.Event(n.id, Event{Kind: "conf_change_proposed", Term: term, Role: Leader,
		Peer: cmd.Key, Info: cmd.ConfigOp})
	transport := n.transport
	n.mu.Unlock()

	pending.apply(transport)
	n.broadcastAppendEntries()
	return index, term, nil
}

// applyConfigChangeLocked mutates n.peers (and, for a leader, the per-peer
// replication bookkeeping) to reflect cmd immediately — this is the append-time
// effect §6 requires, called both from the leader's own append
// (ProposeConfigChange) and a follower's append (HandleAppendEntries), so every
// node that has this entry in its log at all — committed or not — agrees on
// what the current configuration is. entryIndex is the log index cmd itself
// occupies. Caller must hold mu.
//
// It returns a pendingPeerUpdate describing any transport-level address
// bookkeeping the caller must apply — the caller must do so only *after*
// unlocking, per the package's rule that RPCs (and PeerManager, which may
// eagerly dial) are never touched while holding mu.
func (n *Node) applyConfigChangeLocked(entryIndex uint64, cmd Command) pendingPeerUpdate {
	if cmd.Op != "conf_change" {
		return pendingPeerUpdate{}
	}
	// This entry is replicated to (and processed by) every member, including the
	// node it names — n.peers means "the *other* nodes," so a node must never
	// add itself when it happens to be processing the entry that added it.
	if cmd.Key == n.id {
		return pendingPeerUpdate{}
	}
	switch cmd.ConfigOp {
	case "add":
		if !containsString(n.peers, cmd.Key) {
			n.peers = append(n.peers, cmd.Key)
		}
		// Catch-up starts exactly where this peer was introduced; the existing
		// conflict-backtracking mechanism self-heals if this guess is off.
		n.nextIndex[cmd.Key] = entryIndex
		n.matchIndex[cmd.Key] = 0
		return pendingPeerUpdate{id: cmd.Key, addr: cmd.Value, add: true, changed: true}
	case "remove":
		n.peers = removeString(n.peers, cmd.Key)
		delete(n.nextIndex, cmd.Key)
		delete(n.matchIndex, cmd.Key)
		return pendingPeerUpdate{id: cmd.Key, changed: true}
	}
	return pendingPeerUpdate{}
}

// pendingPeerUpdate is the deferred, post-unlock half of applyConfigChangeLocked.
type pendingPeerUpdate struct {
	id      string
	addr    string
	add     bool
	changed bool
}

// apply invokes the transport's PeerManager methods (if it implements that
// optional interface) to reflect the update. Must be called without holding mu.
func (u pendingPeerUpdate) apply(t Transport) {
	if !u.changed {
		return
	}
	pm, ok := t.(PeerManager)
	if !ok {
		return
	}
	if u.add {
		pm.AddPeer(u.id, u.addr)
	} else {
		pm.RemovePeer(u.id)
	}
}

// recomputeConfigFromLogLocked rebuilds n.peers from scratch: starting at the
// original bootstrap configuration (baseConfig) and replaying every committed
// conf_change entry in log order. It must be called whenever HandleAppendEntries
// truncates the log, because the truncated-away suffix might have contained an
// uncommitted conf_change this node had already applied at append time (per
// §6's immediate-effect rule) — without this, a node could retain a phantom
// peer membership from a change a deposed leader proposed but never committed.
// Recomputing strictly from the committed prefix (rather than trying to patch
// forward incrementally) is always correct: §5.3 guarantees a leader never
// truncates a committed entry, so nothing this function reads can itself be
// invalidated by the truncation that triggered it. Caller must hold mu.
func (n *Node) recomputeConfigFromLogLocked() {
	peers := append([]string(nil), n.baseConfig...)
	for idx := uint64(1); idx <= n.commitIndex; idx++ {
		offset := n.offsetLocked(idx)
		if offset < 0 || offset >= len(n.log) {
			continue
		}
		cmd := n.log[offset].Command
		if cmd.Op != "conf_change" || cmd.Key == n.id {
			continue // n.peers never includes this node itself
		}
		switch cmd.ConfigOp {
		case "add":
			if !containsString(peers, cmd.Key) {
				peers = append(peers, cmd.Key)
			}
		case "remove":
			peers = removeString(peers, cmd.Key)
		}
	}
	n.peers = peers
}

func containsString(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func removeString(s []string, v string) []string {
	out := s[:0:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
