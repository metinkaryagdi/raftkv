// Package logfmt is the single source of truth for the one-line, human-
// readable format raft.Events are printed in. cmd/raftkv's stderrLogger
// produces lines in this format; the lab (internal/lab) tails a node's
// stdout/stderr (docker logs -f / kubectl logs -f) and parses them back into
// structured events for its live dashboard — without needing any change to
// the already-tested node binary or the raft.Logger interface itself.
//
// Keeping Format and Parse in one package, exercised by a round-trip test,
// means a future change to the line format breaks a test immediately instead
// of silently desyncing two independent implementations.
package logfmt

import (
	"regexp"
	"strconv"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// Format renders an Event as the single line cmd/raftkv's stderrLogger writes:
//
//	[nodeID] Kind term=T role=R [peer=P] [(Info)]
func Format(nodeID string, e raft.Event) string {
	line := "[" + nodeID + "] " + e.Kind + " term=" + strconv.FormatUint(e.Term, 10) + " role=" + e.Role.String()
	if e.Peer != "" {
		line += " peer=" + e.Peer
	}
	if e.Info != "" {
		line += " (" + e.Info + ")"
	}
	return line
}

// No leading ^ anchor: cmd/raftkv writes this line via a standard log.Logger
// configured with log.LstdFlags|log.Lmicroseconds, which prepends a
// "2006/01/02 15:04:05.000000 " timestamp before the message — so the real
// stdout/stderr line this must parse is NOT anchored at "[", only at the end.
// Found by testing against a real docker logs -f stream, where every line
// silently failed to parse despite passing the in-isolation round-trip test
// (which calls Format then Parse directly, never through a log.Logger).
var lineRE = regexp.MustCompile(`\[(\S+)\] (\S+) term=(\d+) role=(\S+)(?: peer=(\S+))?(?: \((.*)\))?$`)

var roleByName = map[string]raft.Role{
	"Follower":  raft.Follower,
	"Candidate": raft.Candidate,
	"Leader":    raft.Leader,
}

// Parse reverses Format. It returns ok=false if line does not match the
// expected format at all (e.g. it's an unrelated log line from the process,
// such as the startup "node n1: raft(gRPC)=..." message) or names an unknown
// role — callers should skip such lines rather than treat them as an error.
func Parse(line string) (nodeID string, e raft.Event, ok bool) {
	m := lineRE.FindStringSubmatch(line)
	if m == nil {
		return "", raft.Event{}, false
	}
	term, err := strconv.ParseUint(m[3], 10, 64)
	if err != nil {
		return "", raft.Event{}, false
	}
	role, known := roleByName[m[4]]
	if !known {
		return "", raft.Event{}, false
	}
	return m[1], raft.Event{
		Kind: m[2],
		Term: term,
		Role: role,
		Peer: m[5],
		Info: m[6],
	}, true
}
