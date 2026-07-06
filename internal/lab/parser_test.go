package lab

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/orchestrator"
)

// fakeOrchestrator implements orchestrator.Orchestrator, returning a canned
// log stream from Logs and failing every other method (unused by these
// tests, but required to satisfy the interface).
type fakeOrchestrator struct {
	logLines string
}

func (f *fakeOrchestrator) ListNodes() ([]orchestrator.NodeRef, error) { return nil, nil }
func (f *fakeOrchestrator) KillNode(string) error                      { return nil }
func (f *fakeOrchestrator) IsolateNode(string) error                   { return nil }
func (f *fakeOrchestrator) HealNode(string) error                      { return nil }
func (f *fakeOrchestrator) AddNode(string) error                       { return nil }
func (f *fakeOrchestrator) RemoveNode(string) error                    { return nil }
func (f *fakeOrchestrator) Logs(string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(f.logLines)), nil
}

var _ orchestrator.Orchestrator = (*fakeOrchestrator)(nil)

// TestTailNodeParsesEventLinesAndSkipsOthers proves tailNode correctly turns
// a node's real log output (mixing an Event line format with unrelated lines
// like the startup banner) into emitted Events, in order, skipping anything
// internal/logfmt.Parse can't make sense of.
func TestTailNodeParsesEventLinesAndSkipsOthers(t *testing.T) {
	logLines := strings.Join([]string{
		"node n1: raft(gRPC)=127.0.0.1:9001 http=127.0.0.1:8001 peers=[n2 n3]",
		"[n1] start term=0 role=Follower",
		"[n1] election_start term=1 role=Candidate",
		"[n1] become_leader term=1 role=Leader",
		"",
	}, "\n")
	orch := &fakeOrchestrator{logLines: logLines}

	var got []Event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		tailNode(ctx, orch, "n1", func(e Event) { got = append(got, e) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("tailNode did not finish reading the (finite) fake log stream in time")
	}

	if len(got) != 3 {
		t.Fatalf("got %d events, want 3 (banner line should be skipped): %+v", len(got), got)
	}
	wantKinds := []string{"start", "election_start", "become_leader"}
	for i, e := range got {
		if e.NodeID != "n1" {
			t.Errorf("event %d nodeID = %q, want n1", i, e.NodeID)
		}
		if e.Event.Kind != wantKinds[i] {
			t.Errorf("event %d kind = %q, want %q", i, e.Event.Kind, wantKinds[i])
		}
	}
}
