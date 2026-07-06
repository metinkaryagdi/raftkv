package lab

import (
	"bufio"
	"context"

	"github.com/metinkaryagdi/raftkv/internal/logfmt"
	"github.com/metinkaryagdi/raftkv/internal/orchestrator"
	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// Event is what gets broadcast to WebSocket clients: a parsed raft.Event
// tagged with which node emitted it.
type Event struct {
	NodeID string     `json:"nodeId"`
	Event  raft.Event `json:"event"`
}

// tailNode streams id's logs (via orch.Logs, i.e. `docker logs -f`/`kubectl
// logs -f`) and calls emit for every line internal/logfmt can parse back into
// a structured Event — the node binary itself needs no changes for this;
// tailing and parsing its existing stderr output is enough. Runs until ctx is
// canceled or the log stream ends (e.g. the container/pod was removed).
func tailNode(ctx context.Context, orch orchestrator.Orchestrator, id string, emit func(Event)) {
	rc, err := orch.Logs(id)
	if err != nil {
		return
	}
	go func() {
		<-ctx.Done()
		_ = rc.Close()
	}()
	defer rc.Close()

	sc := bufio.NewScanner(rc)
	for sc.Scan() {
		nodeID, e, ok := logfmt.Parse(sc.Text())
		if !ok {
			continue // not every line a node prints is an Event line (e.g. its startup banner)
		}
		if nodeID == "" {
			nodeID = id
		}
		emit(Event{NodeID: nodeID, Event: e})
	}
}
