package lab

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/metinkaryagdi/raftkv/internal/raft"
)

// TestHubBroadcastsToAllConnectedClients verifies the hub's fan-out logic
// against real WebSocket connections (via httptest.Server), not just its
// internal map bookkeeping: N clients connect, one broadcast is sent, and
// every single client must receive it.
func TestHubBroadcastsToAllConnectedClients(t *testing.T) {
	h := newHub()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		h.add(c)
		defer func() {
			h.remove(c)
			c.CloseNow()
		}()
		for {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer ts.Close()

	const n = 3
	wsURL := "ws" + ts.URL[len("http"):]
	conns := make([]*websocket.Conn, n)
	for i := 0; i < n; i++ {
		c, _, err := websocket.Dial(context.Background(), wsURL, nil)
		if err != nil {
			t.Fatalf("client %d dial: %v", i, err)
		}
		conns[i] = c
		defer c.CloseNow()
	}

	// Give the server a moment to register all clients before broadcasting.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		count := len(h.clients)
		h.mu.Unlock()
		if count == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	want := Event{NodeID: "n1", Event: raft.Event{Kind: "become_leader", Term: 3, Role: raft.Leader}}
	h.broadcast(context.Background(), want)

	for i, c := range conns {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var got Event
		err := wsjson.Read(ctx, c, &got)
		cancel()
		if err != nil {
			t.Fatalf("client %d did not receive the broadcast: %v", i, err)
		}
		if got.NodeID != want.NodeID || got.Event.Kind != want.Event.Kind {
			t.Fatalf("client %d got %+v, want %+v", i, got, want)
		}
	}
}
