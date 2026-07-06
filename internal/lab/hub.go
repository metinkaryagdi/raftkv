package lab

import (
	"context"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// hub fans out parsed events to every connected WebSocket client. It is a
// small hand-rolled broadcaster rather than a dependency, matching the
// project's minimal-deps house style — the only thing it needs is "remember
// who's connected, write to all of them."
type hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]struct{}
}

func newHub() *hub {
	return &hub{clients: make(map[*websocket.Conn]struct{})}
}

func (h *hub) add(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[c] = struct{}{}
}

func (h *hub) remove(c *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

// broadcast sends v (JSON-encoded) to every currently-connected client. A
// slow or dead client that errors is dropped rather than allowed to block
// delivery to everyone else.
func (h *hub) broadcast(ctx context.Context, v any) {
	h.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(h.clients))
	for c := range h.clients {
		conns = append(conns, c)
	}
	h.mu.Unlock()

	for _, c := range conns {
		if err := wsjson.Write(ctx, c, v); err != nil {
			h.remove(c)
		}
	}
}
