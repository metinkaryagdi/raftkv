// Package lab is the control-plane backend for the Raft "lab" dashboard: it
// observes and controls a REAL running Docker-Compose or Kubernetes
// deployment (via internal/orchestrator) rather than a simulated cluster, and
// streams live Raft events to connected browsers over a WebSocket by tailing
// each node's existing log output (internal/logfmt) — no changes to the
// already-tested node binary are needed for any of this.
package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/metinkaryagdi/raftkv/internal/orchestrator"
)

// Server is the lab's HTTP + WebSocket control-plane API.
type Server struct {
	orch       orchestrator.Orchestrator
	hub        *hub
	httpClient *http.Client

	mu      sync.Mutex
	tailing map[string]context.CancelFunc // node id -> cancels its log-tailing goroutine
}

// NewServer wraps orch (a compose or k8s Orchestrator).
func NewServer(orch orchestrator.Orchestrator) *Server {
	return &Server{
		orch:       orch,
		hub:        newHub(),
		httpClient: &http.Client{Timeout: 3 * time.Second},
		tailing:    make(map[string]context.CancelFunc),
	}
}

// Handler returns the lab's API routes:
//
//	GET  /api/nodes                       -> []orchestrator.NodeRef
//	GET  /api/nodes/{id}/status           -> proxies the node's own GET /status
//	GET  /api/nodes/{id}/log              -> proxies the node's own GET /debug/log
//	POST /api/orchestrator/kill/{id}      -> Orchestrator.KillNode
//	POST /api/orchestrator/isolate/{id}   -> Orchestrator.IsolateNode
//	POST /api/orchestrator/heal/{id}      -> Orchestrator.HealNode
//	POST /api/orchestrator/add/{id}       -> Orchestrator.AddNode
//	POST /api/orchestrator/remove/{id}    -> Orchestrator.RemoveNode
//	POST /api/cluster/add-server          -> finds the leader, proxies POST /cluster/add-server
//	POST /api/cluster/remove-server       -> finds the leader, proxies POST /cluster/remove-server
//	GET  /ws/events                       -> live parsed Event stream (WebSocket)
//
// It does not serve the static frontend; cmd/raftlab mounts that separately.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/nodes", s.handleListNodes)
	mux.HandleFunc("GET /api/nodes/{id}/status", s.handleNodeStatus)
	mux.HandleFunc("GET /api/nodes/{id}/log", s.handleNodeLog)
	mux.HandleFunc("POST /api/orchestrator/kill/{id}", s.action(s.orch.KillNode))
	mux.HandleFunc("POST /api/orchestrator/isolate/{id}", s.action(s.orch.IsolateNode))
	mux.HandleFunc("POST /api/orchestrator/heal/{id}", s.action(s.orch.HealNode))
	mux.HandleFunc("POST /api/orchestrator/add/{id}", s.action(s.orch.AddNode))
	mux.HandleFunc("POST /api/orchestrator/remove/{id}", s.action(s.orch.RemoveNode))
	mux.HandleFunc("POST /api/cluster/add-server", s.handleClusterAddServer)
	mux.HandleFunc("POST /api/cluster/remove-server", s.handleClusterRemoveServer)
	mux.HandleFunc("GET /api/kv/{key}", s.handleKVGet)
	mux.HandleFunc("PUT /api/kv/{key}", s.handleKVPut)
	mux.HandleFunc("DELETE /api/kv/{key}", s.handleKVDelete)
	mux.HandleFunc("GET /ws/events", s.handleWS)
	return mux
}

func (s *Server) handleListNodes(w http.ResponseWriter, _ *http.Request) {
	nodes, err := s.orch.ListNodes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, n := range nodes {
		s.ensureTailing(n.ID)
	}
	writeJSON(w, http.StatusOK, nodes)
}

// ensureTailing starts a log-tailing goroutine for id the first time it's
// seen, and never more than once — repeated /api/nodes polls (the frontend's
// steady-state behavior) must not pile up duplicate tails.
func (s *Server) ensureTailing(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tailing[id]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.tailing[id] = cancel
	go tailNode(ctx, s.orch, id, func(e Event) {
		s.hub.broadcast(context.Background(), e)
	})
}

// Close stops every log-tailing goroutine. Safe to call once at shutdown.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cancel := range s.tailing {
		cancel()
	}
	s.tailing = make(map[string]context.CancelFunc)
}

func (s *Server) nodeAddr(id string) (string, error) {
	nodes, err := s.orch.ListNodes()
	if err != nil {
		return "", err
	}
	for _, n := range nodes {
		if n.ID == id {
			if n.Addr == "" {
				return "", fmt.Errorf("node %q has no reachable address", id)
			}
			return n.Addr, nil
		}
	}
	return "", fmt.Errorf("unknown node %q", id)
}

func (s *Server) proxyGet(w http.ResponseWriter, path, id string) {
	addr, err := s.nodeAddr(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	resp, err := s.httpClient.Get("http://" + addr + path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleNodeStatus(w http.ResponseWriter, r *http.Request) {
	s.proxyGet(w, "/status", r.PathValue("id"))
}

func (s *Server) handleNodeLog(w http.ResponseWriter, r *http.Request) {
	s.proxyGet(w, "/debug/log", r.PathValue("id"))
}

// action adapts an Orchestrator method into an HTTP handler keyed by the
// {id} path value.
func (s *Server) action(fn func(id string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if err := fn(id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": id})
	}
}

// findLeaderAddr polls each known node's own /status (the same contract
// clients of the KV API already use for the 421-redirect-retry pattern) until
// one reports itself as Leader.
func (s *Server) findLeaderAddr() (string, error) {
	nodes, err := s.orch.ListNodes()
	if err != nil {
		return "", err
	}
	for _, n := range nodes {
		if !n.Ready || n.Addr == "" {
			continue
		}
		resp, err := s.httpClient.Get("http://" + n.Addr + "/status")
		if err != nil {
			continue
		}
		var st struct {
			Role string `json:"role"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if st.Role == "Leader" {
			return n.Addr, nil
		}
	}
	return "", fmt.Errorf("no leader found among known nodes")
}

func (s *Server) proxyPost(w http.ResponseWriter, r *http.Request, addr, path string) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	resp, err := s.httpClient.Post("http://"+addr+path, "application/json", bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) handleClusterAddServer(w http.ResponseWriter, r *http.Request) {
	addr, err := s.findLeaderAddr()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.proxyPost(w, r, addr, "/cluster/add-server")
}

func (s *Server) handleClusterRemoveServer(w http.ResponseWriter, r *http.Request) {
	addr, err := s.findLeaderAddr()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.proxyPost(w, r, addr, "/cluster/remove-server")
}

// handleWS upgrades to a WebSocket and streams parsed Events (pure push — no
// polling) until the client disconnects.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	s.hub.add(c)
	defer func() {
		s.hub.remove(c)
		_ = c.CloseNow()
	}()
	ctx := r.Context()
	for {
		// The lab never expects messages FROM the client; Read just detects
		// disconnection (a closed/broken connection unblocks it with an error).
		if _, _, err := c.Read(ctx); err != nil {
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleKVGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	addr, err := s.findLeaderAddr()
	if err != nil {
		nodes, lErr := s.orch.ListNodes()
		if lErr == nil {
			for _, n := range nodes {
				if n.Ready && n.Addr != "" {
					addr = n.Addr
					break
				}
			}
		}
	}
	if addr == "" {
		http.Error(w, "no reachable cluster node found", http.StatusServiceUnavailable)
		return
	}
	s.proxyGetDirect(w, "http://"+addr+"/kv/"+key)
}

func (s *Server) handleKVPut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	addr, err := s.findLeaderAddr()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	s.proxyPost(w, r, addr, "/kv/"+key)
}

func (s *Server) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	addr, err := s.findLeaderAddr()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodDelete, "http://"+addr+"/kv/"+key, nil)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) proxyGetDirect(w http.ResponseWriter, urlStr string) {
	resp, err := s.httpClient.Get(urlStr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

