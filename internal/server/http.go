package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// HTTPHandler returns the client-facing REST API for this node:
//
//	GET    /kv/{key}              -> read a value (leader only)
//	PUT    /kv/{key}              -> set a value (body is the raw value)
//	DELETE /kv/{key}              -> delete a key
//	GET    /status                -> node role/term/leader/commit
//	GET    /debug/log             -> full retained log + LastIncludedIndex (for the lab's log-matrix view)
//	POST   /cluster/add-server    -> propose adding a node ({"id","raftAddr"})
//	POST   /cluster/remove-server -> propose removing a node ({"id"})
//
// Non-leader nodes reject writes and reads with 421 and a JSON body naming the
// current leader so the client can redirect.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handleSet)
	mux.HandleFunc("POST /kv/{key}", s.handleSet)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /debug/log", s.handleDebugLog)
	mux.HandleFunc("POST /cluster/add-server", s.handleAddServer)
	mux.HandleFunc("POST /cluster/remove-server", s.handleRemoveServer)
	return mux
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value, ok, err := s.Get(key)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err := s.Set(key, string(body)); err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": key})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.Delete(key); err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": key})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := s.node.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":                st.ID,
		"role":              st.Role.String(),
		"term":              st.Term,
		"leader":            st.LeaderID,
		"commitIndex":       st.CommitIndex,
		"logLength":         st.LogLength,
		"lastIncludedIndex": st.LastIncludedIndex,
	})
}

// handleDebugLog exposes the node's retained log entries plus
// LastIncludedIndex, needed together to correctly interpret log positions
// post-compaction: an index at or below LastIncludedIndex was compacted away
// (not "never existed" — a distinction the lab's log-matrix view must render
// differently), and LogCopy() alone can't tell those two cases apart.
func (s *Server) handleDebugLog(w http.ResponseWriter, _ *http.Request) {
	st := s.node.Status()
	writeJSON(w, http.StatusOK, map[string]any{
		"lastIncludedIndex": st.LastIncludedIndex,
		"entries":           s.node.LogCopy(),
	})
}

type addServerRequest struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raftAddr"`
}

func (s *Server) handleAddServer(w http.ResponseWriter, r *http.Request) {
	var req addServerRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ID == "" || req.RaftAddr == "" {
		http.Error(w, "id and raftAddr are required", http.StatusBadRequest)
		return
	}
	if err := s.AddServer(req.ID, req.RaftAddr); err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": req.ID})
}

type removeServerRequest struct {
	ID string `json:"id"`
}

func (s *Server) handleRemoveServer(w http.ResponseWriter, r *http.Request) {
	var req removeServerRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	if err := s.RemoveServer(req.ID); err != nil {
		s.writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": req.ID})
}

// writeErr maps server errors to HTTP responses. A not-leader error carries the
// leader hint so clients can retry against the right node.
func (s *Server) writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		writeJSON(w, http.StatusMisdirectedRequest, map[string]string{
			"error":  "not leader",
			"leader": s.LeaderHint(),
		})
	case errors.Is(err, ErrLostLeadership), errors.Is(err, ErrConfigChangeInFlight):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, ErrTimeout):
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
