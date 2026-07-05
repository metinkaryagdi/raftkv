package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// HTTPHandler returns the client-facing REST API for this node:
//
//	GET    /kv/{key}   -> read a value (leader only)
//	PUT    /kv/{key}   -> set a value (body is the raw value)
//	DELETE /kv/{key}   -> delete a key
//	GET    /status     -> node role/term/leader/commit
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
		"id":          st.ID,
		"role":        st.Role.String(),
		"term":        st.Term,
		"leader":      st.LeaderID,
		"commitIndex": st.CommitIndex,
		"logLength":   st.LogLength,
	})
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
	case errors.Is(err, ErrLostLeadership):
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
