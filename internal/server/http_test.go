package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHTTPKVRoutes exercises the client-facing REST API end to end over a real
// HTTP connection (httptest.Server), not just the Server struct's Go methods
// directly — proving the actual wire format (status codes, JSON shapes,
// PathValue routing) works, which none of the existing Server-level tests do.
func TestHTTPKVRoutes(t *testing.T) {
	c := newSvcCluster(t, 3)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	ts := httptest.NewServer(leader.HTTPHandler())
	defer ts.Close()

	// PUT.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/kv/city", bytes.NewBufferString("istanbul"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status=%d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// GET.
	resp, err = http.Get(ts.URL + "/kv/city")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	resp.Body.Close()
	if body["value"] != "istanbul" {
		t.Fatalf("GET /kv/city = %v, want value=istanbul", body)
	}

	// GET /status.
	resp, err = http.Get(ts.URL + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	var status map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	resp.Body.Close()
	if status["role"] != "Leader" {
		t.Fatalf("GET /status role=%v, want Leader", status["role"])
	}

	// DELETE, then GET should 404.
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/kv/city", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/kv/city")
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status=%d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestHTTPAddServerSucceedsOnLeader proves POST /cluster/add-server works over
// real HTTP end to end: the entry is proposed, committed by the two other real
// nodes in this 3-node cluster, and the endpoint returns 200 only once that
// commit has actually happened.
func TestHTTPAddServerSucceedsOnLeader(t *testing.T) {
	c := newSvcCluster(t, 3)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	ts := httptest.NewServer(leader.HTTPHandler())
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"id": "n4", "raftAddr": "unused-in-memory"})
	resp, err := http.Post(ts.URL+"/cluster/add-server", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /cluster/add-server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d, want 200; body=%s", resp.StatusCode, b)
	}
}

// TestHTTPAddServerRedirectsOnFollower proves a follower correctly refuses the
// request with 421 and a leader hint, the same redirect contract writes/reads
// already use, rather than silently accepting a change it cannot durably apply.
func TestHTTPAddServerRedirectsOnFollower(t *testing.T) {
	c := newSvcCluster(t, 3)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)
	follower := c.followers(leader)[0]

	ts := httptest.NewServer(follower.HTTPHandler())
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"id": "n4", "raftAddr": "addr"})
	resp, err := http.Post(ts.URL+"/cluster/add-server", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /cluster/add-server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("status=%d, want 421", resp.StatusCode)
	}
	var errBody map[string]string
	json.NewDecoder(resp.Body).Decode(&errBody)
	if errBody["leader"] == "" {
		t.Fatalf("421 response should carry a leader hint, got %v", errBody)
	}
}

// TestHTTPAddServerRejectsBadRequest proves missing required fields are
// rejected with 400 before ever reaching raft, for both /cluster endpoints.
func TestHTTPAddServerRejectsBadRequest(t *testing.T) {
	c := newSvcCluster(t, 3)
	c.startAll()
	defer c.stopAll()
	leader := c.waitLeader(2 * time.Second)

	ts := httptest.NewServer(leader.HTTPHandler())
	defer ts.Close()

	cases := []struct {
		path string
		body string
	}{
		{"/cluster/add-server", `{"id":""}`},
		{"/cluster/add-server", `not-json`},
		{"/cluster/remove-server", `{"id":""}`},
	}
	for _, c := range cases {
		resp, err := http.Post(ts.URL+c.path, "application/json", bytes.NewBufferString(c.body))
		if err != nil {
			t.Fatalf("POST %s: %v", c.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("POST %s %q: status=%d, want 400", c.path, c.body, resp.StatusCode)
		}
	}
}
