package compose

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func dummyCmd() *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.Command("cmd", "/c", "exit 0")
	}
	return exec.Command("true")
}

func echoCmd(arg string) *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.Command("cmd", "/c", "echo "+arg)
	}
	return exec.Command("echo", arg)
}

// spyExec records every command the orchestrator tries to run and substitutes
// a harmless real command in its place — this tests command *construction*
// (the args/flags actually being built) without truly invoking docker/bash,
// which the project's plan explicitly scopes as impractical to verify in
// `go test` (see scripts/demo-failover*.{sh,ps1} for how live orchestration is
// instead verified manually).
type spyExec struct {
	calls [][]string
	real  func(name string, args ...string) *exec.Cmd
}

func (s *spyExec) fn() func(name string, args ...string) *exec.Cmd {
	return func(name string, args ...string) *exec.Cmd {
		s.calls = append(s.calls, append([]string{name}, args...))
		if s.real != nil {
			return s.real(name, args...)
		}
		return dummyCmd()
	}
}

func (s *spyExec) last() string {
	if len(s.calls) == 0 {
		return ""
	}
	return strings.Join(s.calls[len(s.calls)-1], " ")
}

// canned answers the two distinct shapes containerName/discoverNetwork issue
// via "docker compose ps": "-q <svc>" (discoverNetwork, wants a container id)
// and "<svc> --format {{.Name}}" (containerName, wants a resolved container
// name — genesis services are NOT simply named after their bare service id in
// real Compose deployments, e.g. "n2" resolves to "raft_konsenss-n2-1"; this
// stands in for that resolution without invoking real docker). Also answers
// "docker inspect ... --format ..." (discoverNetwork's second step).
func canned() func(name string, args ...string) *exec.Cmd {
	return func(name string, args ...string) *exec.Cmd {
		if name == "docker" && len(args) >= 2 && args[0] == "compose" && args[1] == "ps" {
			for _, a := range args {
				if a == "-q" {
					return echoCmd("fake-container-id")
				}
			}
			if len(args) >= 3 {
				return echoCmd("resolved-" + args[2])
			}
		}
		if name == "docker" && len(args) >= 1 && args[0] == "inspect" {
			return echoCmd("fake_network")
		}
		return dummyCmd()
	}
}

func TestKillNodeBuildsDockerKill(t *testing.T) {
	spy := &spyExec{real: canned()}
	o := New(".", []string{"n1", "n2", "n3"})
	o.execCommand = spy.fn()

	if err := o.KillNode("n2"); err != nil {
		t.Fatalf("KillNode: %v", err)
	}
	want := "docker kill resolved-n2"
	if got := spy.last(); got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}

func TestKillNodeOfDynamicNodeUsesRaftkvPrefix(t *testing.T) {
	spy := &spyExec{}
	o := New(".", []string{"n1", "n2", "n3"})
	o.execCommand = spy.fn()

	if err := o.KillNode("n6"); err != nil {
		t.Fatalf("KillNode: %v", err)
	}
	if got := spy.last(); got != "docker kill raftkv-n6" {
		t.Fatalf("command = %q, want %q (dynamically-added nodes use the raftkv-<id> convention from compose-add-node.sh, and need no name resolution)", got, "docker kill raftkv-n6")
	}
}

func TestAddNodeInvokesTheAddNodeScript(t *testing.T) {
	spy := &spyExec{}
	o := New(".", []string{"n1"})
	o.execCommand = spy.fn()

	if err := o.AddNode("n6"); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if got := spy.last(); got != "bash scripts/compose-add-node.sh n6" {
		t.Fatalf("command = %q, want %q", got, "bash scripts/compose-add-node.sh n6")
	}
}

func TestRemoveNodeInvokesTheRemoveNodeScript(t *testing.T) {
	spy := &spyExec{}
	o := New(".", []string{"n1"})
	o.execCommand = spy.fn()

	if err := o.RemoveNode("n6"); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if got := spy.last(); got != "bash scripts/compose-remove-node.sh n6" {
		t.Fatalf("command = %q, want %q", got, "bash scripts/compose-remove-node.sh n6")
	}
}

func TestDynamicNodeIDExcludesComposeManagedContainers(t *testing.T) {
	// Pinning the Compose project name to "raftkv" (docker-compose.yml's
	// `name:`) means genesis containers AND the lab service itself also start
	// with "raftkv-" (e.g. "raftkv-n1-1", "raftkv-lab-1") — the same prefix
	// scripts/compose-add-node.sh uses for genuinely dynamic nodes
	// ("raftkv-n6", no replica suffix). Found live: after adding `name:
	// raftkv`, ListNodes() started reporting the genesis nodes and the lab
	// container itself as "dynamically added" nodes named "n1-1".."lab-1".
	cases := []struct {
		name    string
		wantID  string
		wantDyn bool
	}{
		{"raftkv-n6", "n6", true},
		{"raftkv-n1-1", "", false},
		{"raftkv-lab-1", "", false},
		{"raftkv-n42-3", "", false},
		{"unrelated-container", "", false},
	}
	for _, c := range cases {
		id, ok := dynamicNodeID(c.name)
		if ok != c.wantDyn || id != c.wantID {
			t.Errorf("dynamicNodeID(%q) = (%q, %v), want (%q, %v)", c.name, id, ok, c.wantID, c.wantDyn)
		}
	}
}

func TestIsolateAndHealNodeUseDiscoveredNetworkAndResolvedName(t *testing.T) {
	spy := &spyExec{real: canned()}
	o := New(".", []string{"n1", "n2"})
	o.execCommand = spy.fn()

	if err := o.IsolateNode("n2"); err != nil {
		t.Fatalf("IsolateNode: %v", err)
	}
	// discoverNetwork (2 commands: ps -q, inspect) + containerName (1 command:
	// ps --format) + the disconnect itself = 4.
	if len(spy.calls) != 4 {
		t.Fatalf("expected 4 commands, got %d: %v", len(spy.calls), spy.calls)
	}
	want := "docker network disconnect fake_network resolved-n2"
	if got := spy.last(); got != want {
		t.Fatalf("last command = %q, want %q", got, want)
	}

	// A fresh orchestrator instance, not the same o: containerName caches its
	// resolved name per-instance, and reusing o here would make HealNode's
	// lookup a cache hit (3 calls) instead of exercising the full resolution
	// path (4) a second time.
	spy2 := &spyExec{real: canned()}
	o2 := New(".", []string{"n1", "n2"})
	o2.execCommand = spy2.fn()
	if err := o2.HealNode("n2"); err != nil {
		t.Fatalf("HealNode: %v", err)
	}
	if len(spy2.calls) != 4 {
		t.Fatalf("expected 4 commands, got %d: %v", len(spy2.calls), spy2.calls)
	}
	want = "docker network connect fake_network resolved-n2"
	if got := spy2.last(); got != want {
		t.Fatalf("last command = %q, want %q", got, want)
	}
}
