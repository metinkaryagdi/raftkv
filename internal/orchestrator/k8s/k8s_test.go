package k8s

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// spyExec mirrors the compose package's test helper: it records the command
// the orchestrator tries to run and substitutes a harmless real one, so
// command *construction* is verified without truly invoking kubectl/bash.
type spyExec struct {
	calls [][]string
}

func dummyCmd() *exec.Cmd {
	if runtime.GOOS == "windows" {
		return exec.Command("cmd", "/c", "exit 0")
	}
	return exec.Command("true")
}

func (s *spyExec) fn() func(name string, args ...string) *exec.Cmd {
	return func(name string, args ...string) *exec.Cmd {
		s.calls = append(s.calls, append([]string{name}, args...))
		return dummyCmd()
	}
}

func (s *spyExec) last() string {
	if len(s.calls) == 0 {
		return ""
	}
	return strings.Join(s.calls[len(s.calls)-1], " ")
}

func TestKillNodeBuildsKubectlDeletePod(t *testing.T) {
	spy := &spyExec{}
	o := New(".", "default", "raftkv", "raftkv-headless")
	o.execCommand = spy.fn()

	if err := o.KillNode("raftkv-2"); err != nil {
		t.Fatalf("KillNode: %v", err)
	}
	want := "kubectl delete pod raftkv-2 -n default --grace-period=0 --force"
	if got := spy.last(); got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}

func TestAddNodeInvokesTheAddNodeScript(t *testing.T) {
	spy := &spyExec{}
	o := New(".", "default", "raftkv", "raftkv-headless")
	o.execCommand = spy.fn()

	if err := o.AddNode("ignored"); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	want := "bash scripts/k8s-add-node.sh"
	if got := spy.last(); got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}

func TestRemoveNodeInvokesTheRemoveNodeScript(t *testing.T) {
	spy := &spyExec{}
	o := New(".", "default", "raftkv", "raftkv-headless")
	o.execCommand = spy.fn()

	if err := o.RemoveNode("ignored"); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	want := "bash scripts/k8s-remove-node.sh"
	if got := spy.last(); got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}

func TestHealNodeDeletesTheMatchingNetworkPolicy(t *testing.T) {
	spy := &spyExec{}
	o := New(".", "default", "raftkv", "raftkv-headless")
	o.execCommand = spy.fn()

	if err := o.HealNode("raftkv-5"); err != nil {
		t.Fatalf("HealNode: %v", err)
	}
	want := "kubectl delete networkpolicy lab-isolate-raftkv-5 -n default --ignore-not-found"
	if got := spy.last(); got != want {
		t.Fatalf("command = %q, want %q", got, want)
	}
}
