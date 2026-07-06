// Package k8s implements orchestrator.Orchestrator against a running
// Kubernetes StatefulSet deployment (see deploy/k8s/raftkv.yaml,
// scripts/k8s-add-node.sh, scripts/k8s-remove-node.sh at the repo root).
package k8s

import (
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metinkaryagdi/raftkv/internal/orchestrator"
)

// portForwardBase is the first local port used for a pod's kubectl
// port-forward; pod raftkv-N gets port portForwardBase+N, so the mapping is
// deterministic and stable across calls.
const portForwardBase = 19000

// Orchestrator drives a Kubernetes StatefulSet deployment via kubectl.
type Orchestrator struct {
	Dir             string // repo root containing scripts/
	Namespace       string // e.g. "default"
	StatefulSetName string // e.g. "raftkv"
	ServiceName     string // headless service, e.g. "raftkv-headless"

	execCommand func(name string, args ...string) *exec.Cmd

	// Pod addresses (raftkv-N.raftkv-headless:9001) are only reachable from
	// inside the cluster; ListNodes instead lazily starts and reuses a
	// kubectl port-forward per pod so the lab process (running outside the
	// cluster) can reach each node's HTTP API directly.
	mu           sync.Mutex
	portForwards map[string]*exec.Cmd // pod name -> the running port-forward process
}

// New returns a k8s Orchestrator for the given StatefulSet.
func New(dir, namespace, statefulSetName, serviceName string) *Orchestrator {
	return &Orchestrator{
		Dir: dir, Namespace: namespace, StatefulSetName: statefulSetName, ServiceName: serviceName,
		execCommand:  exec.Command,
		portForwards: make(map[string]*exec.Cmd),
	}
}

var _ orchestrator.Orchestrator = (*Orchestrator)(nil)

func (o *Orchestrator) cmd() func(name string, args ...string) *exec.Cmd {
	if o.execCommand != nil {
		return o.execCommand
	}
	return exec.Command
}

func (o *Orchestrator) run(name string, args ...string) ([]byte, error) {
	c := o.cmd()(name, args...)
	c.Dir = o.Dir
	out, err := c.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("%s %s: %w (%s)", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func (o *Orchestrator) replicas() (int, error) {
	out, err := o.run("kubectl", "get", "statefulset", o.StatefulSetName, "-n", o.Namespace,
		"-o", "jsonpath={.spec.replicas}")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(out)))
}

// ordinal extracts a StatefulSet pod's numeric suffix, e.g. "raftkv-3" -> 3.
func ordinal(podName string) (int, bool) {
	i := strings.LastIndex(podName, "-")
	if i < 0 {
		return 0, false
	}
	n, err := strconv.Atoi(podName[i+1:])
	return n, err == nil
}

// ensurePortForward lazily starts (or reuses) a kubectl port-forward for pod,
// returning the local port the lab can reach it on. The pod's own HTTP address
// (raftkv-N.raftkv-headless:8001) only resolves inside the cluster's DNS, so
// this is how a lab process running outside the cluster reaches it at all.
func (o *Orchestrator) ensurePortForward(pod string) (int, error) {
	n, ok := ordinal(pod)
	if !ok {
		return 0, fmt.Errorf("k8s: cannot determine ordinal for pod %q", pod)
	}
	port := portForwardBase + n

	o.mu.Lock()
	if _, running := o.portForwards[pod]; running {
		o.mu.Unlock()
		return port, nil
	}
	c := o.cmd()("kubectl", "port-forward", "pod/"+pod, fmt.Sprintf("%d:8001", port), "-n", o.Namespace)
	c.Dir = o.Dir
	if err := c.Start(); err != nil {
		o.mu.Unlock()
		return 0, err
	}
	o.portForwards[pod] = c
	o.mu.Unlock()

	// Give the forward a moment to establish before callers start dialing it.
	for i := 0; i < 20; i++ {
		if _, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/status", port)); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return port, nil
}

func (o *Orchestrator) ListNodes() ([]orchestrator.NodeRef, error) {
	n, err := o.replicas()
	if err != nil {
		return nil, err
	}
	refs := make([]orchestrator.NodeRef, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", o.StatefulSetName, i)
		out, err := o.run("kubectl", "get", "pod", id, "-n", o.Namespace, "-o", "jsonpath={.status.phase}")
		ready := err == nil && strings.TrimSpace(string(out)) == "Running"
		ref := orchestrator.NodeRef{ID: id, Ready: ready}
		if ready {
			if port, err := o.ensurePortForward(id); err == nil {
				ref.Addr = fmt.Sprintf("127.0.0.1:%d", port)
			}
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// Close stops all port-forwards this orchestrator started.
func (o *Orchestrator) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, c := range o.portForwards {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
	o.portForwards = make(map[string]*exec.Cmd)
}

func (o *Orchestrator) KillNode(id string) error {
	_, err := o.run("kubectl", "delete", "pod", id, "-n", o.Namespace, "--grace-period=0", "--force")
	return err
}

// networkPolicyName is deterministic per node so HealNode can find and remove
// exactly the policy IsolateNode created.
func (o *Orchestrator) networkPolicyName(id string) string { return "lab-isolate-" + id }

// IsolateNode applies a deny-all NetworkPolicy scoped to this specific pod.
// StatefulSet pods share the same "app" label, so we match on the label
// Kubernetes automatically attaches to every StatefulSet pod:
// statefulset.kubernetes.io/pod-name. Requires a CNI that enforces
// NetworkPolicy (kind's default kindnet does not — see README).
func (o *Orchestrator) IsolateNode(id string) error {
	policy := fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
spec:
  podSelector:
    matchLabels:
      statefulset.kubernetes.io/pod-name: %s
  policyTypes: [Ingress, Egress]
`, o.networkPolicyName(id), o.Namespace, id)

	c := o.cmd()("kubectl", "apply", "-f", "-")
	c.Dir = o.Dir
	c.Stdin = strings.NewReader(policy)
	out, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply (isolate %s): %w (%s)", id, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (o *Orchestrator) HealNode(id string) error {
	_, err := o.run("kubectl", "delete", "networkpolicy", o.networkPolicyName(id), "-n", o.Namespace, "--ignore-not-found")
	return err
}

func (o *Orchestrator) AddNode(id string) error {
	// id is unused: k8s-add-node.sh always scales to the next ordinal itself
	// (StatefulSet pods can't be added out of order), so the resulting pod's
	// name is not caller-chosen the way a Compose container's is.
	_, err := o.run("bash", "scripts/k8s-add-node.sh")
	return err
}

func (o *Orchestrator) RemoveNode(id string) error {
	// Similarly, k8s-remove-node.sh always removes the highest-ordinal pod.
	_, err := o.run("bash", "scripts/k8s-remove-node.sh")
	return err
}

func (o *Orchestrator) Logs(id string) (io.ReadCloser, error) {
	c := o.cmd()("kubectl", "logs", "-f", id, "-n", o.Namespace)
	c.Dir = o.Dir
	stdout, err := c.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	go func() { _ = c.Wait() }()
	return &procReadCloser{ReadCloser: stdout, cmd: c}, nil
}

type procReadCloser struct {
	io.ReadCloser
	cmd *exec.Cmd
}

func (p *procReadCloser) Close() error {
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	return p.ReadCloser.Close()
}
