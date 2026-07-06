// Package compose implements orchestrator.Orchestrator against a running
// docker-compose deployment (see docker-compose.yml, scripts/compose-add-
// node.sh, scripts/compose-remove-node.sh at the repo root).
package compose

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"

	"github.com/metinkaryagdi/raftkv/internal/orchestrator"
)

// Orchestrator drives a docker-compose deployment via the docker CLI.
type Orchestrator struct {
	// Dir is the repo root containing docker-compose.yml and scripts/.
	Dir string
	// GenesisIDs are the static compose service names (e.g. n1..n5). Any other
	// id is assumed to have been dynamically added via
	// scripts/compose-add-node.sh, whose container naming convention is
	// "raftkv-<id>".
	GenesisIDs []string

	// execCommand is overridable in tests so command construction can be
	// asserted without actually invoking docker/bash.
	execCommand func(name string, args ...string) *exec.Cmd

	mu        sync.Mutex
	nameCache map[string]string // genesis service id -> resolved container name
}

// New returns a compose Orchestrator rooted at dir.
func New(dir string, genesisIDs []string) *Orchestrator {
	return &Orchestrator{Dir: dir, GenesisIDs: genesisIDs, execCommand: exec.Command}
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

// containerName maps a logical node id to its actual Docker container name.
// Dynamically-added nodes follow compose-add-node.sh's literal "raftkv-<id>"
// convention (docker run --name), so no lookup is needed. Genesis nodes do
// need one: Compose does NOT name a container after its bare service name
// ("n2") — the real name is "<project>-<service>-<replica>" (e.g.
// "raft_konsenss-n2-1"), a normalization of the project directory name that
// varies per checkout and must never be assumed. Found the hard way: an
// earlier version of this code assumed the bare id worked, and `docker logs
// n2`/`docker kill n2` silently failed with "no such container" against a
// real running cluster.
func (o *Orchestrator) containerName(id string) (string, error) {
	isGenesis := false
	for _, g := range o.GenesisIDs {
		if g == id {
			isGenesis = true
			break
		}
	}
	if !isGenesis {
		return "raftkv-" + id, nil
	}

	o.mu.Lock()
	if o.nameCache == nil {
		o.nameCache = make(map[string]string)
	}
	if name, ok := o.nameCache[id]; ok {
		o.mu.Unlock()
		return name, nil
	}
	o.mu.Unlock()

	out, err := o.run("docker", "compose", "ps", id, "--format", "{{.Name}}")
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf("compose: service %q is not running", id)
	}
	o.mu.Lock()
	o.nameCache[id] = name
	o.mu.Unlock()
	return name, nil
}

func (o *Orchestrator) discoverNetwork() (string, error) {
	if len(o.GenesisIDs) == 0 {
		return "", fmt.Errorf("compose: no genesis node ids configured")
	}
	out, err := o.run("docker", "compose", "ps", "-q", o.GenesisIDs[0])
	if err != nil {
		return "", err
	}
	containerID := strings.TrimSpace(string(out))
	if containerID == "" {
		return "", fmt.Errorf("compose: service %q is not running", o.GenesisIDs[0])
	}
	out, err = o.run("docker", "inspect", containerID,
		"--format", `{{range $net, $cfg := .NetworkSettings.Networks}}{{$net}}{{end}}`)
	if err != nil {
		return "", err
	}
	network := strings.TrimSpace(string(out))
	if network == "" {
		return "", fmt.Errorf("compose: could not determine network for %s", containerID)
	}
	return network, nil
}

func (o *Orchestrator) ListNodes() ([]orchestrator.NodeRef, error) {
	var refs []orchestrator.NodeRef
	for _, id := range o.GenesisIDs {
		out, err := o.run("docker", "compose", "port", id, "8001")
		if err != nil {
			refs = append(refs, orchestrator.NodeRef{ID: id, Ready: false})
			continue
		}
		addr := strings.TrimSpace(string(out))
		_, port, ok := strings.Cut(addr, ":")
		if !ok {
			refs = append(refs, orchestrator.NodeRef{ID: id, Ready: false})
			continue
		}
		refs = append(refs, orchestrator.NodeRef{ID: id, Addr: "127.0.0.1:" + port, Ready: true})
	}
	// Filter dynamically-added nodes by NETWORK membership, not just a
	// "raftkv-*" name match: a name-only filter can accidentally pick up
	// unrelated containers on the host that happen to share the prefix (e.g.
	// a kind Kubernetes cluster's own "raftkv-control-plane" container) —
	// found by testing this against a real machine already running one.
	if network, err := o.discoverNetwork(); err == nil {
		out, err := o.run("docker", "ps",
			"--filter", "network="+network,
			"--filter", "name=^raftkv-",
			"--format", "{{.Names}}")
		if err == nil {
			for _, name := range strings.Fields(string(out)) {
				id := strings.TrimPrefix(name, "raftkv-")
				// Dynamically-added nodes have no published host port in this demo
				// (see scripts/compose-add-node.sh); the lab reaches them only
				// indirectly, through the genesis nodes' view of the cluster.
				refs = append(refs, orchestrator.NodeRef{ID: id, Ready: true})
			}
		}
	}
	return refs, nil
}

func (o *Orchestrator) KillNode(id string) error {
	name, err := o.containerName(id)
	if err != nil {
		return err
	}
	_, err = o.run("docker", "kill", name)
	return err
}

func (o *Orchestrator) IsolateNode(id string) error {
	network, err := o.discoverNetwork()
	if err != nil {
		return err
	}
	name, err := o.containerName(id)
	if err != nil {
		return err
	}
	_, err = o.run("docker", "network", "disconnect", network, name)
	return err
}

func (o *Orchestrator) HealNode(id string) error {
	network, err := o.discoverNetwork()
	if err != nil {
		return err
	}
	name, err := o.containerName(id)
	if err != nil {
		return err
	}
	_, err = o.run("docker", "network", "connect", network, name)
	return err
}

func (o *Orchestrator) AddNode(id string) error {
	_, err := o.run("bash", "scripts/compose-add-node.sh", id)
	return err
}

func (o *Orchestrator) RemoveNode(id string) error {
	_, err := o.run("bash", "scripts/compose-remove-node.sh", id)
	return err
}

// procReadCloser kills the underlying process when closed, so the caller
// doesn't leak a `docker logs -f` subprocess just by walking away.
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

func (o *Orchestrator) Logs(id string) (io.ReadCloser, error) {
	name, err := o.containerName(id)
	if err != nil {
		return nil, err
	}
	c := o.cmd()("docker", "logs", "-f", name)
	c.Dir = o.Dir
	pr, pw := io.Pipe()
	c.Stdout = pw
	c.Stderr = pw // the container's stderr is where stderrLogger writes Events
	if err := c.Start(); err != nil {
		_ = pw.Close()
		return nil, err
	}
	go func() {
		_ = c.Wait()
		_ = pw.Close()
	}()
	return &procReadCloser{ReadCloser: pr, cmd: c}, nil
}
