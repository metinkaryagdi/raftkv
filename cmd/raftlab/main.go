// Command raftlab runs the observability/control-plane dashboard for a real
// running raftkv deployment (Docker Compose or Kubernetes) — the "lab": it
// shows live cluster state and lets an operator inject faults (kill/isolate/
// heal/add/remove a node) against actual containers/pods, not a simulation.
package main

import (
	"embed"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"strings"

	"github.com/metinkaryagdi/raftkv/internal/lab"
	"github.com/metinkaryagdi/raftkv/internal/orchestrator"
	"github.com/metinkaryagdi/raftkv/internal/orchestrator/compose"
	"github.com/metinkaryagdi/raftkv/internal/orchestrator/k8s"
)

//go:embed web
var webFS embed.FS

func main() {
	var (
		target      = flag.String("target", "compose", `deployment target: "compose" or "k8s"`)
		listen      = flag.String("listen", ":7070", "address for the lab's HTTP+WS server")
		dir         = flag.String("dir", ".", "repo root (contains docker-compose.yml / deploy/k8s / scripts/)")
		namespace   = flag.String("namespace", "default", "kubernetes namespace (k8s target only)")
		statefulSet = flag.String("statefulset", "raftkv", "kubernetes StatefulSet name (k8s target only)")
		service     = flag.String("service", "raftkv-headless", "kubernetes headless service name (k8s target only)")
		genesisIDs  = flag.String("genesis-ids", "n1,n2,n3,n4,n5", "comma-separated compose service names (compose target only)")
	)
	flag.Parse()

	var orch orchestrator.Orchestrator
	switch *target {
	case "compose":
		orch = compose.New(*dir, strings.Split(*genesisIDs, ","))
	case "k8s":
		orch = k8s.New(*dir, *namespace, *statefulSet, *service)
	default:
		log.Fatalf("unknown -target %q (want %q or %q)", *target, "compose", "k8s")
	}

	labServer := lab.NewServer(orch)
	defer labServer.Close()

	webRoot, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("embed web dir: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", labServer.Handler())
	mux.Handle("/ws/", labServer.Handler())
	mux.Handle("/", http.FileServerFS(webRoot))

	log.Printf("raftlab listening on %s (target=%s, dir=%s)", *listen, *target, *dir)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
