#!/usr/bin/env bash
# Manual acceptance script for the lab: brings up a real docker-compose
# cluster, starts the lab against it, and exercises its control API end to
# end — proving the dashboard's buttons actually affect real containers, not
# a simulation. This is the lab's equivalent of demo-failover.ps1/.sh: run
# once against a live environment to confirm everything works, not part of
# `go test` (real docker/orchestrator behavior isn't practical to verify
# there — see internal/orchestrator's own test files for what IS covered by
# `go test`: command construction, via an injected exec spy).
set -euo pipefail
cd "$(dirname "$0")/.."

step() { echo -e "\n=== $1 ==="; }

step "Building raftkv and bringing up a 5-node compose cluster"
DOCKER_BUILDKIT=0 docker build -t raftkv:latest . >/dev/null
docker compose up -d --no-build >/dev/null
sleep 5

step "Building and starting the lab (target=compose) on :7070"
go build -o bin/raftlab.exe ./cmd/raftlab 2>/dev/null || go build -o bin/raftlab ./cmd/raftlab
BIN="bin/raftlab"; [ -x bin/raftlab.exe ] && BIN="bin/raftlab.exe"
"$BIN" --target=compose --dir=. --listen=:7070 >/tmp/raftlab-demo.log 2>&1 &
lab_pid=$!
trap 'kill $lab_pid 2>/dev/null; docker compose down >/dev/null 2>&1' EXIT
sleep 2

step "GET /api/nodes lists all 5 real containers"
curl -s -m5 http://127.0.0.1:7070/api/nodes
echo ""

step "Proxying one node's real /status through the lab"
curl -s -m5 http://127.0.0.1:7070/api/nodes/n1/status
echo ""

step "Writing a value directly to whichever node is leader, then re-reading via the lab's log proxy"
for p in 8001 8002 8003 8004 8005; do
  role=$(curl -s -m2 "http://127.0.0.1:$p/status" | grep -o '"role":"[A-Za-z]*"' | cut -d'"' -f4)
  if [ "$role" = "Leader" ]; then leader_port=$p; fi
done
curl -s -m5 -X PUT "http://127.0.0.1:$leader_port/kv/demo" -d lab-works >/dev/null
sleep 1
curl -s -m5 http://127.0.0.1:7070/api/nodes/n1/log
echo ""

step "Killing n2's real container via the lab's orchestrator API"
curl -s -m5 -X POST http://127.0.0.1:7070/api/orchestrator/kill/n2
echo ""
sleep 2
docker ps --filter "name=raft_konsenss-n2" --format '{{.Names}}: {{.Status}}'

echo -e "\nDemo complete: the lab observed and controlled a real cluster end to end."
