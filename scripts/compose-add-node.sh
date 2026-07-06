#!/usr/bin/env bash
# Dynamically adds a new node to an already-running docker-compose cluster
# (see docker-compose.yml), demonstrating live cluster membership changes: the
# new container starts with -join (no prior peer knowledge) and is admitted via
# POST /cluster/add-server on whichever node currently answers as leader.
#
#   docker compose up -d          # if not already running
#   scripts/compose-add-node.sh n6
#
set -euo pipefail
cd "$(dirname "$0")/.."

ID="${1:?usage: compose-add-node.sh <id> (e.g. n6)}"
RAFT_PORT=9001
HTTP_PORT=8001
IMAGE="raftkv:latest"

# Discover the actual Docker network Compose created rather than hardcode it:
# its name is normalized from the project directory name (e.g.
# "raft_konsenss_raft" for a folder with non-ASCII characters) and would be
# wrong to assume.
existing="$(docker compose ps -q n1)"
if [ -z "$existing" ]; then
  echo "no running compose cluster found (expected service 'n1' to be up — run 'docker compose up -d' first)" >&2
  exit 1
fi
network="$(docker inspect "$existing" --format '{{range $net, $cfg := .NetworkSettings.Networks}}{{$net}}{{end}}')"
echo "discovered network: $network"

echo "starting container raftkv-$ID (join mode, no prior peer knowledge)..."
docker run -d --name "raftkv-$ID" --network "$network" \
  -e RAFTKV_ID="$ID" \
  -e RAFTKV_JOIN=1 \
  -e RAFTKV_RAFT_ADDR="0.0.0.0:$RAFT_PORT" \
  -e RAFTKV_HTTP_ADDR="0.0.0.0:$HTTP_PORT" \
  "$IMAGE" >/dev/null

# Find the current leader by probing each known compose node's published host
# port (the same 421-redirect-retry pattern scripts/demo-failover.ps1 uses).
find_leader_port() {
  for svc in n1 n2 n3 n4 n5; do
    port="$(docker compose port "$svc" "$HTTP_PORT" 2>/dev/null | cut -d: -f2)"
    [ -z "$port" ] && continue
    role="$(curl -s -m2 "http://127.0.0.1:$port/status" 2>/dev/null | grep -o '"role":"[A-Za-z]*"' | cut -d'"' -f4)"
    if [ "$role" = "Leader" ]; then
      echo "$port"
      return 0
    fi
  done
  return 1
}

echo "waiting for a leader among the existing nodes..."
leader_port=""
for _ in $(seq 1 30); do
  leader_port="$(find_leader_port || true)"
  [ -n "$leader_port" ] && break
  sleep 1
done
if [ -z "$leader_port" ]; then
  echo "no leader found among the existing compose nodes" >&2
  exit 1
fi

new_addr="raftkv-$ID:$RAFT_PORT" # resolvable within the shared network before the container even starts
echo "adding $ID ($new_addr) via the leader on host port $leader_port..."
curl -s -m5 -X POST "http://127.0.0.1:$leader_port/cluster/add-server" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$ID\",\"raftAddr\":\"$new_addr\"}"
echo ""
echo "done. raftkv-$ID has no published host port (it isn't a client-facing node in"
echo "this demo); inspect it with: docker logs raftkv-$ID"
