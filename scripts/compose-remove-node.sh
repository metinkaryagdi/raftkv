#!/usr/bin/env bash
# Removes a dynamically-added node from a running docker-compose cluster.
# Order matters: the membership change must commit BEFORE the container stops,
# or the leader keeps trying (and failing) to replicate to a peer that's about
# to disappear.
#
#   scripts/compose-remove-node.sh n6
#
set -euo pipefail
cd "$(dirname "$0")/.."

ID="${1:?usage: compose-remove-node.sh <id> (e.g. n6)}"
HTTP_PORT=8001

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

echo "waiting for a leader..."
leader_port=""
for _ in $(seq 1 30); do
  leader_port="$(find_leader_port || true)"
  [ -n "$leader_port" ] && break
  sleep 1
done
if [ -z "$leader_port" ]; then
  echo "no leader found among the compose nodes" >&2
  exit 1
fi

echo "removing $ID from the cluster configuration via the leader on host port $leader_port..."
curl -s -m5 -X POST "http://127.0.0.1:$leader_port/cluster/remove-server" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$ID\"}"
echo ""

echo "stopping and removing container raftkv-$ID..."
docker rm -f "raftkv-$ID" >/dev/null
echo "done."
