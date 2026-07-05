#!/usr/bin/env bash
# Launches a local 5-node raftkv cluster as background processes.
# Raft RPCs use gRPC on ports 9001-9005; the client HTTP API is on 8001-8005.
# Logs go to ./clusterlogs/nN.log. Stop with: kill $(cat clusterlogs/pids)
set -euo pipefail
cd "$(dirname "$0")/.."

NODES="${1:-5}"
mkdir -p clusterlogs
: > clusterlogs/pids

if [ ! -x bin/raftkv.exe ] && [ ! -x bin/raftkv ]; then
  echo "Building raftkv..."
  go build -o bin/raftkv ./cmd/raftkv
fi
BIN="bin/raftkv"; [ -x bin/raftkv.exe ] && BIN="bin/raftkv.exe"

peers=""
for i in $(seq 1 "$NODES"); do
  [ -n "$peers" ] && peers="$peers,"
  peers="${peers}n${i}=127.0.0.1:$((9000 + i))"
done

echo "Starting ${NODES}-node cluster (peers: $peers)"
for i in $(seq 1 "$NODES"); do
  "$BIN" --id "n${i}" --peers "$peers" --http-addr "127.0.0.1:$((8000 + i))" \
    > "clusterlogs/n${i}.log" 2>&1 &
  echo $! >> clusterlogs/pids
  echo "  n${i} -> raft 127.0.0.1:$((9000 + i))  http 127.0.0.1:$((8000 + i))"
done

echo ""
echo "Cluster up. Try:  curl http://127.0.0.1:8001/status"
echo "Stop with:        kill \$(cat clusterlogs/pids)"
