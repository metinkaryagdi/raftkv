#!/usr/bin/env bash
# Builds and runs the raftkv "lab" — the observability/control-plane dashboard
# for a REAL running Docker-Compose or Kubernetes deployment (not a
# simulation): live cluster state, and buttons that actually kill/isolate/
# heal/add/remove containers or pods.
#
#   scripts/run-lab.sh --target=compose   # against `docker compose up -d`
#   scripts/run-lab.sh --target=k8s       # against a deployed StatefulSet
#
# The target deployment must already be running (docker compose up -d / the
# k8s manifests applied) — the lab only observes and controls it, it does not
# start it.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="compose"
for arg in "$@"; do
  case "$arg" in
    --target=*) TARGET="${arg#--target=}" ;;
    *) echo "unknown argument: $arg" >&2; exit 1 ;;
  esac
done

echo "Building raftlab..."
go build -o bin/raftlab.exe ./cmd/raftlab 2>/dev/null || go build -o bin/raftlab ./cmd/raftlab
BIN="bin/raftlab"; [ -x bin/raftlab.exe ] && BIN="bin/raftlab.exe"

echo "Starting the lab against target=$TARGET on http://127.0.0.1:7070 ..."
exec "$BIN" --target="$TARGET" --dir="."
