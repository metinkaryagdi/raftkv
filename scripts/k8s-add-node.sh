#!/usr/bin/env bash
# Scales the raftkv StatefulSet up by one pod and admits it into the running
# Raft cluster via POST /cluster/add-server.
#
#   scripts/k8s-add-node.sh
#
set -euo pipefail

current=$(kubectl get statefulset/raftkv -o jsonpath='{.spec.replicas}')
new=$((current + 1))
new_pod="raftkv-$current" # StatefulSet pods are raftkv-0..raftkv-(N-1); the new one takes the next ordinal

echo "scaling raftkv: $current -> $new replicas"

# Bump RAFTKV_REPLICAS in the template FIRST (before scaling): the new pod's
# own k8sPeersFromEnv (cmd/raftkv/main.go) then generates a peer list covering
# every ordinal < RAFTKV_REPLICAS, so it boots already knowing every existing
# member's address. This does not restart already-running pods — StatefulSets
# only apply template changes to newly (re)created pods.
kubectl set env statefulset/raftkv RAFTKV_REPLICAS="$new"
kubectl scale statefulset/raftkv --replicas="$new"

echo "waiting for $new_pod to be Ready..."
kubectl wait --for=condition=Ready "pod/$new_pod" --timeout=90s

pf_pid=""
cleanup() { [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true; }
trap cleanup EXIT

port_forward() {
  cleanup
  kubectl port-forward "pod/$1" 18080:8001 >/tmp/k8s-add-node-pf.log 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 30); do
    curl -s -m1 http://127.0.0.1:18080/status >/dev/null 2>&1 && return 0
    sleep 0.3
  done
  return 1
}

find_leader() {
  for i in $(seq 0 $((current - 1))); do
    pod="raftkv-$i"
    kubectl get pod "$pod" >/dev/null 2>&1 || continue
    port_forward "$pod" || continue
    role=$(curl -s -m2 http://127.0.0.1:18080/status | grep -o '"role":"[A-Za-z]*"' | cut -d'"' -f4)
    if [ "$role" = "Leader" ]; then
      echo "$pod"
      return 0
    fi
  done
  return 1
}

echo "finding the current leader among the existing $current pods..."
leader=""
for _ in $(seq 1 30); do
  leader=$(find_leader || true)
  [ -n "$leader" ] && break
  sleep 1
done
if [ -z "$leader" ]; then
  echo "no leader found" >&2
  exit 1
fi
echo "  leader = $leader"

new_addr="$new_pod.raftkv-headless:9001" # deterministic StatefulSet DNS, computable before the pod even existed
echo "adding $new_pod ($new_addr) via $leader..."
port_forward "$leader"
curl -s -m5 -X POST http://127.0.0.1:18080/cluster/add-server \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$new_pod\",\"raftAddr\":\"$new_addr\"}"
echo ""
echo "done."
