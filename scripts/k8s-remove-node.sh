#!/usr/bin/env bash
# Removes the highest-ordinal pod from the raftkv StatefulSet — the only way a
# StatefulSet ever scales down. Order matters: the membership change must
# commit BEFORE the pod is scaled away, or the leader keeps trying (and
# failing) to replicate to a peer that's about to disappear.
#
#   scripts/k8s-remove-node.sh
#
set -euo pipefail

current=$(kubectl get statefulset/raftkv -o jsonpath='{.spec.replicas}')
if [ "$current" -le 1 ]; then
  echo "refusing to scale below 1 replica" >&2
  exit 1
fi
target_pod="raftkv-$((current - 1))"
new=$((current - 1))

pf_pid=""
cleanup() { [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true; }
trap cleanup EXIT

port_forward() {
  cleanup
  kubectl port-forward "pod/$1" 18080:8001 >/tmp/k8s-remove-node-pf.log 2>&1 &
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
    [ "$pod" = "$target_pod" ] && continue
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

echo "finding the current leader..."
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

echo "removing $target_pod from the cluster configuration via $leader..."
port_forward "$leader"
curl -s -m5 -X POST http://127.0.0.1:18080/cluster/remove-server \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$target_pod\"}"
echo ""
cleanup

echo "scaling raftkv: $current -> $new replicas"
kubectl scale statefulset/raftkv --replicas="$new"
echo "done."
