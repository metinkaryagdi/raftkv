#!/usr/bin/env bash
# Live failover demo on Kubernetes: finds the current leader pod, writes a value
# through it, deletes that pod (the StatefulSet controller recreates it), and
# shows the cluster elect a new leader and keep serving the pre-crash write.
set -euo pipefail
cd "$(dirname "$0")/.."

step() { echo -e "\n=== $1 ==="; }
pf_pid=""
cleanup() { [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true; }
trap cleanup EXIT

port_forward() {
  cleanup
  kubectl port-forward "pod/$1" 18080:8001 >/tmp/raftkv-pf.log 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 30); do
    curl -s -m1 http://127.0.0.1:18080/status >/dev/null 2>&1 && return 0
    sleep 0.3
  done
  echo "  (port-forward to $1 never became ready, skipping)" >&2
  return 1
}

find_leader() {
  for i in 0 1 2 3 4; do
    pod="raftkv-$i"
    kubectl get pod "$pod" >/dev/null 2>&1 || continue
    port_forward "$pod" || continue
    role=$(curl -s -m2 http://127.0.0.1:18080/status | tr -d '\0' | grep -o '"role":"[A-Za-z]*"' | cut -d'"' -f4)
    if [ "$role" = "Leader" ]; then
      echo "$pod"
      return 0
    fi
  done
  return 1
}

step "Waiting for a leader among the 5 pods"
leader=""
for _ in $(seq 1 30); do
  leader=$(find_leader || true)
  [ -n "$leader" ] && break
  sleep 1
done
[ -z "$leader" ] && { echo "no leader found"; exit 1; }
echo "  leader = $leader"

step "Writing city=istanbul via $leader"
port_forward "$leader"
curl -s -m3 -X PUT http://127.0.0.1:18080/kv/city -d istanbul; echo
curl -s -m3 http://127.0.0.1:18080/kv/city; echo

step "DELETING leader pod $leader (StatefulSet will recreate it)"
kubectl delete pod "$leader" --grace-period=0 --force >/dev/null 2>&1
cleanup # the deleted pod's port-forward is now dead; drop it before reusing the port

step "Waiting for the cluster to elect a NEW leader"
newleader=""
for _ in $(seq 1 30); do
  newleader=$(find_leader || true)
  [ -n "$newleader" ] && [ "$newleader" != "$leader" ] && break
  newleader=""
  sleep 1
done
[ -z "$newleader" ] && { echo "no new leader elected"; exit 1; }
echo "  new leader = $newleader"

step "Pre-crash write survived, and a new write commits"
port_forward "$newleader"
curl -s -m3 http://127.0.0.1:18080/kv/city; echo
curl -s -m3 -X PUT http://127.0.0.1:18080/kv/lang -d go; echo
curl -s -m3 http://127.0.0.1:18080/kv/lang; echo

step "Pods after failover"
kubectl get pods -l app=raftkv

echo -e "\nDemo complete: the StatefulSet recreated the killed pod and the cluster kept serving."
