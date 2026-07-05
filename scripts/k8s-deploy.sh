#!/usr/bin/env bash
# Builds the raftkv image, loads it into a local kind cluster, and deploys the
# 5-pod StatefulSet. Requires: kind, kubectl, a running kind cluster (create one
# with `kind create cluster --name raftkv` if needed).
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${1:-raftkv}"

echo "Building raftkv:latest..."
DOCKER_BUILDKIT=0 docker build -t raftkv:latest .

echo "Loading image into kind cluster '$CLUSTER'..."
kind load docker-image raftkv:latest --name "$CLUSTER"

echo "Applying manifests..."
kubectl apply -f deploy/k8s/raftkv.yaml

echo "Waiting for pods to be ready..."
kubectl rollout status statefulset/raftkv --timeout=90s

echo ""
echo "Pods:"
kubectl get pods -l app=raftkv -o wide

echo ""
echo "Try:"
echo "  kubectl port-forward pod/raftkv-0 8001:8001 &"
echo "  curl http://127.0.0.1:8001/status"
