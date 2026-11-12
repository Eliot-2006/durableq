#!/usr/bin/env bash
# Idempotent: create the kind cluster, install addons, build+load the image, deploy the chart.
# DEPLOY_ONLY=1 skips cluster/addons and just rebuilds, reloads, and rolls the chart.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER=durableq
NS=durableq
# Pinned chart versions (looked up with `helm search repo` on 2026-11-12).
METRICS_SERVER_VERSION=3.14.0
KEDA_VERSION=2.21.0
KPS_VERSION=93.2.2

kubectl_ctx() { kubectl --context "kind-$CLUSTER" "$@"; }
helm_ctx() { helm --kube-context "kind-$CLUSTER" "$@"; }

if [ -z "${DEPLOY_ONLY:-}" ]; then
  if ! kind get clusters | grep -qx "$CLUSTER"; then
    kind create cluster --config deploy/kind/cluster.yaml --wait 120s
  fi

  helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ >/dev/null 2>&1 || true
  helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo update >/dev/null

  helm_ctx upgrade --install metrics-server metrics-server/metrics-server --version "$METRICS_SERVER_VERSION" \
    -n kube-system --set 'args={--kubelet-insecure-tls}' --wait
  helm_ctx upgrade --install keda kedacore/keda --version "$KEDA_VERSION" \
    -n keda --create-namespace --wait
  helm_ctx upgrade --install monitoring prometheus-community/kube-prometheus-stack --version "$KPS_VERSION" \
    -n monitoring --create-namespace -f deploy/kind/monitoring-values.yaml --wait --timeout 10m
fi

docker build -t durableq:dev .
kind load docker-image durableq:dev --name "$CLUSTER"

helm_ctx upgrade --install durableq deploy/helm/durableq -n "$NS" --create-namespace \
  -f deploy/helm/durableq/values-kind.yaml --wait --timeout 5m

# Same tag (dev) on rebuild: force pods to pick up the freshly loaded image.
if [ -n "${DEPLOY_ONLY:-}" ]; then
  kubectl_ctx -n "$NS" rollout restart deploy/durableq-api deploy/durableq-worker
  kubectl_ctx -n "$NS" rollout status deploy/durableq-api deploy/durableq-worker --timeout=5m
fi

kubectl_ctx -n "$NS" get pods,scaledobject,podmonitor
