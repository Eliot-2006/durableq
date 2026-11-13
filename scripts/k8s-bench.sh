#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
stamp=$(date -u +%Y%m%dT%H%M%SZ)
result="bench/k8s/results/$stamp"
go test -tags k8s -timeout 60m -count=1 -v ./test/k8s/ -run 'TestBench' -args -seed "${K8S_SEED:-42}" -report "$result/report.json"
echo "results: $result"
