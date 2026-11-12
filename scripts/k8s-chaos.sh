#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
stamp=$(date -u +%Y%m%dT%H%M%SZ)
report="bench/k8s/chaos/$stamp.json"
go test -tags k8s -timeout 60m -count=1 -v ./test/k8s/ -run 'Test(RandomPodKill|GracefulTermination|RollingUpdate|ScaleUpDown|PostgresRestart|ConnectionKill|RetryExhaustionDLQ|RepeatedCrashRecovery|KEDAAutoscale)$' -args -seed "${K8S_SEED:-42}" -report "$report"
echo "report: $report"
