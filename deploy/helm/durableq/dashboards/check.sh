#!/bin/sh
# Asserts every metric referenced by the dashboard is in the contract (docs/kubernetes-plan.md §3)
# or is a known kube-state-metrics series. Run from anywhere.
set -eu
dir=$(cd "$(dirname "$0")" && pwd)
json="$dir/durableq.json"

contract="
durableq_job_claims_total durableq_lease_expirations_total durableq_jobs_completed_total
durableq_jobs_failed_total durableq_jobs_retried_total durableq_jobs_dead_lettered_total
durableq_jobs_released_total durableq_lease_lost_total durableq_stale_transitions_total
durableq_job_duration_seconds durableq_queue_wait_seconds durableq_worker_active_jobs
durableq_worker_concurrency durableq_jobs_enqueued_total durableq_http_requests_total
durableq_jobs_pending durableq_jobs_scheduled durableq_jobs_running durableq_jobs_lease_expired
durableq_jobs_dead
kube_deployment_spec_replicas kube_deployment_status_replicas_available
kube_horizontalpodautoscaler_status_desired_replicas
"

# strip histogram suffixes, dedupe
used=$(grep -oE '\b(durableq|kube)_[a-z_]+' "$json" | sed -E 's/_(bucket|sum|count)$//' | sort -u)
bad=0
for m in $used; do
  case " $(echo $contract) " in *" $m "*) ;; *) echo "NOT IN CONTRACT: $m"; bad=1 ;; esac
done
[ "$bad" = 0 ] && echo "ok: $(echo "$used" | wc -l | tr -d ' ') metrics, all in contract"
exit $bad
