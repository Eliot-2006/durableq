# Kubernetes chaos and measurements

This suite owns the demo queue in namespace `durableq` on context
`kind-durableq`: **it truncates jobs, job_attempts and effects**. Do not run
against a shared workload. Only one invocation may run at a time (local lock).
No product code or queue semantics are changed; no dependencies are added.
Seeds reproduce harness progress thresholds and target choices, not handler
jitter or Kubernetes scheduling.

Implementation plan: share kubectl/psql helpers, reset with workers stopped,
exercise each fault at seeded terminal-job progress thresholds, verify actual
fault coverage, drain, then parse the existing CLI's I1–I7 checks. Add benchmark
measurements to the same helpers, preserve raw samples and environment metadata,
run both suites on kind, run local integration checks, and commit the evidence.

```sh
make k8s-chaos
# Every scenario is independently selectable; report paths are relative to repo root.
go test -tags k8s -timeout 60m -count=1 -v ./test/k8s/ \
  -run RandomPodKill -args -seed 42 -report bench/k8s/chaos/manual.json
make k8s-bench
```

`K8S_SEED` overrides the script seed. `k8s-chaos.sh` runs all nine scenarios once;
run again with a different seed for repetition. `k8s-bench.sh` runs the 1/2/5/10
replica × 0/50ms matrix, SIGKILL recovery, and KEDA reaction measurements.
Commands always specify the context, namespace and container; distroless execs
invoke `/durableq` directly. Failed runs still write JSON. The local lock is
removed on ordinary exit; after SIGKILL of the harness, remove
`/tmp/durableq-k8s.lock` only after checking that no other harness is running.

KEDA's `autoscaling.keda.sh/paused-replicas` annotation fixes replicas for chaos
and benchmarks. The original annotation and deployment replica count are restored
on cleanup. KEDA is unpaused for the autoscale scenario; it is explicitly skipped
if its ScaledObject is absent. Workers are stopped before TRUNCATE, so old handlers
cannot write into a new run. Chaos workloads have an initial delay; their drain time is observed from enqueue
completion. Benchmark batches use a single due-time gate after all jobs are
enqueued; benchmark throughput timing begins at that database due time.

A real SIGKILL resolves a selected *active* worker pod's container ID and kind
node, obtains its PID with `crictl inspect`, validates the container labels and
process arguments, then runs `docker exec NODE kill -KILL PID`. Exit code 137 is
verified before counting the fault. Force pod deletion is not used: kubelet can
still send SIGTERM with a minimum grace period. Graceful deletion is a separate
scenario using the normal pod grace period. Crash cycles wait for affected jobs
to have a subsequent claim before the next cycle.

Reports distinguish abandoned attempts and distinct jobs with abandoned attempts
followed by another claim (duplicate executions) from duplicate effects (I2).
Healthy workloads must all succeed; DLQ workloads must all die without effects.
Permanent payloads fail immediately by handler design; retryable payloads sleep
before failing. Graceful termination uses jobs longer than worker.grace and an
initial budget of one to exercise budget-preserving release.

Benchmark methodology:

- Fixed chart concurrency and maxConns; autoscaling paused at each replica count.
  One batch up front, fixed JSON payload (recorded in each report). Jobs are
  initially scheduled 24h out; after enqueue, a single test-only UPDATE gives
  unclaimed jobs the same run_at, three seconds in the future. The existing
  demo handler jitters its configured sleep by ±50%; 50ms is the fixed *configured*
  duration, not an exact sleep. The 0ms run still commits a transactional effect.
- Throughput = terminal jobs / (last jobs.finished_at − earliest jobs.run_at),
  measured with the DB clock; excludes enqueue and initial scheduling delay.
- Processing percentiles use finished attempts' finished_at − claimed_at.
  Queue wait uses first attempts' claimed_at − jobs.run_at (the initial due time;
  retries can change run_at, so clean throughput runs assert zero retries).
- Every ~2s sample records pg_stat_activity connections, raw `kubectl top pods`,
  and the underlying timestamped PodMetrics API. Sampling continues until all
  worker/postgres metric timestamps pass the DB drain time. CPU peaks use only
  averaging windows that start within this workload; memory peaks use points
  timestamped during it. This excludes setup/previous-batch CPU carryover and
  retains the final loaded window published after drain. Final CPU windows can
  include idle time after drain; these remain observed averages, not instantaneous
  maxima. Coverage counts and raw timestamps/windows are retained, and every
  worker/postgres CPU/memory category must have a relevant sample. The matrix uses
  60,000 jobs at 0ms and 40,000 at 50ms to outlast metrics-server windows.
- Recovery snapshots an active claim to identify the killed process and uses
  the node's container finishedAt timestamp as kill time. Its attempt history
  selects every claim current at that time, including claims made after the
  snapshot. Closed attempts and attempts reclaimed before the kill are excluded.
  Every remaining abandoned claim must be reclaimed; its next claimed_at gives
  recovery latency. The chart's lease and heartbeat are retained and recorded.
- KEDA samples desired/ready replicas every ~2s, measures first scale-up and
  max from enqueue completion, and scale-down from DB drain. Observed timings
  include polling and HPA stabilization, not only cooldownPeriod. Raw timestamped
  samples are retained. Max must be reached and min restored for a pass.

`env.txt` records host CPU, Docker VM budget, versions, chart values, pod image IDs,
git SHA and working-tree status. JSON is authoritative; CSV is the compact matrix.
No extrapolation or comparison to bare-process benchmarks is made.
