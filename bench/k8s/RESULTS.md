# DurableQ kind results — Task D

Measured on an Apple M3 Pro (11 CPU cores, 18 GiB host RAM), Docker VM
11 CPUs / 9,434,791,936 bytes, kind Kubernetes v1.37.0. Full environment,
chart values, image IDs and source SHA are in the benchmark run's `env.txt`.

No product Go code, chart settings, queue semantics or dependencies changed.
Worker configuration: concurrency 10, maxConns 6, lease 30s, heartbeat 10s,
shutdown grace 10s. KEDA uses min 3 / max 10, target 50, polling 15s,
cooldown 60s and HPA scale-down stabilization 120s. Fixed replica tests use
KEDA's pause annotation and restore it afterwards.

## Verification

Ran and passed:

- `go vet ./...`
- `go vet -tags k8s ./test/k8s/`
- `go build -tags k8s ./test/k8s`
- `DATABASE_URL=postgres://localhost:5432/durableq?sslmode=disable go test -race -count=1 -v ./...`
  — all five packages with tests passed; zero skips. Full output: [local-race.log](local-race.log).
- `DATABASE_URL=postgres://localhost:5432/durableq?sslmode=disable go test -race -tags k8s -count=1 -v ./test/k8s -run 'Test(ParseChecks|StatusDrain|ContainerTime|Resources|RecoveryExcludesCompletedSnapshot|KilledClaimsIncludesPostSnapshotClaims|ResourceWindowsExcludeOtherBatches|FreshWorkerWindowIncludesItsOwnIdleSetup)$'`
  — eight passed, zero skips. Full output: [harness-checks.log](harness-checks.log).
- `bash -n scripts/k8s-chaos.sh scripts/k8s-bench.sh`
- `git diff --check`

The independent read-only code review found the snapshot/kill completion race;
a real-Postgres regression reproduced it before the fix. A second regression
covers claims created after the pre-kill snapshot and excludes older attempts
already reclaimed before the kill.

## Failed harness trials (retained, not counted as passes)

- [initial-random-41.json](chaos/initial-random-41.json): this crictl uses RFC3339
  timestamps instead of integer nanoseconds; SIGKILL happened but reporting failed.
- [random-41.json](chaos/random-41.json): crictl's running-container zero timestamp
  was mistaken for a completed exit. This invalid trial includes a zero-time
  event and must not be used as fault/recovery evidence. The fix waits for
  `CONTAINER_EXITED` and verifies code 137 before counting.
- [20261112T233044Z.json](chaos/20261112T233044Z.json),
  [stdout](chaos/suite42-trial.log): first end-to-end run, **19m28s**, seven passes
  and two harness failures. Recovery waited for jobs that completed between
  snapshot and kill; KEDA completed 3→10→3 but PostgreSQL's `+00` timestamp text
  failed Go parsing before invariant checks. Neither failure is a product bug.
  Recovery now filters attempt history; DB timestamps now use JSON encoding.

- [20261112T235407Z.json](chaos/20261112T235407Z.json),
  [stdout](chaos/suite43.log): second end-to-end run, **16m10s**, eight passes.
  RepeatedCrashRecovery encountered a Ready worker between claims and failed
  before injecting any fault. The final helper waits for an active claim on
  each target rather than treating that ordinary idle interval as a failure.

The first executable was built before the recovery and timestamp fixes. Seed-42
recovery was rerun successfully using the corrected helper. Seed 43 used commit
`702cbd0`. The final full chaos suite uses `feb74c9`, which also collects
every current claim of the killed process instead of only the snapshot and waits
for active workers before crash injection. Workloads, faults and I1–I7 checks
remain the same. The timestamped benchmark trial uses `baa0062`. The final
confirmation uses `63c40cc`, with the scaling pause held across the matrix and
resource windows attributed to fresh worker pods separately from persistent
Postgres. Chaos behavior is unchanged.

## Measurement limits

These are real measurements on one laptop and one dev Postgres PVC. There is no
extrapolation. `demo.effect` has ±50% jitter around configured sleep, including
transactional effect work for the 0ms case. Each duration series uses an identical
payload across replica counts. Resource peaks are sampled about every two seconds. CPU uses timestamped
metrics-server averaging windows. Postgres windows begin within the workload;
fresh worker windows may begin during that case's setup, and must overlap the
workload. Final loaded windows published after drain are retained; memory uses
points timestamped during the workload. CPU windows can include idle edges, so
these are observed averages,
not instantaneous maxima. Queue
wait is first-claim time minus the common initial due time. Full methodology:
[test/k8s/README.md](../../test/k8s/README.md).

## Final chaos validation

`K8S_SEED=44 make k8s-chaos` — **ran and passed all nine in 19m40s**. Each scenario has at least two successful real runs. No skips, lost jobs or duplicate effects. All seven invariant violation counts are zero in every passing row.

Final report: [20261113T001148Z.json](chaos/20261113T001148Z.json); [stdout](chaos/suite44.log). First validated counterparts use seed 42, except KEDA (43).

| Scenario | Jobs | First drain (s) | Seed 44 drain (s) | First / final duplicate-execution jobs | Final actual faults |
|---|---:|---:|---:|---:|---|
| RandomPodKill | 1200 | 48.73 | 48.97 | 30 / 30 | sigkill=3 |
| GracefulTermination | 90 | 121.17 | 121.01 | 0 / 0 | sigterm_delete=1 |
| RollingUpdate | 2400 | 80.64 | 79.91 | 0 / 0 | rollout_restart=3 |
| ScaleUpDown | 4000 | 340.54 | 339.40 | 0 / 0 | scale=4 |
| PostgresRestart | 900 | 61.00 | 80.67 | 29 / 30 | postgres_restart=1 |
| ConnectionKill | 900 | 44.39 | 42.85 | 9 / 9 | connection_kill_round=3, connections_terminated=23 |
| RetryExhaustionDLQ | 240 | 39.28 | 39.64 | 10 / 10 | sigkill=1 |
| RepeatedCrashRecovery | 3600 | 160.54 | 164.99 | 90 / 88 | crash_recovery_cycle=3, sigkill=9 |
| KEDAAutoscale | 3000 | 79.70 | 73.48 | 0 / 0 | KEDA 3→10→3 |

Graceful deletion released 10 attempts in each validated run, and every job succeeded. DLQ runs ended with all 240 jobs dead and zero effects. Postgres job count was exactly 900 before and after each restart. Repeated crash recovery produced 90 abandoned attempts in seed 44 (88 distinct re-executed jobs).

Exact stdout summaries for two successful runs per scenario, with report paths:

```text
CHAOS RandomPodKill            seed=42 pass=true faults=map[sigkill:3] drain=48.73s duplicates=30 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS GracefulTermination      seed=42 pass=true faults=map[sigterm_delete:1] drain=121.17s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS RollingUpdate            seed=42 pass=true faults=map[rollout_restart:3] drain=80.64s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS ScaleUpDown              seed=42 pass=true faults=map[scale:4] drain=340.54s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS PostgresRestart          seed=42 pass=true faults=map[postgres_restart:1] drain=61.00s duplicates=29 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS ConnectionKill           seed=42 pass=true faults=map[connection_kill_round:3 connections_terminated:25] drain=44.39s duplicates=9 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS RetryExhaustionDLQ       seed=42 pass=true faults=map[sigkill:1] drain=39.28s duplicates=10 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T233044Z.json
CHAOS RepeatedCrashRecovery    seed=42 pass=true faults=map[crash_recovery_cycle:3 sigkill:9] drain=160.54s duplicates=90 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T235033Z-recovery42.json
CHAOS KEDAAutoscale            seed=43 pass=true faults=map[] drain=79.70s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261112T235407Z.json
CHAOS RandomPodKill            seed=44 pass=true faults=map[sigkill:3] drain=48.97s duplicates=30 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS GracefulTermination      seed=44 pass=true faults=map[sigterm_delete:1] drain=121.01s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS RollingUpdate            seed=44 pass=true faults=map[rollout_restart:3] drain=79.91s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS ScaleUpDown              seed=44 pass=true faults=map[scale:4] drain=339.40s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS PostgresRestart          seed=44 pass=true faults=map[postgres_restart:1] drain=80.67s duplicates=30 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS ConnectionKill           seed=44 pass=true faults=map[connection_kill_round:3 connections_terminated:23] drain=42.85s duplicates=9 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS RetryExhaustionDLQ       seed=44 pass=true faults=map[sigkill:1] drain=39.64s duplicates=10 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS RepeatedCrashRecovery    seed=44 pass=true faults=map[crash_recovery_cycle:3 sigkill:9] drain=164.99s duplicates=88 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
CHAOS KEDAAutoscale            seed=44 pass=true faults=map[] drain=73.48s duplicates=0 effects_violations=0 report=/Users/eliotboda/.paradev/worktrees/durableq/feat/k8s-chaos/bench/k8s/chaos/20261113T001148Z.json
```

## Superseded resource-sampling trial

[20261113T003202Z](results/20261113T003202Z/) contains the first complete benchmark,
its environment, CSV/JSON and [stdout](results/20261113T003202Z/run.log). All queue
checks passed; its DB throughput/latency, SIGKILL recovery and KEDA observations
are real. Its CPU peaks are **not authoritative**: cached metrics-server averages
could include the preceding batch or miss a short loaded window. For example,
50ms/one-replica reported 4,790m Postgres CPU carried over from the previous load.
The [native metrics observation](results/20261113T003202Z/metrics-window-observation.json)
confirmed roughly 16–18s CPU windows. The corrected runs keep timestamped native
PodMetrics alongside raw `kubectl top`, exclude previous-workload windows, wait
for post-drain publication and assert relevant CPU/memory coverage. Regressions
prove stale windows cannot inflate the next batch and fresh workers can include
their own idle setup. Original trial files remain unchanged.


## Timestamped benchmark trial and confirmation

[20261113T004916Z](results/20261113T004916Z/) ran the full matrix, recovery and
KEDA. Five matrix rows passed, as did recovery and KEDA. Two replica-2 cases
failed before enqueue: KEDA's previous resume could overwrite the requested zero
replicas during reset. The matrix now retains its pause between child cases and
reasserts its requested scale if a stale controller update arrives. The 50ms / 10
row drained successfully with all invariants, but was rejected for missing worker
CPU coverage. Its fresh worker CPU windows included only that case's idle setup;
the old cutoff rejected them unnecessarily. The corrected filter admits those
windows while retaining the stricter persistent-Postgres cutoff. These are harness
failures, not product failures; failed rows are not accepted as final measurements.
The original report and [stdout](results/20261113T004916Z/run.log) remain unchanged.

The final confirmation **passed in 18m48s**: replicas 2, 5 and 10 for both
configured sleep values, then recovery and KEDA. The accepted one-replica rows come from the
long timestamped trial; the other six rows come from this confirmation. All
sources use the same image, chart configuration, payload and batch sizes.

The retained one-replica worker CPU maxima were recomputed from raw PodMetrics
using the corrected fresh-worker rule and are unchanged (71.870373m at 0ms,
42.761636m at 50ms). [Window check](results/20261113T020847Z/one-replica-window-check.json).


## Accepted benchmark measurements

[Confirmation report](results/20261113T020847Z/report.json),
[stdout](results/20261113T020847Z/run.log),
[environment](results/20261113T020847Z/env.txt).
The [accepted CSV](results/20261113T020847Z/accepted-matrix.csv) and
[accepted JSON](results/20261113T020847Z/accepted-matrix.json) name each row's
source report. No source report was modified. Both sources record the same image
digest and chart values. The confirmation command was:

```sh
go test -tags k8s -timeout 60m -count=1 -v ./test/k8s/ \
  -run '^TestBench(Matrix|Recovery|KEDA)$/^sleep(0|50)$/^replicas(2|5|10)$' \
  -args -seed 42 -report bench/k8s/results/20261113T020847Z/report.json
```

All eight accepted configurations passed: exact job counts; I1–I7 zero;
zero failed, retried, abandoned or released attempts. Every recorded sample had
the requested desired and ready replica count, with no sample errors. Each
CPU/memory category had workload coverage. Zero effects were duplicated.
Throughput uses the common DB due time, excluding enqueue. Queue wait includes
waiting behind the up-front backlog; it is not an unloaded service latency.

| Sleep (ms) | Replicas | Jobs | Jobs/s | Processing p50 / p95 / p99 (ms) | Queue wait p50 / p95 / p99 (s) |
|---:|---:|---:|---:|---|---|
| 0 | 1 | 60,000 | 494.80 | 1.84 / 4.92 / 6.97 | 69.323 / 116.903 / 120.392 |
| 0 | 2 | 60,000 | 870.30 | 2.53 / 6.27 / 10.72 | 37.804 / 66.408 / 68.437 |
| 0 | 5 | 60,000 | 1420.88 | 6.49 / 19.08 / 32.35 | 24.804 / 41.010 / 41.973 |
| 0 | 10 | 60,000 | 1997.32 | 10.67 / 30.71 / 43.63 | 16.532 / 29.053 / 29.837 |
| 50 | 1 | 40,000 | 159.45 | 52.05 / 74.64 / 77.15 | 130.303 / 239.023 / 248.461 |
| 50 | 2 | 40,000 | 304.96 | 52.91 / 75.56 / 78.50 | 67.247 / 124.929 / 129.855 |
| 50 | 5 | 40,000 | 730.90 | 53.74 / 76.17 / 80.55 | 28.110 / 52.021 / 54.178 |
| 50 | 10 | 40,000 | 1288.91 | 54.64 / 77.37 / 81.38 | 16.351 / 29.464 / 30.667 |

Worker peaks are contemporaneous aggregate observations across all worker pods,
not a sum of separate per-pod maxima. CPU values are metrics-server window
averages; memory values are working-set observations. Peaks need not coincide
between CPU, memory and connections. Connections include the sampling psql session,
API, KEDA and workers connected to this database.

| Sleep (ms) | Replicas | Peak PG connections | Worker CPU (m) | Worker memory (MiB) | Postgres CPU (m) | Postgres memory (MiB) | Retry / failure rate |
|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 1 | 8 | 71.87 | 11.95 | 1027.42 | 97.18 | 0% / 0% |
| 0 | 2 | 15 | 146.37 | 22.73 | 1905.70 | 126.64 | 0% / 0% |
| 0 | 5 | 32 | 367.56 | 56.28 | 4778.01 | 268.35 | 0% / 0% |
| 0 | 10 | 62 | 493.69 | 101.43 | 6894.23 | 406.55 | 0% / 0% |
| 50 | 1 | 9 | 42.76 | 10.61 | 857.20 | 102.83 | 0% / 0% |
| 50 | 2 | 14 | 99.35 | 21.12 | 1674.33 | 138.43 | 0% / 0% |
| 50 | 5 | 32 | 254.70 | 49.00 | 3486.90 | 222.09 | 0% / 0% |
| 50 | 10 | 63 | 446.69 | 93.79 | 5844.50 | 355.29 | 0% / 0% |

SIGKILL recovery: **10 abandoned claims, 29.461–30.229s**
from the container's exit timestamp to each next claim. Lease `30s`,
heartbeat `10s`. All 1,200 jobs succeeded; exactly 10 jobs were
re-executed and no effects were duplicated. Per-job timestamps and latencies are
in the confirmation JSON.

| KEDA observation | Seconds |
|---|---:|
| Enqueue completion → first desired scale-up | 8.74 |
| Enqueue completion → 10 ready workers | 26.15 |
| DB drain → 3 desired and ready workers | 100.54 |

KEDA timings include the initial 5s scheduled delay, polling, startup and HPA
stabilization. The enqueue operation itself is excluded and separately recorded.
All 3,000 jobs succeeded. The final cluster check showed 3/3 ready workers,
KEDA enabled and unpaused, a drained queue and I1–I7 passing. The local harness
lock was removed. No Go product changes, image rebuild or chart changes were
needed. No production or multi-node performance claims are made.
