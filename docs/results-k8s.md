# Kubernetes results

Chaos and benchmark results for DurableQ on a local kind cluster. Every number here was measured; nothing is extrapolated. The raw reports, logs and environment captures are in [`bench/k8s/`](../bench/k8s/), and [`bench/k8s/RESULTS.md`](../bench/k8s/RESULTS.md) is the full evidence log, including the harness trials that failed and why. Results for the queue on bare processes (no Kubernetes) are in [results.md](results.md).

## Setup

| | |
|---|---|
| Host | Apple M3 Pro, 11 cores, 18 GiB RAM |
| Docker VM | 11 CPUs, 8.8 GiB |
| Cluster | kind v0.33.0, Kubernetes v1.37.0, 1 control-plane + 2 worker nodes |
| Postgres | in-chart `postgres:17` StatefulSet, default config, 1 Gi PVC |
| Worker pod | concurrency 10, `maxConns` 6, lease 30s, heartbeat 10s, grace 10s |
| KEDA | min 3 / max 10, 50 eligible jobs per replica, polling 15s, HPA scale-down window 120s |
| Workload | `demo.effect`: sleeps the configured time ±50%, then writes one effect row in the completion transaction |

Fixed-replica runs pause KEDA with its `paused-replicas` annotation and restore it afterwards. Exact chart values, image digests and tool versions are in each run's `env.txt`.

## Chaos: nine scenarios, I1–I7 hold in every run

Each scenario injects real faults into the running cluster, waits for the queue to drain, then runs the seven invariant checks (`durableq check`) in-cluster. Every scenario passed in at least two full runs (seeds 42/43 and 44) with zero violations of all seven invariants: no lost jobs, no duplicate effects. The table shows the seed-44 run.

| Scenario | Fault | Jobs | Drain (s) | Jobs re-executed |
|---|---|---:|---:|---:|
| RandomPodKill | 3 × SIGKILL of a worker container, via the kind node | 1,200 | 48.97 | 30 |
| GracefulTermination | pod delete (SIGTERM) mid-job | 90 | 121.01 | 0 |
| RollingUpdate | 3 × `rollout restart` under load | 2,400 | 79.91 | 0 |
| ScaleUpDown | 4 × replica changes under load | 4,000 | 339.40 | 0 |
| PostgresRestart | Postgres pod deleted mid-run; the StatefulSet recreates it | 900 | 80.67 | 30 |
| ConnectionKill | 3 rounds of `pg_terminate_backend` (23 connections) | 900 | 42.85 | 9 |
| RetryExhaustionDLQ | 180 always-failing + 60 permanently failing jobs, plus 1 SIGKILL | 240 | 39.64 | 10 |
| RepeatedCrashRecovery | 3 cycles of 3 SIGKILLs | 3,600 | 164.99 | 88 |
| KEDAAutoscale | burst of 3,000 jobs; KEDA scales 3→10→3 | 3,000 | 73.48 | 0 |

"Jobs re-executed" is at-least-once delivery working as designed: a SIGKILLed worker's in-flight jobs run again after their lease expires, and only one attempt's effect commits. Graceful paths (pod delete, rollout, scale-down, KEDA) re-executed nothing; the graceful-delete run released 10 in-flight attempts and all 90 jobs succeeded. The dead-letter run ended with all 240 jobs dead and zero effects.

## Throughput by replica count

A batch is enqueued up front with one shared due time, so throughput is measured from that due time to drain and excludes enqueueing. Every row passed I1–I7 with zero failed, retried or abandoned attempts.

| Work per job | Replicas | Jobs | Jobs/s | Speed-up vs 1 | Processing p50 / p95 / p99 (ms) |
|---|---:|---:|---:|---:|---|
| 0ms | 1 | 60,000 | 494.8 | 1.0× | 1.84 / 4.92 / 6.97 |
| 0ms | 2 | 60,000 | 870.3 | 1.8× | 2.53 / 6.27 / 10.72 |
| 0ms | 5 | 60,000 | 1,420.9 | 2.9× | 6.49 / 19.08 / 32.35 |
| 0ms | 10 | 60,000 | 1,997.3 | 4.0× | 10.67 / 30.71 / 43.63 |
| 50ms | 1 | 40,000 | 159.5 | 1.0× | 52.05 / 74.64 / 77.15 |
| 50ms | 2 | 40,000 | 305.0 | 1.9× | 52.91 / 75.56 / 78.50 |
| 50ms | 5 | 40,000 | 730.9 | 4.6× | 53.74 / 76.17 / 80.55 |
| 50ms | 10 | 40,000 | 1,288.9 | 8.1× | 54.64 / 77.37 / 81.38 |

Queue wait in these runs mostly measures time spent behind the up-front backlog, not service latency; per-row queue-wait percentiles are in the [evidence log](../bench/k8s/RESULTS.md#accepted-benchmark-measurements).

| Work per job | Replicas | Peak PG connections | Worker CPU (m) | Postgres CPU (m) | Postgres memory (MiB) |
|---|---:|---:|---:|---:|---:|
| 0ms | 1 | 8 | 72 | 1,027 | 97 |
| 0ms | 10 | 62 | 494 | 6,894 | 407 |
| 50ms | 1 | 9 | 43 | 857 | 103 |
| 50ms | 10 | 63 | 447 | 5,845 | 355 |

CPU values are metrics-server window averages, summed across worker pods at the same moment; all eight rows are in the evidence log.

**Where it stops scaling.** With 50ms of work per job, ten replicas give 8.1× the throughput of one. With no work per job, they give 4.0×, while Postgres reaches 6.9 of the VM's 11 cores and the workers together use under half a core. Each job costs a claim, an effect insert and a fenced completion transaction, so when handlers do no work the database is the bottleneck, and more workers mostly add contention. Real handlers that spend time on work scale close to linearly until Postgres saturates.

These numbers are not comparable to the [bare-process benchmark](results.md) (about 11,000 jobs/s), which used 8 workers × 50 slots and 80 connections against Postgres running directly on the host. Here Postgres runs inside a kind node on the Docker VM, and each worker pod is limited to 6 connections.

## Recovery and autoscaling

| Measurement | Result |
|---|---|
| SIGKILL a worker holding 10 claims → each job claimed again | 29.46–30.23s (lease 30s, heartbeat 10s) |
| Burst enqueued → KEDA's first scale-up decision | 8.74s |
| Burst enqueued → 10 ready workers | 26.15s |
| Queue drained → back to 3 ready workers | 100.54s |

The burst's jobs become due 5s after enqueue, and both scale-up times include that delay. Crash recovery takes one lease, as designed: a killed worker cannot release its jobs, so they wait for their lease to expire. A shorter lease recovers faster at the cost of more frequent heartbeats. The scale-up delay is KEDA's 15s polling plus pod start-up. The slow scale-down is deliberate: the 120s stabilization window and the 2-pods-per-minute limit stop the cluster from shedding workers that are still finishing jobs (see [kubernetes.md](kubernetes.md#autoscaling-keda)).

## Reproduce

```sh
make kind-up       # cluster, KEDA, Prometheus, image and chart
make k8s-chaos     # all nine scenarios (~20 min); K8S_SEED=44 make k8s-chaos for another seed
make k8s-bench     # replica matrix, SIGKILL recovery, KEDA timing (~40 min)
```

The suites truncate the demo tables in namespace `durableq` on context `kind-durableq`, so don't point them at anything else. Methodology and per-scenario details are in [`test/k8s/README.md`](../test/k8s/README.md).

## Which code was measured

The chaos and benchmark runs above used an image built from commit `0ec3769`. Changes merged after that touched the stats query, metric labels, chart settings, and the bare-process chaos harness, but not the claim, heartbeat or completion paths. To confirm the final code, the final build was redeployed to the same cluster and rerun; see [Final-build check](#final-build-check).

## Final-build check

After all branches were merged (merge commit `23d96c1`), the image was rebuilt, redeployed with `make kind-deploy` (which applied migration 0002), and rerun with seed 45:

| Check | Result |
|---|---|
| `make k8s-smoke` (200 jobs via the API, I1–I7 in-cluster) | pass |
| RandomPodKill (3 SIGKILLs, 1,200 jobs) | pass, drain 49.97s, 30 jobs re-executed |
| GracefulTermination (pod delete, 90 jobs) | pass, drain 113.92s, 0 re-executed |
| PostgresRestart (900 jobs) | pass, drain 52.69s, 30 jobs re-executed |

I1–I7 had zero violations in every run. Report: [`bench/k8s/chaos/final-build-seed45.json`](../bench/k8s/chaos/final-build-seed45.json).

## Limits

- One laptop and one dev Postgres. These numbers show how the system scales, not production capacity.
- Resource figures are sampled about every 2s. CPU figures are metrics-server window averages (about 15s), not instantaneous peaks.
- Seeds fix the harness's fault timing and targets, not handler jitter or Kubernetes scheduling.
