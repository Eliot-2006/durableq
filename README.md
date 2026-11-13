# DurableQ

**An at-least-once job queue in Go, with PostgreSQL leases and fencing.**

DurableQ uses `FOR UPDATE SKIP LOCKED` to distribute work across concurrent workers and reclaim jobs after crashes. Run it as a Go library or deploy the HTTP API and workers on Kubernetes with Helm, KEDA autoscaling, and Prometheus/Grafana observability; PostgreSQL remains the only coordinator.

[![ci](https://github.com/Eliot-2006/durableq/actions/workflows/ci.yml/badge.svg)](https://github.com/Eliot-2006/durableq/actions/workflows/ci.yml)
[![Go 1.27.3](https://img.shields.io/badge/Go-1.27.3-007d9c)](go.mod)
[![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-4169e1)](docker-compose.yml)
[![MIT license](https://img.shields.io/badge/license-MIT-555555)](LICENSE)

[Docs](docs/design.md) · [Benchmarks](docs/results.md) · [Kubernetes](docs/kubernetes.md) · [Site](https://eliot-2006.github.io/durableq/)

## Engineering highlights

- **10,946 jobs/s** at 8 processes × 50 slots: a no-op handler, host PostgreSQL 17.11, Apple M3 Pro. The recorded peak is 10,946.4 jobs/s. [Bare process measurements](docs/results.md#throughput).
- **8.1× throughput at 50ms of work per job; 4.0× at 0ms**, going from 1 to 10 worker replicas on a kind cluster. Each pod has 10 slots and 6 database connections; Postgres runs inside the Docker VM. [Replica matrix](docs/results-k8s.md#throughput-by-replica-count).
- **20/20 bare process chaos acceptance runs over 200,000 jobs**, with 100 SIGKILLs per run plus failpoints, connection kills and stalls. All seven SQL invariants passed. [Documented acceptance summary](docs/PROGRESS.md#m3-acceptance-2026-11-05-same-machine).
- **Nine Kubernetes fault scenarios, each passing I1–I7 in at least two runs**: pod kills, graceful termination, rollouts, scaling, Postgres restart, connection kills, retry exhaustion, repeated crash recovery and KEDA autoscaling. [Kubernetes evidence](docs/results-k8s.md#chaos-nine-scenarios-i1i7-hold-in-every-run).
- **Crashed workers’ jobs are reclaimed after about one lease:** 29.46–30.23s with a 30s lease on the kind cluster; p50 1,992ms and max 1,993.7ms with a 2s lease in the bare process run. [Recovery measurements](docs/results-k8s.md#recovery-and-autoscaling) · [Bare process recovery](docs/results.md#crash-recovery).

**Limits:** external side effects require idempotency; no ordering guarantee, database HA/backups, or API authentication/TLS. [Guarantees and limitations](#guarantees).

## Architecture

```mermaid
flowchart TB
  producer[Producer / Go client]
  subgraph cluster[Kubernetes · Helm deployment]
    api[HTTP API]
    pg[(PostgreSQL · jobs / attempts / effects)]
    subgraph workers[Worker pods]
      claim[Claim due jobs or expired leases]
      handler[Run handler outside transaction]
      complete[Fenced transition + CommitFunc transaction]
      claim --> handler --> complete
    end
    keda[KEDA PostgreSQL scaler]
    prom[Prometheus]
    graf[Grafana]
    api -->|Enqueue / status| pg
    claim -->|FOR UPDATE SKIP LOCKED| pg
    handler -->|Heartbeat · database-clock lease| pg
    complete -->|Current attempt + running + unexpired lease| pg
    keda -->|Read eligible backlog| pg
    keda -->|HPA · adjust replicas| workers
    prom -->|Scrape /metrics| api
    prom -->|Scrape /metrics| workers
    graf -->|Query metrics| prom
  end
  producer -->|POST /v1/jobs| api
  producer -->|Enqueue / EnqueueTx| pg
```

Claiming and completion use short transactions. A handler holds a lease rather than a database connection while it works. The completion transaction runs its fenced update before the handler's transactional effects. [SQL contracts and failure model](docs/design.md).

## Performance

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/charts/k8s-throughput-dark.svg">
  <img src="docs/assets/charts/k8s-throughput-light.svg" alt="Measured kind throughput at 1, 2, 5 and 10 replicas: 0ms jobs scale 4.0× and 50ms jobs scale 8.1×. Dashed lines show one-replica linear references, not measurements." width="880">
</picture>

At 10 kind cluster replicas, the 0ms workload reaches 4.0× throughput while Postgres uses 6.9 CPU cores and all workers together use 0.49 cores. This setup is database-bound; the 50ms workload reaches 8.1× throughput. See the [CPU chart and full matrix](docs/results-k8s.md#throughput-by-replica-count), including latency, connections and resource samples.

These are single-laptop measurements, one run per cell, without confidence intervals. The kind cluster setup uses in-cluster Postgres and 6 connections per pod; the bare process benchmark uses host Postgres and up to 80 worker connections. The two setups measure different workloads and are not a capacity comparison. [Methodology and limits](docs/results.md#setup).

## Reliability

The chaos harness kills real worker processes, stalls them past their leases, interrupts database connections and restarts Postgres. After draining, it checks I1–I7 in SQL, including final ownership of each transactional effect. Re-executions are expected under at-least-once delivery. A stale attempt cannot commit its database transition or transactional effects; fencing cannot undo an external side effect the handler already performed. See the [demo transcript](docs/results.md#chaos-demo-transcript), [injected fencing-bug experiment](docs/results.md#the-checker-catches-a-fencing-bug), and [Kubernetes chaos chart](docs/results-k8s.md#chaos-nine-scenarios-i1i7-hold-in-every-run).

Try it: [deterministic simulation of lease expiry and fencing](https://eliot-2006.github.io/durableq/reliability/).

<img src="site/public/media/terminal-demo.gif" alt="Terminal recording of a real durableq demo run: 3,000 jobs across 4 worker processes while the harness SIGKILLs workers, kills their database connections, stalls them past their leases and restarts them; the queue drains and all seven invariant checks print PASS." width="880">

*A real `durableq demo` run (3,000 jobs, 12 SIGKILLs, stalls and connection kills), ending with all seven invariant checks passing. [How it was recorded](docs/demo/README.md).*

<img src="site/public/media/grafana-keda-summary.png" alt="The repository's Grafana dashboard on the kind cluster during a 30,000-job run: KEDA scales workers from 3 to 10, throughput holds near 95 jobs per second at full slot utilization, pending jobs peak near 20,000 before draining, and no jobs are dead-lettered." width="880">

*The repository's Grafana dashboard on the kind cluster during a 30,000-job run with 5% injected failures: KEDA scales workers from 3 to 10, pending jobs peak near 20,000 before draining at about 95 jobs/s, and nothing is dead-lettered. [Full dashboard](site/public/media/grafana-keda.png) · [workload](docs/demo/README.md).*

<a id="guarantees"></a>

## Guarantees and limitations

- **At-least-once execution:** committed jobs run until success or dead-lettering, while workers and Postgres are available. Executions may repeat.
- **At most one terminal transition per job:** `succeeded` or `dead` commits at most once.
- **Fenced transitions:** heartbeat, completion, failure and release check the current attempt, running state and lease against the database clock. See the [row-lock nuance](docs/design.md#lock-wait-subtlety-from-the-independent-review).
- **Exactly-once transactional effects:** `CommitFunc` writes commit if and only if that attempt's completion commits. External side effects, such as HTTP calls and emails, may repeat; handlers must make them idempotent.
- **Bounded attempts:** crashes spend the attempt budget; a healthy job can dead-letter after repeated infrastructure faults. Graceful release raises the budget without decrementing the fencing token.
- **No ordering guarantee.** No progress during a database outage, no protection against permanent database loss, and no built-in retention of finished jobs.

The Helm chart's single Postgres instance has no HA or backups, and the demo HTTP API has no authentication or TLS. See [design limits](docs/design.md#9-known-limitations) and [deployment limits](docs/kubernetes.md#known-limitations) before adapting it to an exposed service.

## Quick start

### Compose + Go

Requires Go 1.27.3 and Docker Compose (or PostgreSQL 17 with your own `DATABASE_URL`).

```sh
make up
export DATABASE_URL='postgres://durableq:durableq@localhost:5432/durableq?sslmode=disable'
go run ./cmd/durableq migrate
go run ./cmd/durableq enqueue --kind demo.effect --payload '{"sleep_ms":100,"fail_rate":0.2}' --count 1000
go run ./cmd/durableq worker --concurrency 20   # Ctrl-C drains, releases unfinished jobs and exits
# After stopping the worker:
go run ./cmd/durableq status
go run ./cmd/durableq check                   # run after the queue is drained
```

If port 5432 is occupied, use `DURABLEQ_PG_PORT=55432 make up` and change the URL to port 55432. For the API and workers entirely in Compose, see [the Compose guide](docs/kubernetes.md#docker-compose-no-kubernetes).

### Kubernetes (kind cluster)

Requires Docker, kind, kubectl and Helm. No cloud account.

```sh
make kind-up       # cluster, addons, image and Helm chart
make k8s-smoke     # submit via HTTP API, drain, check I1–I7 in-cluster
kubectl -n durableq port-forward svc/durableq-api 8080:8080
# In another terminal:
curl -XPOST localhost:8080/v1/jobs -d '{"kind":"demo.effect","payload":{"sleep_ms":100}}'
curl localhost:8080/v1/stats
# Stop the port-forward, then clean up when finished:
make kind-down
```

[Helm values, API, autoscaling and dashboard access](docs/kubernetes.md).

<details>
<summary>Use as a Go library</summary>

```go
client := durableq.NewClient(pool)
client.Enqueue(ctx, durableq.EnqueueParams{Kind: "email.send", Payload: payload, IdempotencyKey: "order-42"})
client.EnqueueTx(ctx, tx, params) // atomic with your own writes (transactional outbox)

w := durableq.NewWorker(pool, durableq.WorkerConfig{Concurrency: 10})
w.Handle("email.send", func(ctx context.Context, job durableq.Job) (durableq.CommitFunc, error) {
	if err := send(ctx, job.Payload); err != nil { // external effect: may repeat, so make it idempotent
		return nil, err // retried with exponential backoff; durableq.Permanent(err) dead-letters
	}
	return func(ctx context.Context, tx pgx.Tx) error { // runs in the fenced completion transaction
		_, err := tx.Exec(ctx, `UPDATE orders SET emailed = true WHERE id = $1`, orderID(job))
		return err
	}, nil
})
w.Run(ctx) // until ctx is cancelled; then drains for ShutdownGrace and releases the rest
```

</details>

## Documentation

- [Design](docs/design.md) — state machine, SQL contracts, leases, fencing and failure model
- [Bare process results](docs/results.md) — setup, benchmarks, chaos transcript and checker experiments
- [Kubernetes guide](docs/kubernetes.md) — Helm, API, KEDA, Prometheus and Grafana
- [Kubernetes results](docs/results-k8s.md) — replica matrix, fault scenarios, recovery and autoscaling
- [Requirements and decisions](docs/PRD.md) · [Kubernetes plan](docs/kubernetes-plan.md) · [Implementation plan](docs/IMPLEMENTATION_PLAN.md) · [Engineering log](docs/PROGRESS.md)
- [GitHub Pages site](https://eliot-2006.github.io/durableq/) · [Regenerate results and charts](tools/resultsgen/README.md)

MIT licensed. See [LICENSE](LICENSE).
