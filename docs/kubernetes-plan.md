# DurableQ on Kubernetes: Implementation Plan

**Goal:** Run DurableQ (an HTTP API plus N workers) on a local Kubernetes cluster with Helm, Prometheus/Grafana, and KEDA queue-depth autoscaling. Show with chaos tests and benchmarks that Kubernetes lifecycle events don't break the queue's guarantees.

**Architecture:** One Go binary and one distroless image. `durableq serve` is the API; `durableq worker` runs the workers. Postgres remains the only coordination point: KEDA reads the backlog from Postgres, and Prometheus scrapes `/metrics` from API and worker pods. The core queue (claim, lease, fencing, retry, dead-lettering) is unchanged. This work adds observation hooks and process lifecycle around it.

**Tech stack:** Go 1.27, pgx/v5, prometheus/client_golang, Docker (multi-stage, distroless), Helm 3, kind, KEDA (PostgreSQL scaler), kube-prometheus-stack, GitHub Actions.

**Spec:** the task brief (milestones M1–M8 below) and `docs/PRD.md` §16 (decisions D12–D17). D1–D11 remain binding.

---

## 1. Audit summary (2026-11-12, `main` @ 956d4a9)

| Area | State |
|---|---|
| Claim (`SKIP LOCKED`), DB-clock leases, heartbeats, reclaim of expired leases | Done, tested (`lease_test.go`, `queue_test.go`) |
| Fencing on `(id, attempt, running, lease unexpired)`; stale workers rejected | Done; injected-bug test proves the checker catches regressions |
| Retries with full-jitter backoff, `Permanent`, dead state (the DLQ is `state='dead'`) | Done |
| Idempotency keys; `EnqueueTx`; exactly-once *transactional* effects via `CommitFunc` | Done |
| SIGTERM/SIGINT: stop claiming, drain for `ShutdownGrace`, release the rest without spending budget | Done (`Worker.Run`, `qRelease`) |
| Process-level chaos (SIGKILL, SIGSTOP, conn kills, PG restarts) + invariants I1–I7 | Done (`internal/demo`, `chaos/`) |
| Benchmarks with honest results | Done for bare processes (`docs/results.md`) |
| CI | vet, staticcheck, race tests, short chaos + PG restart |
| **HTTP API** | **Missing.** Submission is library/CLI only |
| **Metrics / health endpoints** | **Missing.** Only in-process `Stats()` |
| **Container** | Minimal: distroless, but runs as root, has no healthcheck, and the build is not reproducible |
| **Kubernetes / Helm / KEDA / Grafana** | **Missing** |

Debt that matters for Kubernetes: worker pool sizing (`concurrency+2` connections per pod) exceeds Postgres's default `max_connections=100` at 10 replicas. Migrations are manual.

## 2. Architecture

```
                    ┌────────────── namespace: durableq ──────────────┐
  client ─HTTP──▶ Service durableq-api:8080 ─▶ Deployment durableq-api (serve)
                    │      /v1/jobs  /healthz /readyz /metrics (queue gauges)
                    │                                   │
                    │   Deployment durableq-worker ×N (worker --http-addr :9090)
                    │      /healthz /readyz /metrics (per-process counters)
                    │        ▲  replicas set by KEDA ScaledObject ◀── SELECT count(*) eligible
                    │        │                                          │
                    │   StatefulSet durableq-postgres (dev only, PVC) ◀─┘  or external DATABASE_URL
                    └────────────────────────────────────────────────────┘
  monitoring ns: kube-prometheus-stack ── PodMonitor ─▶ api + worker /metrics; Grafana sidecar loads dashboard ConfigMap
  keda ns: KEDA operator
```

## 3. Interface contract (binding for every workstream)

### CLI (`cmd/durableq`)
- `durableq serve --addr :8080`: the HTTP API. Reads `DATABASE_URL`. Flags: `--max-conns` (default 5), `--shutdown-timeout` (default 10s).
- `durableq worker ... --http-addr :9090`: existing flags plus an HTTP listener for health and metrics. An empty value disables it.
- `durableq healthcheck --url http://127.0.0.1:8080/healthz`: exits 0 on a 2xx within 2s, else 1. Used by the Docker `HEALTHCHECK`, because distroless has no shell or curl.
- `durableq migrate`, `enqueue`, `status`, `check`, `demo`, `bench`: unchanged.

### HTTP API (`serve`)
| Method/path | Behavior |
|---|---|
| `POST /v1/jobs` | Body `{"kind": string (required, 1–200 chars), "payload": JSON object (default {}), "max_attempts": int 1–100 (default 5), "delay_ms": int ≥ 0, "idempotency_key": string ≤ 200}`. Body limit 1 MiB (413), unknown fields → 400. Returns **201** `{"id":n,"existed":false}`, or **200** `{"id":n,"existed":true}` on an idempotency hit. |
| `GET /v1/jobs/{id}` | 200 `{"id","kind","state","attempt","max_attempts","run_at","created_at","finished_at","last_error","attempts":[{"attempt","worker_id","claimed_at","finished_at","outcome","error"}]}`, 404 if missing, 400 if the id is not an integer. Timestamps are RFC 3339; nulls stay null. |
| `GET /v1/stats` | 200 `{"pending":n,"scheduled":n,"running":n,"lease_expired":n,"dead":n}` |
| `GET /healthz` | 200 `ok` while the process serves. Never checks the database (liveness). |
| `GET /readyz` | 200 when `pool.Ping` succeeds within 1s and the process is not shutting down, else 503. |
| `GET /metrics` | Prometheus text format |

Errors are JSON: `{"error":"..."}`. No auth: the Service is ClusterIP only, with no Ingress (documented limitation).

SIGTERM on `serve`: `/readyz` returns 503 at once, `http.Server.Shutdown(--shutdown-timeout)`, then the pool closes.
SIGTERM on `worker`: `/readyz` returns 503 at once, `Worker.Run` drains as today, then the HTTP server shuts down and the pool closes.

### Library additions (root package `durableq`; SQL in `queries.go`)
- `WorkerConfig.OnEvent func(Event)`: optional, called synchronously from worker goroutines, so it must be fast and must not block. The `Event` type carries `Kind`, `Type` (claimed, succeeded, retried, dead, released, lease_lost, stale, error), `Attempt`, `QueueWait` (claimed only; DB clock, claim time minus `run_at`), `Reclaimed` (claimed only; the previous attempt's lease had expired), and `Duration` (attempt-ending events; claim to transition). Every claimed job produces exactly one claimed event and exactly one attempt-ending event. Emitting an event must never change transition semantics.
- `func (w *Worker) Active() int`: jobs currently in flight.
- `Client.Stats(ctx) (QueueStats, error)` and `Client.Job(ctx, id) (JobInfo, error)` (`ErrNotFound` when missing): one SQL query each.
- `qClaim` additionally returns the queue wait and whether the row was reclaimed from `running`. Claim semantics, ordering, and fencing are unchanged.

### Metrics (namespace `durableq_`; `kind` labels come only from registered handler kinds; no job IDs; no client-supplied labels)
Worker process:
| Metric | Type | Labels |
|---|---|---|
| `durableq_job_claims_total` | counter | kind |
| `durableq_lease_expirations_total` (claims that reclaimed an expired lease) | counter | kind |
| `durableq_jobs_completed_total` | counter | kind |
| `durableq_jobs_failed_total` (every failed attempt: retried + dead-lettered) | counter | kind |
| `durableq_jobs_retried_total` | counter | kind |
| `durableq_jobs_dead_lettered_total` | counter | kind |
| `durableq_jobs_released_total` (graceful shutdown) | counter | kind |
| `durableq_lease_lost_total` (worker abandoned a job) | counter | kind |
| `durableq_stale_transitions_total` (fenced transition rejected) | counter | kind |
| `durableq_job_duration_seconds` | histogram | kind, outcome |
| `durableq_queue_wait_seconds` | histogram | kind |
| `durableq_worker_active_jobs` | gauge | — |
| `durableq_worker_concurrency` | gauge | — |

API process: the Go/process collectors, `durableq_jobs_enqueued_total` (counter, no labels, new jobs only), `durableq_http_requests_total{route,code}` (route is the mux pattern, never the raw path), and the queue gauges, computed by one query per scrape with `ContinueOnError`: `durableq_jobs_pending`, `durableq_jobs_scheduled`, `durableq_jobs_running`, `durableq_jobs_lease_expired`, `durableq_jobs_dead`. *(Revised after review: `durableq_jobs_succeeded` and the `succeeded` field of `/v1/stats` were dropped. Counting the unbounded history table is a full scan on every scrape. Each remaining count uses a partial index.)*

### Eligible backlog (KEDA query, D15)
```sql
SELECT count(*) FROM jobs
WHERE (state = 'queued' AND run_at <= clock_timestamp())
   OR (state = 'running' AND lease_expires_at < clock_timestamp())
```
It excludes scheduled and backoff jobs (`run_at` in the future) and healthy leased jobs. `metricType: AverageValue`, `targetQueryValue` = backlog per replica (default 50).

### Helm (`deploy/helm/durableq`, release `durableq`, namespace `durableq`)
- Names: `durableq-api` (Deployment and Service, port `http` 8080), `durableq-worker` (Deployment, container port `metrics` 9090), `durableq-postgres` (StatefulSet and Service, when `postgresql.enabled`).
- Labels: `app.kubernetes.io/{name: durableq, instance, component: api|worker|postgres, version, managed-by}`, `helm.sh/chart`.
- Values (top-level keys): `image.{repository,tag,pullPolicy}`, `database.{existingSecret,secretKey}` (default key `DATABASE_URL`), `api.{replicas,resources,maxConns}`, `worker.{replicas,concurrency,maxConns,lease,heartbeat,poll,grace,terminationGracePeriodSeconds,resources}`, `autoscaling.{enabled,minReplicas,maxReplicas,pollingInterval,cooldownPeriod,backlogPerReplica}`, `metrics.podMonitor.enabled`, `grafana.dashboards.enabled`, `postgresql.{enabled,image,auth.password,maxConnections,persistence.size,resources}`.
- Grafana dashboard JSON lives at `deploy/helm/durableq/dashboards/durableq.json`. The chart ships it as a ConfigMap labeled `grafana_dashboard: "1"`.
- Values files: `values.yaml` (safe defaults), `values-kind.yaml` (local cluster), `values-ci.yaml` (no monitoring CRDs, small resources).
- Connection budget (document it and enforce it in the template when `postgresql.enabled`): `maxReplicas × worker.maxConns + api.replicas × api.maxConns + 10 ≤ postgresql.maxConnections`.

### Local cluster
- `deploy/kind/cluster.yaml`. `scripts/kind-up.sh` is idempotent: it creates the cluster `durableq`, installs metrics-server, KEDA, and kube-prometheus-stack (trimmed values in `deploy/kind/monitoring-values.yaml`), builds the image `durableq:dev`, loads it with `kind load`, and runs `helm upgrade --install`. `scripts/kind-down.sh` deletes the cluster.
- Make targets: `docker-build`, `helm-lint`, `kind-up`, `kind-deploy`, `kind-down`, `k8s-smoke`.

## 4. Workstreams and delegation

```
 A backend (Codex Sol) ─┐
 B infra   (Sonnet)  ───┼─▶ I1 integrate + kind smoke (orchestrator) ─┬─▶ D k8s chaos + bench (Codex Sol) ─┐
 C dash+CI (Sonnet)  ───┘                                             └─▶ E independent review (Sonnet) ──┼─▶ I2 integrate, fix, docs, final verify (orchestrator)
```

| Task | Owner | Branch | Owns (may edit) | Depends on |
|---|---|---|---|---|
| A | Codex (gpt-6.1-sol) | `feat/k8s-backend` | `*.go` in the root package, `queries.go`, `cmd/durableq/**`, `internal/api/**`, `internal/metrics/**`, `go.mod`, `go.sum` | contract |
| B | Claude Sonnet | `feat/k8s-infra` | `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `Makefile`, `deploy/**` except `dashboards/`, `scripts/**` | contract |
| C | Claude Sonnet | `feat/k8s-observability-ci` | `deploy/helm/durableq/dashboards/**`, `.github/workflows/**` | contract |
| I1 | orchestrator | `feat/kubernetes` | merges; integration fixes | A, B, C |
| D | Codex (gpt-6.1-sol) | `feat/k8s-chaos` | `test/k8s/**`, `scripts/k8s-*.sh`, `bench/k8s/**` | I1 |
| E | Claude Sonnet | read-only | produces a review report | I1 |
| I2 | orchestrator | `feat/kubernetes` | fixes, `docs/kubernetes.md`, README, results | D, E |

Rules: no workstream changes the queue's SQL semantics (fencing predicates, claim ordering, attempt accounting). Changes to `qClaim` are additive output columns only. Workers commit on their own branches with no AI attribution trailers. Nobody pushes.

## 5. Milestone mapping and acceptance

| Brief milestone | Where | Acceptance (verified by the orchestrator) |
|---|---|---|
| M1 Containerization | B | `docker build` works; image is non-root, `-trimpath`, healthcheck; `docker compose up` runs api+worker+pg; enqueue via API completes |
| M2 Kubernetes deploy | B | `make kind-up` deploys; `kubectl -n durableq get pods` shows api, 3 workers, and pg Ready |
| M3 Worker reliability | A (+ existing) + D | SIGTERM drains and releases; readiness flips; pool closes; existing lease/fencing tests stay green; pod kill/rollout chaos passes I1–I7 |
| M4 Prometheus/Grafana | A + C | `/metrics` exposes the contract names; Prometheus targets are up; the dashboard loads and shows data during load |
| M5 KEDA | B + D | Replicas rise under backlog and fall after drain; correctness holds through scale-down |
| M6 Chaos | D | `make k8s-chaos` writes a JSON report; repeated runs pass |
| M7 Benchmarks | D + I2 | Measured 1/2/5/10-replica table with methodology and hardware; no invented numbers |
| M8 Helm + CI | B + C | `helm lint`, kubeconform, image build, Trivy scan, and a kind smoke job in CI |

## 6. Testing strategy

- **Go:** `go vet ./... && go test -race -count=1 ./...` against real Postgres, run with `-v` to confirm nothing skipped. New tests cover: API validation and status codes (httptest + `testdb`), idempotent POST, 404, stats correctness, exactly one claimed and one ending event per job (success, retry, dead, release, lease loss, stale), `Reclaimed` and `QueueWait` on reclaim, readiness 503 during shutdown, and a metrics endpoint containing the contract names.
- **Static:** `helm lint`, `helm template` with each values file piped to kubeconform (CRDs skipped or schema-located), and `docker build`.
- **Cluster:** a kind smoke test (deploy, POST 200 jobs, wait for drain, `durableq check` in-cluster → all pass).
- **Chaos (D):** random pod delete with `--grace-period=0 --force` (SIGKILL), graceful delete, rollout restart, scale 1↔10 mid-run, Postgres pod delete (PVC durability), and network interruption via `pg_terminate_backend`. Lease expiry, retry exhaustion, and dead-lettering are already covered deterministically in Go tests. Every scenario ends with I1–I7 and a JSON report.
- **Bench (D):** replicas ∈ {1,2,5,10}, fixed job duration and payload, DB-side timings (`job_attempts`) for latency and wait percentiles, `pg_stat_activity` for connections, and `kubectl top` for CPU and memory.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Laptop resources (Docker VM: 11 CPU, ~9 GB) with kube-prometheus-stack, KEDA, and 10 workers | Trimmed monitoring values, small requests, `values-kind.yaml`; benchmarks record the limits honestly |
| Postgres connection exhaustion at max replicas | `worker.maxConns` default 6, `postgresql.maxConnections` 200, template guard, documented formula |
| KEDA scale-down thrash while workers hold jobs | HPA scale-down stabilization window plus a pods-per-minute limit (KEDA's `cooldownPeriod` only governs scaling to and from zero); scale-down is a graceful release (no budget spent) |
| Metrics hook perturbs the hot path | Synchronous counter increments only; the bench compares against the bare-process results |
| Worker/agent drift from the contract | The contract above is binding; the orchestrator reviews each branch against it before merging |
| Chart and operator version drift | Pin exact chart versions in scripts; record them in the docs |
