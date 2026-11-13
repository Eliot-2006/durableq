# Media

Every image here comes from a real run. None is drawn or simulated.

## `site/public/media/terminal-demo.gif`

Recorded with [VHS](https://github.com/charmbracelet/vhs) from [`terminal-demo.tape`](terminal-demo.tape): a real `durableq demo` run against local PostgreSQL 17 (3,000 jobs, 4 worker processes, 12 SIGKILLs, 3 connection-kill rounds, 2 SIGSTOP stalls past the lease, 3 graceful restarts, seed 7), ending with the seven invariant checks. Regenerate with:

```sh
DATABASE_URL='postgres://localhost:5432/durableq?sslmode=disable' vhs docs/demo/terminal-demo.tape
```

Fault timing is seeded, but process scheduling isn't, so counts differ slightly between recordings.

## `site/public/media/grafana-keda.png` and `grafana-keda-summary.png`

The repository's Grafana dashboard (`deploy/helm/durableq/dashboards/durableq.json`) on the local kind cluster from `make kind-up`, with KEDA enabled (min 3, max 10 workers). The build is the one checked in [results-k8s.md](../results-k8s.md#final-build-check). Workload, enqueued through the API pod's CLI:

```sh
kubectl -n durableq exec deploy/durableq-api -c api -- /durableq enqueue --kind demo.effect --count 6000  --payload '{"sleep_ms":1000,"fail_rate":0.05}'
kubectl -n durableq exec deploy/durableq-api -c api -- /durableq enqueue --kind demo.effect --count 24000 --payload '{"sleep_ms":1000,"fail_rate":0.05}'   # about 90s later
```

Captured right after the queue drained, over a 15-minute window, in Grafana's kiosk mode (`kubectl -n monitoring port-forward svc/monitoring-grafana 3300:80`, local-only credentials from `deploy/kind/monitoring-values.yaml`). It shows the workers scaling from 3 to 10, about 95 jobs/s at full slot utilization, the backlog draining, retries from the injected 5% failure rate, and no dead jobs. The stat panels show values at capture time (after the drain), and their sparklines show the run. The summary image is the stat row, the queue and throughput row, and the workers and scaling row of the same screenshot.
