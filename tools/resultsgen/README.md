# Results generator

Run `make results` (or `go run ./tools/resultsgen`) from the repository root. Go's standard library reads the checked-in CSV/JSON evidence and writes `site/src/data/results.json` and six SVGs under `docs/assets/charts/`. No dependencies or network access are needed.

`go test ./tools/resultsgen` regenerates into a temporary directory and compares all seven outputs byte-for-byte. CI's existing `go test ./...` therefore catches drift. Parser tests reject missing columns, malformed values and incomplete or failed chaos evidence.

`documented.json` contains only environment descriptions and explicitly sourced prose summaries. The generator verifies supporting excerpts before carrying these summaries forward. The process acceptance summary comes from `docs/PROGRESS.md`; it is separate from the five older pre-audit runs in `chaos.csv`. The benchmark report linked by the seed also contains raw recovery and KEDA timings: the generator reads them directly, alongside the final-build timings from seed 45. It computes the recovery range from the recorded per-claim timings. The “at least two runs per scenario” figure is a documented summary, not inferred from the partial seed-45 rerun.

Raw numerical precision is retained in JSON. Queue-wait milliseconds are converted to seconds; CPU millicores are converted to cores only for plotting. Display labels use decimal formatting. Throughput speed-ups divide the ten-replica rate by the one-replica rate. Dashed linear references multiply the one-replica rate by replica count; they are not additional measurements. No unmeasured matrix cells are generated. CPU samples are maxima of metrics-server window averages, not instantaneous peaks.

The chaos graphic stacks scenarios above paired, aligned bars with separate scales for drain seconds and re-executed jobs, plus a zero-violations column for each of I1–I7. Workloads differ across scenarios, so drain times are not directly comparable. All charts use a 720px canvas, 22px base text, 26px titles and 16px footers, with light/dark variants, text descriptions, units, environment and source captions. Throughput and CPU legends use one column; the chaos chart labels its columns directly. The throughput frame ends at 2,200 jobs/s: the 0ms linear reference is clipped, with its ten-replica value (4,948 jobs/s) labelled above the frame. Workload series use both colour and marker shape; line style distinguishes measured/reference throughput and Postgres/worker CPU.

Light palette: white background, `#24292f` text, `#57606a` secondary text, `#0065a8` blue, `#a34500` orange. Dark palette: `#0d1117` background, `#e6edf3` text, `#aeb9c5` secondary text, `#56b4e9` blue, `#f3ab63` orange. Text exceeds 4.5:1 and data marks exceed 3:1 against their background. Grid lines are decorative.

## README claims ledger

| README number | Evidence |
|---|---|
| Go 1.27.3; PostgreSQL 17 | `go.mod:3`; `docker-compose.yml:3` (`image: postgres:17`) |
| 10,946 jobs/s; recorded 10,946.4; 8 processes × 50 slots; up to 80 connections | `bench/results/20261106T050117Z/throughput.csv:13`: `procs=8`, `concurrency=50`, `jobs_per_sec=10946.4`, `max_worker_conns=80` |
| Host PostgreSQL 17.11; Apple M3 Pro | `bench/results/20261106T050117Z/env.txt:1,7` |
| 4.0× at 0ms, 1→10 replicas | `bench/k8s/results/20261113T020847Z/accepted-matrix.csv:2,5`: `1997.3232539524947 / 494.7991292425964`, displayed to one decimal |
| 8.1× at 50ms, 1→10 replicas | Same CSV, lines 6,9: `1288.9134656044257 / 159.454289380379`, displayed to one decimal |
| 10 slots, 6 DB connections per pod | `docs/results-k8s.md:13`; raw reports' `params.worker_config.WORKER_CONCURRENCY` and `WORKER_MAX_CONNS` |
| 20/20 process acceptance runs; 200,000 jobs; 100 SIGKILLs/run; seven invariants | `docs/PROGRESS.md:14`: 20 seeds × 10,000 jobs, 100 SIGKILLs each, 20/20 passed I1–I7; documented summary, not a raw CSV aggregate |
| Nine Kubernetes scenarios, each passing I1–I7 in at least two runs | `docs/results-k8s.md:21` and `docs/PROGRESS.md:64` document repeat coverage; `bench/k8s/chaos/20261113T001148Z.json` has nine seed-44 records with `passed=true` and every `invariant_violations.I1` through `.I7` equal to zero |
| One run per benchmark cell | `docs/results.md:19`; accepted matrix contains one row per `(replicas,sleep_ms)` |
| Quick-start ports and numeric flags | These are example configuration, not measurements: Compose ports are in `docker-compose.yml:8,31–32`; count, sleep, fail rate, concurrency and alternate port are explicit choices in the commands |

The README chart's intermediate replica counts (2 and 5) and every plotted value come from the accepted matrix, lines 2–9. Its chart caption names the 11-CPU Docker environment from `docs/results-k8s.md:11`.

Recovery in the README is described as “reclaimed after about one lease”: kind cluster timings span 29.461497724–30.229478724s for a 30s lease (`bench/k8s/results/20261113T020847Z/report.json`, `BenchRecovery.benchmark.recovery_seconds`); the bare process run records p50 1,992.1ms and max 1,993.7ms for a 2s lease (`bench/results/20261106T050117Z/recovery.csv:2`). The README displays the p50 as 1,992ms. The 0ms bottleneck description uses the accepted matrix’s ten-replica row: Postgres 6,894.230701 millicores and total workers 493.686378 millicores, displayed as 6.9 and 0.49 cores.
