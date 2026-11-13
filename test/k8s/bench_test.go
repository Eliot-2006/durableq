//go:build k8s

package k8s

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBenchMatrix(t *testing.T) {
	acquireCluster(t)
	captureEnv(t)
	t.Cleanup(func() { writeCSV(t) })
	var owner *harness
	t.Cleanup(func() {
		if owner != nil {
			if err := owner.restore(); err != nil {
				owner.r.Errors = append(owner.r.Errors, "matrix restore: "+err.Error())
				owner.r.Passed = false
				t.Error(err)
			}
		}
	})
	for _, sleep := range []int{0, 50} {
		for _, replicas := range []int{1, 2, 5, 10} {
			t.Run(fmt.Sprintf("sleep%d/replicas%d", sleep, replicas), func(t *testing.T) {
				h := newHarness(t)
				if owner == nil {
					owner = h
				}
				h.parentRestores = true
				h.fixed(replicas)
				h.r.Params["replicas"] = replicas
				h.r.Params["sleep_ms"] = sleep
				h.wait(2*time.Minute, "metrics-server samples for every worker and postgres", func() bool {
					out, err := kube("top", "pods", "--no-headers")
					if err != nil {
						return false
					}
					peaks, err := resources(out)
					return err == nil && peaks.Workers == replicas && peaks.Postgres == 1
				})
				h.delay = "24h"
				n := 40000
				if sleep == 0 {
					n = 60000
				}
				h.enqueue(n, sleep, 5, 0, false)
				done, active := h.progress()
				h.check(done == 0 && active == 0, "benchmark batch must be fully enqueued before processing")
				h.must(sql(qStartBatch))
				h.drain(true)
				h.verify(false)
				h.decode(h.must(sql(qBench)), &h.r.Benchmark)
				start, err := time.Parse(time.RFC3339Nano, h.r.Benchmark["started_at"].(string))
				if err != nil {
					t.Fatal(err)
				}
				finish, err := time.Parse(time.RFC3339Nano, h.r.Benchmark["finished_at"].(string))
				if err != nil {
					t.Fatal(err)
				}
				h.wait(time.Minute, "metrics-server publishes the workload's final window", func() bool {
					h.sample(true)
					published := metricsPublished(h.r.Samples[len(h.r.Samples)-1].PodMetrics, finish, replicas)
					if !published {
						time.Sleep(time.Second)
					}
					return published
				})
				peaks, coverage, err := windowPeaks(h.r.Samples, h.r.Started, start, finish, replicas)
				if err != nil {
					t.Fatal(err)
				}
				for kind, count := range coverage {
					h.check(count > 0, "no workload resource window: "+kind)
				}
				maxConns := 0
				for _, s := range h.r.Samples {
					maxConns = max(maxConns, s.Connections)
				}
				h.r.Benchmark["peak_resources"] = peaks
				h.r.Benchmark["peak_postgres_connections"] = maxConns
				h.r.Benchmark["resource_window_coverage"] = coverage
				h.r.Benchmark["retry_rate"] = float64(h.r.Metrics["retried_attempts"]) / float64(h.r.Metrics["attempts"])
				h.r.Benchmark["failure_rate"] = float64(h.r.Metrics["failed_attempts"]) / float64(h.r.Metrics["attempts"])
				h.check(h.r.Metrics["failed_attempts"] == 0 && h.r.Metrics["abandoned_attempts"] == 0 && h.r.Metrics["released"] == 0, "controlled benchmark must have zero failures, retries, abandoned attempts and releases")
				fmt.Printf("BENCH replicas=%d sleep_ms=%d jobs=%d jobs/s=%.2f processing_ms=%v queue_wait_ms=%v peak_connections=%d resources=%+v\n", replicas, sleep, n, h.r.Benchmark["jobs_per_second"], h.r.Benchmark["processing_ms"], h.r.Benchmark["queue_wait_ms"], maxConns, peaks)
			})
		}
	}
}
func TestBenchRecovery(t *testing.T) {
	h := newHarness(t)
	h.enqueue(1200, 500, 10, 0, false)
	h.at(.05)
	p, snapshot := h.activePod()
	killed := h.kill(p)
	var jobs []claim
	h.decode(h.must(sql(killedClaimsQuery(snapshot[0], killed.Format(time.RFC3339Nano)))), &jobs)
	recovery := h.nextClaims(jobs)
	claims := recovery.Claims
	latencies := make([]float64, 0, len(claims))
	for _, c := range claims {
		d := c.ClaimedAt.Sub(killed).Seconds()
		h.check(d >= 0, "next claim must be after SIGKILL")
		latencies = append(latencies, d)
	}
	h.r.Benchmark = map[string]any{"killed_at": killed, "pre_kill_snapshot": snapshot, "killed_jobs": recovery.Abandoned, "next_claims": claims, "recovery_seconds": latencies, "lease": h.config["WORKER_LEASE"], "heartbeat": h.config["WORKER_HEARTBEAT"]}
	h.drain(false)
	h.verify(false)
	fmt.Printf("RECOVERY lease=%s heartbeat=%s jobs=%d seconds=%v\n", h.config["WORKER_LEASE"], h.config["WORKER_HEARTBEAT"], len(recovery.Abandoned), latencies)
}
func TestBenchKEDA(t *testing.T) { h := newHarness(t); h.autoscale() }

type resourceUsage struct {
	WorkerCPU   float64 `json:"worker_cpu_millicores"`
	WorkerMiB   float64 `json:"worker_memory_mib"`
	PostgresCPU float64 `json:"postgres_cpu_millicores"`
	PostgresMiB float64 `json:"postgres_memory_mib"`
	Workers     int     `json:"workers_in_sample"`
	Postgres    int     `json:"postgres_in_sample"`
}

func resources(top string) (resourceUsage, error) {
	var r resourceUsage
	for line := range strings.SplitSeq(strings.TrimSpace(top), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return r, fmt.Errorf("invalid top line %q", line)
		}
		name := fields[0]
		if !strings.HasPrefix(name, "durableq-worker-") && name != "durableq-postgres-0" {
			continue
		}
		cpu, err := quantity(fields[1], true)
		if err != nil {
			return r, err
		}
		mem, err := quantity(fields[2], false)
		if err != nil {
			return r, err
		}
		if name == "durableq-postgres-0" {
			r.Postgres++
			r.PostgresCPU += cpu
			r.PostgresMiB += mem
		} else {
			r.Workers++
			r.WorkerCPU += cpu
			r.WorkerMiB += mem
		}
	}
	return r, nil
}
func quantity(s string, cpu bool) (float64, error) {
	suffix := map[string]float64{"Ki": 1. / 1024, "Mi": 1, "Gi": 1024, "Ti": 1048576}
	base := 1. / 1048576
	if cpu {
		suffix = map[string]float64{"n": .000001, "u": .001, "m": 1}
		base = 1000
	}
	for unit, scale := range suffix {
		if strings.HasSuffix(s, unit) {
			n, err := strconv.ParseFloat(strings.TrimSuffix(s, unit), 64)
			return n * scale, err
		}
	}
	n, err := strconv.ParseFloat(s, 64)
	return n * base, err
}
func captureEnv(t *testing.T) {
	t.Helper()
	var b strings.Builder
	commands := [][]string{
		{"date", "-u"}, {"uname", "-a"}, {"sysctl", "machdep.cpu.brand_string", "hw.model", "hw.ncpu", "hw.memsize"},
		{"docker", "info", "--format", "Docker VM CPUs={{.NCPU}} memory_bytes={{.MemTotal}} server={{.ServerVersion}}"},
		{"kind", "version"}, {"kubectl", "--context", "kind-durableq", "version", "-o", "json"},
		{"helm", "--kube-context", "kind-durableq", "-n", "durableq", "get", "values", "durableq", "--all", "-o", "yaml"},
		{"kubectl", "--context", "kind-durableq", "-n", "durableq", "get", "pods", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\t\"}{range .status.containerStatuses[*]}{.name}{\"=\"}{.imageID}{\" \"}{end}{\"\\n\"}{end}"},
		{"git", "rev-parse", "HEAD"}, {"git", "status", "--short"},
	}
	for _, args := range commands {
		fmt.Fprintf(&b, "$ %s\n", strings.Join(args, " "))
		out, err := command(args[0], args[1:]...)
		fmt.Fprintln(&b, out)
		if err != nil {
			fmt.Fprintln(&b, "ERROR:", err)
		}
	}
	path := filepath.Join(filepath.Dir(*reportPath), "env.txt")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
		t.Fatal(err)
	}
}
func writeCSV(t *testing.T) {
	t.Helper()
	path := filepath.Join(filepath.Dir(*reportPath), "matrix.csv")
	f, err := os.Create(path)
	if err != nil {
		t.Error(err)
		return
	}
	defer f.Close()
	w := csv.NewWriter(f)
	header := []string{"replicas", "sleep_ms", "jobs", "jobs_per_second", "processing_p50_ms", "processing_p95_ms", "processing_p99_ms", "queue_wait_p50_ms", "queue_wait_p95_ms", "queue_wait_p99_ms", "peak_postgres_connections", "worker_peak_cpu_millicores", "worker_peak_memory_mib", "postgres_peak_cpu_millicores", "postgres_peak_memory_mib", "retry_rate", "failure_rate", "passed"}
	if err = w.Write(header); err != nil {
		t.Error(err)
		return
	}
	for _, r := range reports {
		if !strings.HasPrefix(r.Scenario, "BenchMatrix/") || r.Benchmark == nil {
			continue
		}
		b := r.Benchmark
		proc, _ := b["processing_ms"].([]any)
		wait, _ := b["queue_wait_ms"].([]any)
		if len(proc) != 3 || len(wait) != 3 {
			t.Error("missing latency percentiles")
			continue
		}
		peaks, ok := b["peak_resources"].(resourceUsage)
		if !ok {
			t.Error("missing resource peaks")
			continue
		}
		row := []any{r.Params["replicas"], r.Params["sleep_ms"], r.Params["enqueued"], b["jobs_per_second"], proc[0], proc[1], proc[2], wait[0], wait[1], wait[2], b["peak_postgres_connections"], peaks.WorkerCPU, peaks.WorkerMiB, peaks.PostgresCPU, peaks.PostgresMiB, b["retry_rate"], b["failure_rate"], r.Passed}
		cells := make([]string, len(row))
		for i, v := range row {
			cells[i] = fmt.Sprint(v)
		}
		if err = w.Write(cells); err != nil {
			t.Error(err)
			return
		}
	}
	w.Flush()
	if err = w.Error(); err != nil {
		t.Error(err)
	}
}
