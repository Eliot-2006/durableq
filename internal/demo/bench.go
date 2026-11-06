package demo

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Eliot-2006/durableq"
)

// BenchConfig parameterizes `durableq bench` (PRD §11).
type BenchConfig struct {
	DatabaseURL, WorkerBin, OutDir string
	Jobs                           int   // demo.noop jobs per throughput cell
	Procs, Concurrency             []int // throughput sweep
	LatencyRate                    int   // jobs/s enqueued during the latency run
	LatencyFor                     time.Duration
	RecoveryJobs                   int
	Lease                          time.Duration // lease used by the recovery run
	ChaosRuns                      int
	Out                            io.Writer
}

// startWorkers spawns n worker processes tagged with tag; stop SIGTERMs and reaps them.
func startWorkers(bin, dbURL, tag string, n int, args ...string) (procs []*exec.Cmd, stop func(), err error) {
	stop = func() {
		for _, c := range procs {
			c.Process.Signal(syscall.SIGTERM)
		}
		for _, c := range procs {
			c.Wait()
		}
	}
	for range n {
		c := exec.Command(bin, append([]string{"worker", "--tag", tag}, args...)...)
		c.Env = append(os.Environ(), "DATABASE_URL="+dbURL, "DURABLEQ_FAILPOINTS=")
		if err := c.Start(); err != nil {
			stop()
			return nil, nil, err
		}
		procs = append(procs, c)
	}
	return procs, stop, nil
}

// workerActivity scopes pg_stat_activity to one benchmark's workers ($1 = tag).
const workerActivity = `FROM pg_stat_activity WHERE application_name LIKE 'durableq-worker-' || $1 || '-%' AND datname = current_database()`

func waitFor(ctx context.Context, s *Schema, cond string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var ok bool
		if err := s.Pool.QueryRow(ctx, "SELECT "+cond).Scan(&ok); err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %v waiting for: %s", timeout, cond)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

const drained = `NOT EXISTS (SELECT 1 FROM jobs WHERE state IN ('queued', 'running'))`

func RunBench(ctx context.Context, cfg BenchConfig) error {
	out := cfg.Out
	dir := filepath.Join(cfg.OutDir, time.Now().UTC().Format("20060102T150405Z"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	env, err := benchEnv(ctx, cfg)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "env.txt"), []byte(env), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s\nresults: %s\n\n", env, dir)
	stamp := time.Now().UnixNano()

	// Throughput sweep, with contention, connections, and bloat sampled per cell.
	tp := [][]string{{"procs", "concurrency", "pool_per_proc", "jobs", "seconds", "jobs_per_sec", "max_worker_conns", "top_wait_events", "jobs_n_dead_tup"}}
	fmt.Fprintln(out, "Throughput (demo.noop, pre-enqueued backlog)")
	for _, p := range cfg.Procs {
		for _, c := range cfg.Concurrency {
			row, err := benchThroughput(ctx, cfg, fmt.Sprintf("bench_tp_%d_%d_%d", p, c, stamp), p, c)
			if err != nil {
				return fmt.Errorf("throughput %d×%d: %w", p, c, err)
			}
			fmt.Fprintf(out, "  %d procs × %-3d (pool %-2s)  %8s jobs/s   conns %-3s  waits %s\n", p, c, row[2], row[5], row[6], row[7])
			tp = append(tp, row)
		}
	}
	if err := writeCSV(dir, "throughput.csv", tp); err != nil {
		return err
	}

	lat, err := benchLatency(ctx, cfg, fmt.Sprintf("bench_lat_%d", stamp))
	if err != nil {
		return fmt.Errorf("latency: %w", err)
	}
	fmt.Fprintf(out, "\nQueue latency at %d jobs/s for %v (4 procs × 10, poll 100ms): p50 %s ms  p95 %s ms  p99 %s ms\n",
		cfg.LatencyRate, cfg.LatencyFor, lat[1][2], lat[1][3], lat[1][4])
	if err := writeCSV(dir, "latency.csv", lat); err != nil {
		return err
	}

	rec, err := benchRecovery(ctx, cfg, fmt.Sprintf("bench_rec_%d", stamp))
	if err != nil {
		return fmt.Errorf("recovery: %w", err)
	}
	fmt.Fprintf(out, "Crash recovery (%s jobs, lease %s, SIGKILL → next claim): p50 %s ms  max %s ms\n",
		rec[1][0], rec[1][1], rec[1][2], rec[1][3])
	if err := writeCSV(dir, "recovery.csv", rec); err != nil {
		return err
	}

	ch := [][]string{{"seed", "passed", "seconds", "kills", "self_kills", "conn_kills", "stalls", "abandoned", "stale_rejected"}}
	passed := 0
	for i := range cfg.ChaosRuns {
		r, err := RunChaos(ctx, ChaosConfig{
			DatabaseURL: cfg.DatabaseURL, WorkerBin: cfg.WorkerBin, Seed: uint64(i + 1),
			Jobs: 1000, Workers: 4, Concurrency: 10, SleepMS: 20, FailRate: 0.05, MaxAttempts: 5,
			Lease: 2 * time.Second, Heartbeat: 500 * time.Millisecond,
			Failpoints: "after_claim=0.002,during_run=0.002,before_complete=0.002,in_complete_tx=0.002,after_commit=0.002",
			Kills:      20, ConnKills: 5, Stalls: 2, Restarts: 3, DrainTimeout: 5 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("chaos seed %d: %w", i+1, err)
		}
		if r.Passed() {
			passed++
		}
		ch = append(ch, []string{strconv.Itoa(i + 1), strconv.FormatBool(r.Passed()), f1(r.Elapsed.Seconds()),
			strconv.Itoa(r.Kills), strconv.Itoa(r.SelfKills), strconv.Itoa(r.ConnKills), strconv.Itoa(r.Stalls),
			strconv.FormatInt(r.Metrics.Abandoned, 10), strconv.FormatInt(r.StaleRejected, 10)})
	}
	if cfg.ChaosRuns > 0 {
		fmt.Fprintf(out, "Chaos reliability: %d/%d seeded runs passed all invariants\n", passed, cfg.ChaosRuns)
	}
	return writeCSV(dir, "chaos.csv", ch)
}

func benchThroughput(ctx context.Context, cfg BenchConfig, name string, procs, conc int) ([]string, error) {
	s, err := NewSchema(ctx, cfg.DatabaseURL, name)
	if err != nil {
		return nil, err
	}
	defer s.Close(true)
	if _, err := s.Pool.Exec(ctx, `INSERT INTO jobs (kind) SELECT 'demo.noop' FROM generate_series(1, $1)`, cfg.Jobs); err != nil {
		return nil, err
	}
	// ponytail: fixed 80-connection budget split across processes, leaving
	// headroom under the default max_connections=100; make it a flag if needed.
	pool := min(conc+2, max(2, 80/procs))
	_, stop, err := startWorkers(cfg.WorkerBin, s.URL, name, procs, "--concurrency", strconv.Itoa(conc),
		"--poll", "100ms", "--max-conns", strconv.Itoa(pool))
	if err != nil {
		return nil, err
	}

	// Sample wait events and connection counts until drained.
	waits := map[string]int{}
	maxConns := 0
	done := make(chan error, 1)
	go func() { done <- waitFor(ctx, s, drained, 10*time.Minute) }()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
sample:
	for {
		select {
		case err := <-done:
			if err != nil {
				stop()
				return nil, err
			}
			break sample
		case <-tick.C:
			rows, err := s.Admin.Query(ctx, `SELECT coalesce(wait_event_type || ':' || wait_event, 'running'), count(*) `+workerActivity+` GROUP BY 1`, name)
			if err != nil {
				continue
			}
			n := 0
			for rows.Next() {
				var ev string
				var c int
				if rows.Scan(&ev, &c) == nil {
					waits[ev] += c
					n += c
				}
			}
			rows.Close()
			maxConns = max(maxConns, n)
		}
	}
	stop() // exiting workers flush their table stats

	var secs float64
	if err := s.Pool.QueryRow(ctx, `SELECT extract(epoch FROM max(j.finished_at) - min(a.claimed_at))
		FROM jobs j JOIN job_attempts a ON a.job_id = j.id`).Scan(&secs); err != nil {
		return nil, err
	}
	var dead int64
	time.Sleep(600 * time.Millisecond)
	s.Admin.QueryRow(ctx, `SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = 'jobs'`, name).Scan(&dead)
	return []string{strconv.Itoa(procs), strconv.Itoa(conc), strconv.Itoa(pool), strconv.Itoa(cfg.Jobs), f1(secs),
		f1(float64(cfg.Jobs) / secs), strconv.Itoa(maxConns), topWaits(waits, 3), strconv.FormatInt(dead, 10)}, nil
}

func benchLatency(ctx context.Context, cfg BenchConfig, name string) ([][]string, error) {
	s, err := NewSchema(ctx, cfg.DatabaseURL, name)
	if err != nil {
		return nil, err
	}
	defer s.Close(true)
	_, stop, err := startWorkers(cfg.WorkerBin, s.URL, name, 4, "--concurrency", "10", "--poll", "100ms")
	if err != nil {
		return nil, err
	}
	defer stop()
	c := durableq.NewClient(s.Pool)
	start := time.Now()
	n := int(cfg.LatencyFor.Seconds() * float64(cfg.LatencyRate))
	for i := range n { // open-loop: job i is due at start + i/rate
		time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second / time.Duration(cfg.LatencyRate))))
		if _, err := c.Enqueue(ctx, durableq.EnqueueParams{Kind: "demo.noop"}); err != nil {
			return nil, err
		}
	}
	if err := waitFor(ctx, s, drained, time.Minute); err != nil {
		return nil, err
	}
	var p []float64
	if err := s.Pool.QueryRow(ctx, `SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP
		(ORDER BY extract(epoch FROM a.claimed_at - j.run_at) * 1000)
		FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE a.attempt = 1`).Scan(&p); err != nil {
		return nil, err
	}
	return [][]string{{"rate_per_sec", "jobs", "p50_ms", "p95_ms", "p99_ms"},
		{strconv.Itoa(cfg.LatencyRate), strconv.Itoa(n), f1(p[0]), f1(p[1]), f1(p[2])}}, nil
}

func benchRecovery(ctx context.Context, cfg BenchConfig, name string) ([][]string, error) {
	s, err := NewSchema(ctx, cfg.DatabaseURL, name)
	if err != nil {
		return nil, err
	}
	defer s.Close(true)
	n := cfg.RecoveryJobs
	if _, err := s.Pool.Exec(ctx, `INSERT INTO jobs (kind, payload) SELECT 'demo.effect', '{"sleep_ms": 60000}' FROM generate_series(1, $1)`, n); err != nil {
		return nil, err
	}
	lease, hb := cfg.Lease.String(), (cfg.Lease / 4).String()
	victim, stopVictim, err := startWorkers(cfg.WorkerBin, s.URL, name, 1, "--concurrency", strconv.Itoa(n), "--lease", lease, "--heartbeat", hb, "--poll", "100ms")
	if err != nil {
		return nil, err
	}
	if err := waitFor(ctx, s, fmt.Sprintf(`(SELECT count(*) = %d FROM jobs WHERE state = 'running')`, n), 30*time.Second); err != nil {
		stopVictim()
		return nil, err
	}
	victim[0].Process.Kill()
	victim[0].Wait()
	var killedAt time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&killedAt); err != nil {
		return nil, err
	}
	_, stop, err := startWorkers(cfg.WorkerBin, s.URL, name, 1, "--concurrency", strconv.Itoa(n), "--lease", lease, "--heartbeat", hb, "--poll", "100ms")
	if err != nil {
		return nil, err
	}
	defer stop()
	if err := waitFor(ctx, s, fmt.Sprintf(`(SELECT count(*) = %d FROM job_attempts WHERE attempt = 2)`, n), 3*cfg.Lease+10*time.Second); err != nil {
		return nil, err
	}
	var p50, pmax float64
	if err := s.Pool.QueryRow(ctx, `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), max(ms) FROM
		(SELECT extract(epoch FROM claimed_at - $1) * 1000 AS ms FROM job_attempts WHERE attempt = 2) x`, killedAt).Scan(&p50, &pmax); err != nil {
		return nil, err
	}
	return [][]string{{"jobs", "lease", "p50_ms", "max_ms"}, {strconv.Itoa(n), lease, f1(p50), f1(pmax)}}, nil
}

func benchEnv(ctx context.Context, cfg BenchConfig) (string, error) {
	s, err := NewSchema(ctx, cfg.DatabaseURL, fmt.Sprintf("bench_env_%d", time.Now().UnixNano()))
	if err != nil {
		return "", err
	}
	defer s.Close(true)
	var b strings.Builder
	var ver string
	s.Admin.QueryRow(ctx, `SELECT version()`).Scan(&ver)
	fmt.Fprintf(&b, "postgres: %s\n", ver)
	rows, err := s.Admin.Query(ctx, `SELECT name, setting || coalesce(unit, '') FROM pg_settings
		WHERE name IN ('max_connections', 'shared_buffers', 'synchronous_commit', 'fsync', 'wal_level') ORDER BY 1`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var k, v string
		rows.Scan(&k, &v)
		fmt.Fprintf(&b, "  %s = %s\n", k, v)
	}
	rows.Close()
	fmt.Fprintf(&b, "host: %s/%s, %d CPUs, %s, %s RAM\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), cpuModel(), ram())
	fmt.Fprintf(&b, "go: %s\ngit: %s\n", runtime.Version(), gitSHA())
	fmt.Fprintf(&b, "params: jobs=%d procs=%v concurrency=%v latency=%d/s×%v recovery_jobs=%d lease=%v chaos_runs=%d\n",
		cfg.Jobs, cfg.Procs, cfg.Concurrency, cfg.LatencyRate, cfg.LatencyFor, cfg.RecoveryJobs, cfg.Lease, cfg.ChaosRuns)
	return b.String(), nil
}

func cmdOut(name string, args ...string) string {
	o, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(o))
}

func cpuModel() string {
	if runtime.GOOS == "darwin" {
		return cmdOut("sysctl", "-n", "machdep.cpu.brand_string")
	}
	return cmdOut("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
}

func ram() string {
	if runtime.GOOS == "darwin" {
		if b, err := strconv.ParseInt(cmdOut("sysctl", "-n", "hw.memsize"), 10, 64); err == nil {
			return fmt.Sprintf("%d GiB", b>>30)
		}
		return "unknown"
	}
	return cmdOut("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2 \" kB\"}'")
}

func gitSHA() string {
	sha := cmdOut("git", "rev-parse", "--short", "HEAD")
	if cmdOut("git", "status", "--porcelain", "--untracked-files=no") != "" { // results files are untracked
		sha += "-dirty"
	}
	return sha
}

func topWaits(m map[string]int, n int) string {
	type kv struct {
		k string
		v int
	}
	var all []kv
	total := 0
	for k, v := range m {
		all = append(all, kv{k, v})
		total += v
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v > all[j].v })
	var parts []string
	for i := 0; i < len(all) && i < n; i++ {
		parts = append(parts, fmt.Sprintf("%s %.0f%%", all[i].k, 100*float64(all[i].v)/float64(total)))
	}
	return strings.Join(parts, "; ")
}

func f1(x float64) string { return strconv.FormatFloat(x, 'f', 1, 64) }

func writeCSV(dir, name string, rows [][]string) error {
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.WriteAll(rows)
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
