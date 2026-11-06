package demo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ChaosConfig drives one chaos run. Exactly Kills SIGKILLs, ConnKills
// connection-kill rounds, Stalls SIGSTOPs, Restarts graceful SIGTERM
// restarts, and DBRestarts Postgres restarts are injected at seeded random
// points of queue progress.
type ChaosConfig struct {
	DatabaseURL string // URL form; the run uses its own fresh schema
	WorkerBin   string // durableq binary used to spawn worker processes
	Seed        uint64 // drives the fault schedule (workers' own randomness is not seeded)

	Jobs, Workers, Concurrency int
	SleepMS, MaxAttempts       int
	FailRate                   float64
	Lease, Heartbeat           time.Duration
	Failpoints                 string // DURABLEQ_FAILPOINTS for workers

	Kills, ConnKills, Stalls, Restarts int
	DBRestarts                         int
	DBRestartCmd                       string // shell command that restarts Postgres (e.g. docker restart -t 0 <ctr>); required if DBRestarts > 0
	DrainTimeout                       time.Duration
	KeepSchema                         bool
	Out                                io.Writer // progress output
}

type ChaosReport struct {
	Seed                                     uint64
	Schema                                   string
	Elapsed                                  time.Duration
	Kills, SelfKills, ConnKills, ConnsKilled int
	Stalls, Restarts, DBRestarts             int
	Shortfall                                int // scheduled faults that never landed before drain
	StaleRejected, LeaseLost                 int64
	Metrics                                  Metrics
	Checks                                   []CheckResult
}

func (r ChaosReport) Passed() bool {
	for _, c := range r.Checks {
		if c.Violations != 0 {
			return false
		}
	}
	return len(r.Checks) > 0
}

func (r ChaosReport) Print(w io.Writer) {
	m := r.Metrics
	fmt.Fprintf(w, "Drained in %.1fs (seed %d, schema %s)\n", r.Elapsed.Seconds(), r.Seed, r.Schema)
	fmt.Fprintf(w, "  Jobs succeeded       %-8d dead %d (%d after a failed final attempt, %d with the budget exhausted by abandoned attempts)\n",
		m.Succeeded, m.Dead, m.Dead-m.DeadByBudget, m.DeadByBudget)
	fmt.Fprintf(w, "  Worker kills         %-8d self-kills (failpoints) %d\n", r.Kills, r.SelfKills)
	fmt.Fprintf(w, "  Connection kills     %-8d (%d connections terminated)\n", r.ConnKills, r.ConnsKilled)
	fmt.Fprintf(w, "  Worker stalls        %-8d graceful restarts %d (jobs released %d)\n", r.Stalls, r.Restarts, m.Released)
	if r.DBRestarts > 0 {
		fmt.Fprintf(w, "  Postgres restarts    %d\n", r.DBRestarts)
	}
	fmt.Fprintf(w, "  Abandoned attempts   %-8d jobs possibly executed more than once %d\n", m.Abandoned, m.DuplicateExecutions)
	fmt.Fprintf(w, "  Retries              %-8d stale transitions rejected %d\n", m.Retries, r.StaleRejected)
	fmt.Fprintf(w, "  Leases lost (worker abandoned job on its local deadline) %d\n", r.LeaseLost)
	ok := PrintChecks(w, r.Checks)
	if ok {
		fmt.Fprintf(w, "All %d invariants PASSED\n", len(r.Checks))
	} else {
		fmt.Fprintf(w, "INVARIANTS FAILED — reproduce with seed %d\n", r.Seed)
	}
}

type proc struct {
	slot    int
	cmd     *exec.Cmd
	stopped bool          // SIGKILLed or SIGTERMed by the harness (vs. a failpoint self-kill)
	stalled atomic.Bool   // SIGSTOPped, not yet resumed
	done    chan struct{} // closed when the process has exited
}

func (p *proc) live() bool {
	select {
	case <-p.done:
		return false
	default:
		return !p.stopped && !p.stalled.Load()
	}
}

type exit struct{ p *proc }

// RunChaos migrates a fresh schema, enqueues demo.effect jobs, runs worker
// processes while injecting faults, waits for the queue to drain, and checks
// every invariant. An error means the run could not complete (e.g. drain
// timeout); invariant violations are reported in ChaosReport.
func RunChaos(ctx context.Context, cfg ChaosConfig) (rep ChaosReport, err error) {
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	out := cfg.Out
	if out == nil {
		out = io.Discard
	}
	rep.Seed = cfg.Seed
	rep.Schema = fmt.Sprintf("chaos_%d_%d", cfg.Seed, time.Now().UnixNano())

	sc, err := NewSchema(ctx, cfg.DatabaseURL, rep.Schema)
	if err != nil {
		return rep, err
	}
	defer func() { sc.Close(err == nil && rep.Passed() && !cfg.KeepSchema) }()
	if cfg.DBRestarts > 0 && cfg.DBRestartCmd == "" {
		return rep, fmt.Errorf("DBRestarts set without DBRestartCmd")
	}
	pool, dbURL := sc.Pool, sc.URL
	if _, err := pool.Exec(ctx, `INSERT INTO jobs (kind, payload, max_attempts)
		SELECT 'demo.effect', jsonb_build_object('sleep_ms', $1::int, 'fail_rate', $2::float8), $3
		FROM generate_series(1, $4)`, cfg.SleepMS, cfg.FailRate, cfg.MaxAttempts, cfg.Jobs); err != nil {
		return rep, err
	}
	fmt.Fprintf(out, "Enqueued %d jobs · %d workers × %d · lease %v · failpoints %q\n",
		cfg.Jobs, cfg.Workers, cfg.Concurrency, cfg.Lease, cfg.Failpoints)

	start := time.Now()
	stamp := func() string {
		e := time.Since(start).Round(time.Second)
		return fmt.Sprintf("[%02d:%02d]", int(e.Minutes()), int(e.Seconds())%60)
	}
	// Each run tags its workers' application_name so connection kills and
	// sampling never touch another run's (or a real deployment's) workers.
	tag := fmt.Sprintf("%08x", rng.Uint32())
	var stale, leaseLost atomic.Int64
	exits := make(chan exit)
	stopping := make(chan struct{})
	procs := make([]*proc, cfg.Workers)
	spawn := func(slot int) error {
		cmd := exec.Command(cfg.WorkerBin, "worker", "--tag", tag,
			"--concurrency", strconv.Itoa(cfg.Concurrency), "--grace", "20ms",
			"--lease", cfg.Lease.String(), "--heartbeat", cfg.Heartbeat.String(), "--poll", "100ms")
		cmd.Env = append(os.Environ(), "DATABASE_URL="+dbURL, "DURABLEQ_FAILPOINTS="+cfg.Failpoints)
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		p := &proc{slot: slot, cmd: cmd, done: make(chan struct{})}
		procs[slot] = p
		go func() {
			sc := bufio.NewScanner(stderr)
			for sc.Scan() {
				switch line := sc.Text(); {
				case strings.Contains(line, "stale transition rejected"):
					stale.Add(1)
				case strings.Contains(line, "lease lost"):
					leaseLost.Add(1)
				}
			}
			cmd.Wait() // after stderr is fully read, so counters are complete
			close(p.done)
			select {
			case exits <- exit{p}:
			case <-stopping:
			}
		}()
		return nil
	}
	for i := range procs {
		if err := spawn(i); err != nil {
			return rep, err
		}
	}
	// stopAll resumes, stops, and reaps every worker. It runs before the
	// invariant checks, so every late transition attempt (e.g. a stalled
	// worker's stale completion) has happened and been judged by then.
	var stopOnce sync.Once
	stopAll := func() {
		stopOnce.Do(func() {
			close(stopping)
			for _, p := range procs {
				p.cmd.Process.Signal(syscall.SIGCONT)
				p.cmd.Process.Signal(syscall.SIGTERM)
			}
			deadline := time.Now().Add(cfg.Lease + 15*time.Second)
			for _, p := range procs {
				select {
				case <-p.done:
				case <-time.After(time.Until(deadline)):
					p.cmd.Process.Kill()
					<-p.done
				}
			}
		})
	}
	defer stopAll()

	pending := func() (int64, error) {
		var n int64
		err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')`).Scan(&n)
		return n, err
	}
	// Faults are scheduled by queue progress, not wall time, so they land
	// before the queue drains however fast the machine is: fault i is due once
	// the completed fraction passes at[i], a seeded random point in [2%, 95%].
	// The fault kinds are shuffled with the same seed.
	const (
		kill = iota
		connKill
		stall
		restart
		dbRestart
	)
	kinds := slices.Repeat([]int{kill}, cfg.Kills)
	kinds = append(kinds, slices.Repeat([]int{connKill}, cfg.ConnKills)...)
	kinds = append(kinds, slices.Repeat([]int{stall}, cfg.Stalls)...)
	kinds = append(kinds, slices.Repeat([]int{restart}, cfg.Restarts)...)
	kinds = append(kinds, slices.Repeat([]int{dbRestart}, cfg.DBRestarts)...)
	rng.Shuffle(len(kinds), func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
	at := make([]float64, len(kinds))
	for i := range at {
		at[i] = 0.02 + 0.93*rng.Float64()
	}
	slices.Sort(at)
	// inject reports whether the fault landed. Faults are only aimed at live
	// workers and only counted when the signal (or termination) took effect;
	// otherwise the fault stays due and is retried on the next tick.
	inject := func(kind int) (bool, error) {
		if kind == connKill {
			var n int
			if err := sc.Admin.QueryRow(ctx, `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
				WHERE application_name LIKE $1 AND datname = current_database()`, "durableq-worker-"+tag+"-%").Scan(&n); err != nil {
				return false, err
			}
			if n == 0 {
				return false, nil
			}
			rep.ConnKills++
			rep.ConnsKilled += n
			fmt.Fprintf(out, "%s terminated %d worker connections\n", stamp(), n)
			return true, nil
		}
		if kind == dbRestart {
			// Restart the whole server under the workers. The harness blocks
			// until Postgres accepts connections again; workers ride it out on
			// their own (claims fail and retry, leases may expire and be
			// reclaimed, in-flight commits may be lost or ambiguous).
			t0 := time.Now()
			var before, after time.Time
			if err := pool.QueryRow(ctx, `SELECT pg_postmaster_start_time()`).Scan(&before); err != nil {
				return false, err
			}
			if out, err := exec.CommandContext(ctx, "sh", "-c", cfg.DBRestartCmd).CombinedOutput(); err != nil {
				return false, fmt.Errorf("postgres restart command: %v\n%s", err, out)
			}
			if err := sc.Reconnect(ctx, time.Minute); err != nil {
				return false, err
			}
			if err := pool.QueryRow(ctx, `SELECT pg_postmaster_start_time()`).Scan(&after); err != nil {
				return false, err
			}
			if !after.After(before) {
				return false, fmt.Errorf("postgres restart command ran but the server did not restart")
			}
			rep.DBRestarts++
			fmt.Fprintf(out, "%s restarted Postgres (back after %.1fs)\n", stamp(), time.Since(t0).Seconds())
			return true, nil
		}
		var live []*proc
		for _, p := range procs {
			if p.live() {
				live = append(live, p)
			}
		}
		if len(live) == 0 {
			return false, nil
		}
		p := live[rng.IntN(len(live))]
		switch kind {
		case kill:
			if p.cmd.Process.Kill() != nil {
				return false, nil
			}
			p.stopped = true
			rep.Kills++
			fmt.Fprintf(out, "%s SIGKILL worker-%d (pid %d) … respawned\n", stamp(), p.slot, p.cmd.Process.Pid)
		case restart:
			if p.cmd.Process.Signal(syscall.SIGTERM) != nil {
				return false, nil
			}
			p.stopped = true
			rep.Restarts++
			fmt.Fprintf(out, "%s SIGTERM worker-%d (graceful restart: in-flight jobs released)\n", stamp(), p.slot)
		default:
			d := cfg.Lease + cfg.Lease/2
			if p.cmd.Process.Signal(syscall.SIGSTOP) != nil {
				return false, nil
			}
			p.stalled.Store(true)
			time.AfterFunc(d, func() { p.cmd.Process.Signal(syscall.SIGCONT); p.stalled.Store(false) })
			rep.Stalls++
			fmt.Fprintf(out, "%s SIGSTOP worker-%d for %v (past its lease)\n", stamp(), p.slot, d)
		}
		return true, nil
	}
	next := 0
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	drainBy := time.Now().Add(cfg.DrainTimeout)

	for {
		select {
		case <-ctx.Done():
			return rep, ctx.Err()
		case e := <-exits:
			if procs[e.p.slot] != e.p {
				continue
			}
			if !e.p.stopped {
				rep.SelfKills++
			}
			if err := spawn(e.p.slot); err != nil {
				return rep, err
			}
		case <-poll.C:
			n, err := pending()
			if err != nil {
				return rep, err
			}
			for next < len(at) && float64(int64(cfg.Jobs)-n) >= at[next]*float64(cfg.Jobs) {
				ok, err := inject(kinds[next])
				if err != nil {
					return rep, err
				}
				if !ok {
					break // no live target right now; retry next tick
				}
				next++
			}
			if n == 0 {
				rep.Elapsed = time.Since(start)
				rep.Shortfall = len(at) - next
				stopAll()
				if n, err = pending(); err != nil || n != 0 {
					return rep, fmt.Errorf("%d jobs became pending after workers stopped (err %v)", n, err)
				}
				rep.StaleRejected, rep.LeaseLost = stale.Load(), leaseLost.Load()
				if rep.Metrics, err = ReadMetrics(ctx, pool); err != nil {
					return rep, err
				}
				rep.Checks, err = Check(ctx, pool)
				return rep, err
			}
			if time.Now().After(drainBy) {
				return rep, fmt.Errorf("not drained after %v: %d jobs pending (seed %d, schema %s kept for inspection)",
					cfg.DrainTimeout, n, cfg.Seed, rep.Schema)
			}
		}
	}
}
