package demo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// ChaosConfig drives one chaos run. Targets (Kills, ConnKills, Stalls) are
// injected at random intervals averaging FaultEvery, until met or the queue
// drains.
type ChaosConfig struct {
	DatabaseURL string // URL form; the run uses its own fresh schema
	WorkerBin   string // durableq binary used to spawn worker processes
	Seed        uint64 // drives the fault schedule (workers' own randomness is not seeded)

	Jobs, Workers, Concurrency int
	SleepMS, MaxAttempts       int
	FailRate                   float64
	Lease, Heartbeat           time.Duration
	Failpoints                 string // DURABLEQ_FAILPOINTS for workers

	Kills, ConnKills, Stalls int
	FaultEvery               time.Duration
	DrainTimeout             time.Duration
	KeepSchema               bool
	Out                      io.Writer // progress output
}

type ChaosReport struct {
	Seed                                     uint64
	Schema                                   string
	Elapsed                                  time.Duration
	Kills, SelfKills, ConnKills, ConnsKilled int
	Stalls                                   int
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
	fmt.Fprintf(w, "  Jobs succeeded       %-8d dead %d\n", m.Succeeded, m.Dead)
	fmt.Fprintf(w, "  Worker kills         %-8d self-kills (failpoints) %d\n", r.Kills, r.SelfKills)
	fmt.Fprintf(w, "  Connection kills     %-8d (%d connections terminated)\n", r.ConnKills, r.ConnsKilled)
	fmt.Fprintf(w, "  Worker stalls        %d\n", r.Stalls)
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
	slot   int
	cmd    *exec.Cmd
	killed bool          // SIGKILLed by the harness (vs. a failpoint self-kill)
	done   chan struct{} // closed when the process has exited
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
	pool, admin, dbURL := sc.Pool, sc.Admin, sc.URL
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
	var stale, leaseLost atomic.Int64
	exits := make(chan exit)
	stopped := make(chan struct{})
	procs := make([]*proc, cfg.Workers)
	spawn := func(slot int) error {
		cmd := exec.Command(cfg.WorkerBin, "worker",
			"--concurrency", strconv.Itoa(cfg.Concurrency),
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
			cmd.Wait()
			close(p.done)
			select {
			case exits <- exit{p}:
			case <-stopped:
			}
		}()
		return nil
	}
	for i := range procs {
		if err := spawn(i); err != nil {
			return rep, err
		}
	}
	defer func() { // stop every worker, even on error
		close(stopped)
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
	}()

	pending := func() (int64, error) {
		var n int64
		err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state IN ('queued', 'running')`).Scan(&n)
		return n, err
	}
	faultsDone := func() bool {
		return rep.Kills >= cfg.Kills && rep.ConnKills >= cfg.ConnKills && rep.Stalls >= cfg.Stalls
	}
	nextFault := func() <-chan time.Time {
		if faultsDone() || cfg.FaultEvery <= 0 {
			return nil
		}
		return time.After(time.Duration(rng.Float64() * 2 * float64(cfg.FaultEvery)))
	}
	fault := nextFault()
	poll := time.NewTicker(250 * time.Millisecond)
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
			if !e.p.killed {
				rep.SelfKills++
			}
			if err := spawn(e.p.slot); err != nil {
				return rep, err
			}
		case <-fault:
			// Pick among the faults whose target isn't met yet, weighted by what's left.
			left := []int{cfg.Kills - rep.Kills, cfg.ConnKills - rep.ConnKills, cfg.Stalls - rep.Stalls}
			total := 0
			for i := range left {
				left[i] = max(left[i], 0)
				total += left[i]
			}
			pick := rng.IntN(total)
			p := procs[rng.IntN(len(procs))]
			switch {
			case pick < left[0]:
				p.killed = true
				p.cmd.Process.Kill()
				rep.Kills++
				fmt.Fprintf(out, "%s SIGKILL worker-%d (pid %d) … respawned\n", stamp(), p.slot, p.cmd.Process.Pid)
			case pick < left[0]+left[1]:
				var n int
				if err := admin.QueryRow(ctx, `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
					WHERE application_name LIKE 'durableq-worker-%' AND datname = current_database()`).Scan(&n); err != nil {
					return rep, err
				}
				rep.ConnKills++
				rep.ConnsKilled += n
				fmt.Fprintf(out, "%s terminated %d worker connections\n", stamp(), n)
			default:
				stall := cfg.Lease + cfg.Lease/2
				p.cmd.Process.Signal(syscall.SIGSTOP)
				time.AfterFunc(stall, func() { p.cmd.Process.Signal(syscall.SIGCONT) })
				rep.Stalls++
				fmt.Fprintf(out, "%s SIGSTOP worker-%d for %v (past its lease)\n", stamp(), p.slot, stall)
			}
			fault = nextFault()
		case <-poll.C:
			n, err := pending()
			if err != nil {
				return rep, err
			}
			if n == 0 {
				rep.Elapsed = time.Since(start)
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
