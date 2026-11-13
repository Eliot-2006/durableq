//go:build chaos

// Chaos tests: real worker processes, real SIGKILL/SIGSTOP, real
// pg_terminate_backend, and (opt-in) Postgres server restarts. Run with:
//
//	go test -tags chaos ./test/chaos/...                                  # short (CI)
//	go test -tags chaos ./test/chaos/ -run TestChaos -timeout 0 -v -args -jobs 10000 -kills 100 -conn-kills 20 -sleep-ms 100 -seeds 20
package chaos

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Eliot-2006/durableq/internal/demo"
	"github.com/Eliot-2006/durableq/internal/failpoint"
)

var (
	jobs      = flag.Int("jobs", 1000, "jobs per run")
	workers   = flag.Int("workers", 6, "worker processes")
	kills     = flag.Int("kills", 20, "SIGKILLs per run")
	connKills = flag.Int("conn-kills", 5, "connection-kill rounds per run")
	stalls    = flag.Int("stalls", 2, "SIGSTOP stalls per run")
	restarts  = flag.Int("restarts", 3, "graceful SIGTERM restarts per run")
	seeds     = flag.Int("seeds", 1, "consecutive seeds to run")
	seed      = flag.Uint64("seed", 0, "first seed (0 = time-based)")
	sleepMS   = flag.Int("sleep-ms", 20, "demo.effect work time")
)

const allFailpoints = "after_claim=0.002,during_run=0.002,before_complete=0.002,in_complete_tx=0.002,after_commit=0.002"

func setup(t *testing.T) (dbURL, bin string) {
	dbURL = os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set")
	}
	bin = filepath.Join(t.TempDir(), "durableq")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/durableq").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return dbURL, bin
}

func base(dbURL, bin string, s uint64) demo.ChaosConfig {
	return demo.ChaosConfig{
		DatabaseURL: dbURL, WorkerBin: bin, Seed: s,
		Jobs: *jobs, Workers: *workers, Concurrency: 10,
		SleepMS: *sleepMS, FailRate: 0.05, MaxAttempts: 5,
		Lease: 2 * time.Second, Heartbeat: 500 * time.Millisecond,
		DrainTimeout: 10 * time.Minute,
	}
}

func run(t *testing.T, cfg demo.ChaosConfig) demo.ChaosReport {
	t.Helper()
	rep, err := demo.RunChaos(context.Background(), cfg)
	if err != nil {
		t.Fatalf("seed %d: %v", cfg.Seed, err)
	}
	if testing.Verbose() || !rep.Passed() {
		rep.Print(tWriter{t})
	}
	if !rep.Passed() {
		t.Fatalf("invariants failed; reproduce with -seed %d", cfg.Seed)
	}
	return rep
}

type tWriter struct{ t *testing.T }

func (w tWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

// TestChaos: SIGKILLs, connection kills, stalls, and every failpoint at once.
func TestChaos(t *testing.T) {
	dbURL, bin := setup(t)
	first := *seed
	if first == 0 {
		first = uint64(time.Now().UnixNano())
	}
	for i := range *seeds {
		cfg := base(dbURL, bin, first+uint64(i))
		cfg.Kills, cfg.ConnKills, cfg.Stalls, cfg.Restarts, cfg.Failpoints = *kills, *connKills, *stalls, *restarts, allFailpoints
		rep := run(t, cfg)
		// The run must actually have injected what it claims.
		if rep.Shortfall != 0 {
			t.Fatalf("seed %d: %d scheduled faults never landed (kills %d/%d, conn kills %d/%d, stalls %d/%d, restarts %d/%d)",
				cfg.Seed, rep.Shortfall, rep.Kills, cfg.Kills, rep.ConnKills, cfg.ConnKills, rep.Stalls, cfg.Stalls, rep.Restarts, cfg.Restarts)
		}
		if cfg.Restarts > 0 && rep.Metrics.Released == 0 {
			t.Fatalf("seed %d: graceful restarts released no jobs; the release path was not exercised", cfg.Seed)
		}
	}
}

// TestEachFailpoint isolates one failpoint at a time with a high probability,
// so every crash site is exercised deterministically often.
func TestEachFailpoint(t *testing.T) {
	dbURL, bin := setup(t)
	for _, name := range failpoint.Names {
		t.Run(name, func(t *testing.T) {
			cfg := base(dbURL, bin, uint64(time.Now().UnixNano()))
			cfg.Jobs, cfg.Workers, cfg.Failpoints = 300, 3, name+"=0.03"
			rep := run(t, cfg)
			if rep.SelfKills == 0 {
				t.Fatalf("failpoint %s never fired", name)
			}
		})
	}
}

// TestStalledWorkerFenced SIGSTOPs workers holding jobs past their lease. On
// resume, a worker usually notices its local deadline passed and abandons the
// job; if its handler result wins that race, the completion reaches the
// database and is fenced out. Either way only the new owner's completion
// counts (I2/I5).
func TestStalledWorkerFenced(t *testing.T) {
	dbURL, bin := setup(t)
	cfg := base(dbURL, bin, uint64(time.Now().UnixNano()))
	// Jobs (1–3s) outlast the stall (1.5 × lease), so a resumed worker can try
	// to complete while the new owner is still running: the window where a
	// missing attempt check would commit a stale effect (caught by I5).
	cfg.Jobs, cfg.Workers, cfg.SleepMS, cfg.FailRate = 600, 4, 2000, 0
	cfg.Stalls = 15
	rep := run(t, cfg)
	if rep.LeaseLost+rep.StaleRejected == 0 {
		t.Fatal("stalled workers neither abandoned nor were fenced out of any job; the stall exercised nothing")
	}
	t.Logf("leases lost: %d, stale transitions rejected: %d", rep.LeaseLost, rep.StaleRejected)
}

// TestGracefulRestartsKeepBudget: SIGTERM releases in-flight jobs. With a
// budget of one attempt, every released job must still run and succeed (a
// release must not push a job toward dead; I6 must hold).
func TestGracefulRestartsKeepBudget(t *testing.T) {
	dbURL, bin := setup(t)
	cfg := base(dbURL, bin, uint64(time.Now().UnixNano()))
	cfg.Jobs, cfg.Workers, cfg.MaxAttempts, cfg.FailRate, cfg.SleepMS = 1500, 4, 1, 0, 100
	cfg.Restarts = 20
	rep := run(t, cfg)
	if rep.Metrics.Released == 0 {
		t.Fatal("no job was released; the restarts exercised nothing")
	}
	if rep.Metrics.Dead != 0 {
		t.Fatalf("%d jobs dead-lettered by graceful restarts", rep.Metrics.Dead)
	}
	t.Logf("jobs released by graceful restarts: %d, all succeeded", rep.Metrics.Released)
}

// TestPostgresRestart restarts the Postgres server itself mid-run, on top of
// worker SIGKILLs. Workers must ride out the outage (failed claims,
// heartbeats and commits) and every invariant must still hold. Needs a
// command that restarts the server, so it is opt-in:
//
//	DURABLEQ_PG_RESTART_CMD='docker restart -t 0 durableq-postgres-1' go test -tags chaos ./test/chaos/ -run TestPostgresRestart -v
func TestPostgresRestart(t *testing.T) {
	cmd := os.Getenv("DURABLEQ_PG_RESTART_CMD")
	if cmd == "" {
		t.Skip("DURABLEQ_PG_RESTART_CMD not set")
	}
	dbURL, bin := setup(t)
	cfg := base(dbURL, bin, uint64(time.Now().UnixNano()))
	cfg.Jobs, cfg.Workers, cfg.SleepMS = 3000, 4, 50
	cfg.Kills, cfg.DBRestarts, cfg.DBRestartCmd = 10, 3, cmd
	rep := run(t, cfg)
	if rep.Shortfall != 0 {
		t.Fatalf("%d scheduled faults never landed (postgres restarts %d/%d)", rep.Shortfall, rep.DBRestarts, cfg.DBRestarts)
	}
}
