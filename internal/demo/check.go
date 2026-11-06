package demo

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Invariant is one PRD §9 check: Query counts violating rows (0 = pass).
type Invariant struct {
	ID, Desc, Query string
}

var Invariants = []Invariant{
	{"I1", "no lost jobs: every job is terminal",
		`SELECT count(*) FROM jobs WHERE state NOT IN ('succeeded', 'dead')`},
	{"I2", "no duplicate effects",
		`SELECT count(*) FROM (SELECT job_id FROM effects GROUP BY 1 HAVING count(*) > 1) x`},
	{"I3", "every succeeded demo.effect job has exactly one effect, and every effect's job succeeded",
		`SELECT (SELECT count(*) FROM jobs j WHERE j.kind = 'demo.effect' AND j.state = 'succeeded'
		           AND NOT EXISTS (SELECT 1 FROM effects e WHERE e.job_id = j.id))
		      + (SELECT count(*) FROM effects e JOIN jobs j ON j.id = e.job_id WHERE j.state <> 'succeeded')`},
	{"I4", "dead jobs have no effects",
		`SELECT count(*) FROM effects e JOIN jobs j ON j.id = e.job_id WHERE j.state = 'dead'`},
	{"I5", "each effect was written by the job's final owner",
		`SELECT count(*) FROM effects e JOIN jobs j ON j.id = e.job_id WHERE e.attempt <> j.attempt`},
	{"I6", "attempt budget respected",
		`SELECT count(*) FROM (SELECT a.job_id FROM job_attempts a JOIN jobs j ON j.id = a.job_id
		   WHERE a.outcome IS NOT NULL GROUP BY a.job_id, j.max_attempts HAVING count(*) > j.max_attempts) x`},
	{"I7", "at most one terminal attempt per job",
		`SELECT count(*) FROM (SELECT job_id FROM job_attempts GROUP BY 1
		   HAVING count(*) FILTER (WHERE outcome IN ('succeeded', 'dead')) > 1) x`},
}

type CheckResult struct {
	Invariant
	Violations int64
}

// Check runs every invariant. Meaningful only once the queue is drained.
func Check(ctx context.Context, pool *pgxpool.Pool) ([]CheckResult, error) {
	var out []CheckResult
	for _, inv := range Invariants {
		r := CheckResult{Invariant: inv}
		if err := pool.QueryRow(ctx, inv.Query).Scan(&r.Violations); err != nil {
			return nil, fmt.Errorf("%s: %w", inv.ID, err)
		}
		out = append(out, r)
	}
	return out, nil
}

// PrintChecks writes one line per invariant and reports whether all passed.
func PrintChecks(w io.Writer, rs []CheckResult) bool {
	ok := true
	for _, r := range rs {
		status := "PASS"
		if r.Violations != 0 {
			status, ok = fmt.Sprintf("FAIL (%d violations)", r.Violations), false
		}
		fmt.Fprintf(w, "  %s %-4s %s\n", r.ID, status, r.Desc)
	}
	return ok
}

// Metrics are reported alongside the invariants; they are not pass/fail.
type Metrics struct {
	Succeeded, Dead, Retries, Released, Abandoned, DuplicateExecutions int64
	DeadByBudget                                                       int64 // dead because abandoned attempts (crashes, lost leases) used up the budget, not handler failures
}

func ReadMetrics(ctx context.Context, pool *pgxpool.Pool) (Metrics, error) {
	var m Metrics
	err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM jobs WHERE state = 'succeeded'),
		(SELECT count(*) FROM jobs WHERE state = 'dead'),
		(SELECT count(*) FROM job_attempts WHERE outcome = 'failed'),
		(SELECT count(*) FROM job_attempts WHERE outcome = 'released'),
		-- an attempt with no outcome that is not the job's current one was abandoned
		(SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
		   WHERE a.outcome IS NULL AND a.attempt < j.attempt),
		-- jobs whose handler may have run more than once: an abandoned attempt, then another attempt
		(SELECT count(DISTINCT a.job_id) FROM job_attempts a JOIN jobs j ON j.id = a.job_id
		   WHERE a.outcome IS NULL AND a.attempt < j.attempt),
		(SELECT count(*) FROM jobs WHERE state = 'dead' AND last_error = 'permanent: attempt budget exhausted')`).
		Scan(&m.Succeeded, &m.Dead, &m.Retries, &m.Released, &m.Abandoned, &m.DuplicateExecutions, &m.DeadByBudget)
	return m, err
}
