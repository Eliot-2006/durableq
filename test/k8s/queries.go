//go:build k8s

package k8s

import (
	"fmt"
	"strings"
)

type claim struct {
	ID      int64 `json:"id"`
	Attempt int   `json:"attempt"`
}

const qReset = `TRUNCATE jobs, job_attempts, effects RESTART IDENTITY`
const qProgress = `SELECT json_build_object('total', count(*), 'done', count(*) FILTER (WHERE state IN ('succeeded','dead')), 'running', count(*) FILTER (WHERE state='running')) FROM jobs`
const qMetrics = `SELECT json_build_object(
 'total', (SELECT count(*) FROM jobs),
 'succeeded', (SELECT count(*) FROM jobs WHERE state='succeeded'),
 'dead', (SELECT count(*) FROM jobs WHERE state='dead'),
 'effects', (SELECT count(*) FROM effects),
 'released', (SELECT count(*) FROM job_attempts WHERE outcome='released'),
 'retried_attempts', (SELECT count(*) FROM job_attempts WHERE outcome='failed'),
 'failed_attempts', (SELECT count(*) FROM job_attempts WHERE outcome IN ('failed','dead')),
 'attempts', (SELECT count(*) FROM job_attempts),
 'abandoned_attempts', (SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.outcome IS NULL AND a.attempt<j.attempt),
 'duplicate_executions', (SELECT count(DISTINCT a.job_id) FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.outcome IS NULL AND a.attempt<j.attempt))`
const qConnections = `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()`
const qKillConnections = `SELECT count(*) FROM (SELECT pg_terminate_backend(pid) AS killed FROM pg_stat_activity WHERE application_name LIKE 'durableq-worker-%' AND pid<>pg_backend_pid()) s WHERE killed`
const qPostmaster = `SELECT pg_postmaster_start_time()::text`
const qBench = `SELECT json_build_object(
 'duration_seconds', (SELECT extract(epoch FROM max(finished_at)-min(run_at)) FROM jobs),
 'jobs_per_second', (SELECT count(*)/NULLIF(extract(epoch FROM max(finished_at)-min(run_at)),0) FROM jobs),
 'processing_ms', (SELECT percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at-claimed_at)*1000) FROM job_attempts WHERE finished_at IS NOT NULL),
 'queue_wait_ms', (SELECT percentile_cont(ARRAY[0.5,0.95,0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM a.claimed_at-j.run_at)*1000) FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.attempt=1),
 'finished_at', (SELECT max(finished_at) FROM jobs))`
const qFinished = `SELECT to_json(max(finished_at)) FROM jobs`

func activeQuery(pod string) string {
	return fmt.Sprintf(`SELECT COALESCE(json_agg(json_build_object('id',id,'attempt',attempt)), '[]') FROM jobs WHERE state='running' AND lease_owner LIKE %s`, sqlQuote("%"+pod+"%"))
}
func recoveryQuery(jobs []claim) string {
	parts := make([]string, len(jobs))
	for i, j := range jobs {
		parts[i] = fmt.Sprintf("(%d,%d)", j.ID, j.Attempt)
	}
	return fmt.Sprintf(`WITH captured AS (
 SELECT a.job_id AS id,a.attempt,a.outcome,
 (SELECT min(n.claimed_at) FROM job_attempts n WHERE n.job_id=a.job_id AND n.attempt>a.attempt) AS next_at
 FROM job_attempts a JOIN (VALUES %s) AS killed(id,attempt) ON a.job_id=killed.id AND a.attempt=killed.attempt)
 SELECT json_build_object(
 'pending',count(*) FILTER (WHERE outcome IS NULL AND next_at IS NULL),
 'abandoned',COALESCE(json_agg(json_build_object('id',id,'attempt',attempt)) FILTER (WHERE outcome IS NULL),'[]'::json),
 'claims',COALESCE(json_agg(json_build_object('id',id,'claimed_at',next_at)) FILTER (WHERE outcome IS NULL AND next_at IS NOT NULL),'[]'::json)) FROM captured`, strings.Join(parts, ","))
}
func sqlQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// All benchmark jobs are present before this single gate opens.
const qStartBatch = `WITH due AS MATERIALIZED (SELECT clock_timestamp()+interval '3 seconds' AS at) UPDATE jobs SET run_at=due.at FROM due WHERE state='queued' AND attempt=0`

const qRecoveryFixture = `INSERT INTO jobs(kind,state,attempt,lease_owner,lease_expires_at) VALUES
 ('demo.effect','succeeded',1,NULL,NULL),
 ('demo.effect','running',2,'test',clock_timestamp()+interval '1 minute'),
 ('demo.effect','running',1,'test',clock_timestamp()+interval '1 minute');
 INSERT INTO job_attempts(job_id,attempt,worker_id,claimed_at,finished_at,outcome) VALUES
 (1,1,'killed',clock_timestamp()-interval '1 second',clock_timestamp(),'succeeded'),
 (2,1,'killed',clock_timestamp()-interval '31 seconds',NULL,NULL),
 (2,2,'replacement',clock_timestamp(),NULL,NULL),
 (3,1,'killed',clock_timestamp()-interval '31 seconds',NULL,NULL)`
const qRecoveryFixtureNext = `INSERT INTO job_attempts(job_id,attempt,worker_id,claimed_at) VALUES (3,2,'next',clock_timestamp())`

func killedClaimsQuery(reference claim, killedAt string) string {
	return fmt.Sprintf(`WITH killed AS (
 SELECT worker_id,%s::timestamptz AS at FROM job_attempts WHERE job_id=%d AND attempt=%d)
 SELECT COALESCE(json_agg(json_build_object('id',a.job_id,'attempt',a.attempt)),'[]'::json)
 FROM job_attempts a CROSS JOIN killed k
 WHERE a.worker_id=k.worker_id AND a.outcome IS NULL AND a.claimed_at<=k.at
 AND NOT EXISTS (SELECT 1 FROM job_attempts n WHERE n.job_id=a.job_id AND n.attempt>a.attempt AND n.claimed_at<=k.at)`, sqlQuote(killedAt), reference.ID, reference.Attempt)
}
