// Deterministic, educational model of one DurableQ job moving through a
// worker stall, lease expiry, reclaim and a fenced (rejected) stale
// completion. It is a SIMULATION of the semantics in docs/design.md, not a
// connection to a running system.
//
// CONTRACT (the UI in site/src/components depends on exactly these exports).
// Pure functions: every call returns a new state and never mutates its input.

export type JobState = 'queued' | 'running' | 'succeeded' | 'dead';
export type WorkerId = 'A' | 'B';
export type WorkerStatus = 'idle' | 'working' | 'stalled' | 'stopped';
export type LogKind = 'info' | 'ok' | 'reject' | 'fault';

export interface SimConfig {
  /** Lease duration in simulated seconds (the chart default is 30). */
  lease: number;
  /** Heartbeat interval in simulated seconds (must be < lease / 2). */
  heartbeat: number;
  /** How long a handler runs before it tries to complete. */
  handlerSeconds: number;
}

export interface Attempt {
  attempt: number;
  worker: WorkerId;
  claimedAt: number;
  /** null while the attempt is still open; 'abandoned' once a newer attempt exists. */
  outcome: null | 'succeeded' | 'abandoned';
}

export interface Effect {
  attempt: number;
  worker: WorkerId;
}

export interface LogEntry {
  /** Simulated database-clock seconds. */
  t: number;
  kind: LogKind;
  text: string;
  /** Optional SQL-ish detail shown on demand, e.g. the fenced UPDATE and its row count. */
  detail?: string;
}

export interface Invariant {
  id: 'I1' | 'I2' | 'I5' | 'I7';
  label: string;
  ok: boolean;
}

export interface SimState {
  config: SimConfig;
  /** Database clock, simulated seconds since the job was enqueued. */
  t: number;
  job: {
    id: number;
    state: JobState;
    attempt: number;
    leaseOwner: WorkerId | null;
    leaseExpiresAt: number | null;
  };
  workers: Record<WorkerId, { status: WorkerStatus; heldAttempt: number | null }>;
  attempts: Attempt[];
  effects: Effect[];
  log: LogEntry[];
  invariants: Invariant[];
  /** True once the scripted scenario has finished. */
  done: boolean;
  /** Short caption describing the step that produced this state. */
  caption: string;
}

export const defaultConfig: SimConfig = { lease: 30, heartbeat: 10, handlerSeconds: 45 };

/** The job is enqueued; no worker holds it. */
export function initialState(config: SimConfig = defaultConfig): SimState {
  for (const key of ['lease', 'heartbeat', 'handlerSeconds'] as const) {
    if (!Number.isFinite(config[key]) || config[key] <= 0) {
      throw new Error(`${key} must be a finite, positive number of seconds`);
    }
  }
  if (config.heartbeat >= config.lease / 2) {
    throw new Error('heartbeat must be less than half the lease');
  }
  return record({
    config: { ...config },
    t: 0,
    job: { id: 1, state: 'queued', attempt: 0, leaseOwner: null, leaseExpiresAt: null },
    workers: {
      A: { status: 'idle', heldAttempt: null },
      B: { status: 'idle', heldAttempt: null },
    },
    attempts: [], effects: [], log: [], invariants: [], done: false, caption: '',
  }, 'info', 'Job enqueued; attempt 0.');
}

/** Advances to the next event of the scripted scenario. A no-op once done. */
export function step(s: SimState): SimState {
  if (s.done) return s;
  const next = structuredClone(s);
  if (next.job.state === 'queued') return claim(next, 'A');

  const { lease, heartbeat: interval, handlerSeconds } = next.config;
  const heartbeatAt = next.job.leaseExpiresAt! - lease + interval;
  if (next.workers.A.status === 'working') {
    const claimedAt = next.attempts.find(a => a.worker === 'A')!.claimedAt;
    // Stall before the handler finishes, at half a lease with the defaults.
    const stallAt = claimedAt + Math.min(lease / 2, handlerSeconds / 2);
    if (heartbeatAt < stallAt) return heartbeat(next, 'A', heartbeatAt);
    next.t = Math.max(next.t, stallAt);
    return stall(next);
  }

  if (next.workers.B.status === 'idle') {
    const expiresAt = next.job.leaseExpiresAt!;
    if (next.t < expiresAt) {
      next.t = expiresAt;
      return record(next, 'info', "A's lease expired; job still running.",
        'No reaper or UPDATE. Renewals require lease_expires_at > clock_timestamp(); '
        + 'claims require lease_expires_at < clock_timestamp(), so reclaim waits for a later poll.');
    }
    // An illustrative poll one second later satisfies qClaim's strict < fence.
    next.t = Math.max(next.t, expiresAt + 1);
    return claim(next, 'B');
  }

  if (next.workers.B.status === 'working') {
    const claimedAt = next.attempts.find(a => a.worker === 'B')!.claimedAt;
    const completeAt = claimedAt + handlerSeconds;
    const resumeAt = next.workers.A.status === 'stalled'
      ? claimedAt + handlerSeconds / 2 : Infinity;
    if (resumeAt <= heartbeatAt && resumeAt < completeAt) {
      next.t = Math.max(next.t, resumeAt);
      return complete(next, 'A');
    }
    if (heartbeatAt < completeAt) return heartbeat(next, 'B', heartbeatAt);
    next.t = Math.max(next.t, completeAt);
    return complete(next, 'B');
  }
  return s;
}

/**
 * Stalls the worker currently holding the lease (a long GC pause or network
 * partition: to the database this looks like a crash). Allowed only while A
 * holds an unexpired lease and has not stalled; otherwise returns unchanged.
 */
export function injectFailure(s: SimState): SimState {
  return canInjectFailure(s) ? stall(structuredClone(s)) : s;
}

/** Whether injectFailure would change the state right now. */
export function canInjectFailure(s: SimState): boolean {
  return !s.done && s.job.state === 'running' && s.job.leaseOwner === 'A'
    && s.workers.A.status === 'working' && ownsLease(s, 'A');
}

// Helpers mutate only the fresh state allocated by an exported operation.
function record(s: SimState, kind: LogKind, text: string, detail?: string): SimState {
  s.caption = text;
  s.log.push({ t: s.t, kind, text, ...(detail === undefined ? {} : { detail }) });
  s.invariants = [
    { id: 'I1', label: 'no lost jobs: every job is terminal',
      ok: s.job.state === 'succeeded' || s.job.state === 'dead' },
    { id: 'I2', label: 'no duplicate effects', ok: s.effects.length <= 1 },
    { id: 'I5', label: "each effect was written by the job's final owner",
      ok: s.effects.every(e => e.attempt === s.job.attempt) },
    { id: 'I7', label: 'at most one terminal attempt per job',
      ok: s.attempts.filter(a => a.outcome === 'succeeded').length <= 1 },
  ];
  return s;
}

function ownsLease(s: SimState, worker: WorkerId): boolean {
  return s.job.state === 'running' && s.workers[worker].heldAttempt === s.job.attempt
    && s.job.leaseExpiresAt !== null && s.job.leaseExpiresAt > s.t;
}

function fence(s: SimState, worker: WorkerId): string {
  return `WHERE id = $1 AND attempt = ${s.workers[worker].heldAttempt} `
    + "AND state = 'running' AND lease_expires_at > clock_timestamp()";
}

function claim(s: SimState, worker: WorkerId): SimState {
  // In Postgres these old rows remain outcome=NULL; 'abandoned' is a derived display value.
  for (const attempt of s.attempts) {
    if (attempt.outcome === null) attempt.outcome = 'abandoned';
  }
  s.job = { ...s.job, state: 'running', attempt: s.job.attempt + 1,
    leaseOwner: worker, leaseExpiresAt: s.t + s.config.lease };
  s.workers[worker] = { status: 'working', heldAttempt: s.job.attempt };
  s.attempts.push({ attempt: s.job.attempt, worker, claimedAt: s.t, outcome: null });
  return record(s, 'info', `${worker} ${worker === 'A' ? 'claims' : 'reclaims'} attempt ${s.job.attempt}.`,
    "WITH next AS (SELECT id FROM jobs WHERE (state = 'queued' AND run_at <= clock_timestamp()) "
    + "OR (state = 'running' AND lease_expires_at < clock_timestamp()) FOR UPDATE SKIP LOCKED) "
    + `UPDATE jobs SET state = 'running', attempt = attempt + 1, lease_owner = '${worker}', `
    + `lease_expires_at = clock_timestamp() + interval '${s.config.lease} seconds'; `
    + 'INSERT INTO job_attempts (...); -- same short transaction');
}

function stall(s: SimState): SimState {
  s.workers.A.status = 'stalled';
  return record(s, 'fault', 'A stalls; no more heartbeats.',
    'A long GC pause or network partition sends nothing to Postgres. '
    + 'The database cannot distinguish a stalled worker from a crashed one; its lease is unchanged.');
}

function heartbeat(s: SimState, worker: WorkerId, at: number): SimState {
  s.t = Math.max(s.t, at);
  const sql = `UPDATE jobs SET lease_expires_at = clock_timestamp() + interval '${s.config.lease} seconds' `
    + fence(s, worker);
  if (!ownsLease(s, worker)) {
    s.workers[worker] = { status: 'stopped', heldAttempt: null };
    return record(s, 'reject', `${worker}'s heartbeat rejected.`, `${sql}; -- 0 rows; lease lost`);
  }
  s.job.leaseExpiresAt = s.t + s.config.lease;
  return record(s, 'ok', `${worker} renews attempt ${s.job.attempt}.`, `${sql}; -- 1 row`);
}

function complete(s: SimState, worker: WorkerId): SimState {
  const attempt = s.workers[worker].heldAttempt;
  const sql = "UPDATE jobs SET state = 'succeeded', lease_owner = NULL, lease_expires_at = NULL "
    + fence(s, worker);
  const resumeDetail = worker === 'A'
    ? ' In practice a resumed worker usually abandons on its local deadline first. '
      + 'Anything its handler did outside the database may already have happened; '
      + 'execution is at-least-once, so handlers must be idempotent.' : '';
  if (!ownsLease(s, worker)) {
    s.workers[worker] = { status: 'stopped', heldAttempt: null };
    s.done = worker === 'B';
    return record(s, 'reject', `${worker}'s stale completion rejected.`,
      `BEGIN; ${sql}; -- 0 rows\nROLLBACK; no CommitFunc effect commits. `
      + 'The fenced UPDATE runs first, so the CommitFunc insert is never executed.' + resumeDetail);
  }
  s.job = { ...s.job, state: 'succeeded', leaseOwner: null, leaseExpiresAt: null };
  s.effects.push({ attempt: attempt!, worker });
  s.attempts.find(a => a.attempt === attempt)!.outcome = 'succeeded';
  s.workers[worker] = { status: 'idle', heldAttempt: null };
  s.done = true;
  return record(s, 'ok', `${worker} completes; transactional effect committed.`,
    `BEGIN; ${sql}; -- 1 row\nINSERT INTO effects (job_id, attempt) VALUES ($1, ${attempt}); `
    + '-- CommitFunc\nUPDATE job_attempts SET outcome = \'succeeded\' ...; COMMIT; '
    + 'Completion and transactional effects commit together.' + resumeDetail);
}
