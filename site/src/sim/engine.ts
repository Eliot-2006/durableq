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
  throw new Error('not implemented: task W3');
}

/** Advances to the next event of the scripted scenario. A no-op once done. */
export function step(s: SimState): SimState {
  throw new Error('not implemented: task W3');
}

/**
 * Stalls the worker currently holding the lease (a long GC pause or network
 * partition: to the database this looks like a crash). Allowed only while a
 * worker holds the job; otherwise returns the state unchanged.
 */
export function injectFailure(s: SimState): SimState {
  throw new Error('not implemented: task W3');
}

/** Whether injectFailure would change the state right now. */
export function canInjectFailure(s: SimState): boolean {
  throw new Error('not implemented: task W3');
}
