import assert from 'node:assert/strict';
import test from 'node:test';
import { canInjectFailure, defaultConfig, initialState, injectFailure, step } from './engine.ts';
import type { SimConfig, SimState } from './engine.ts';

function freeze<T>(value: T): T {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

function advance(s: SimState, action = step): SimState {
  const before = structuredClone(s);
  const next = action(freeze(s));
  assert.deepEqual(s, before, 'input state must not change');
  return next;
}

function run(s = initialState()): SimState[] {
  const states = [s];
  for (let i = 0; !s.done && i < 1000; i++) {
    s = advance(s);
    states.push(s);
  }
  assert.equal(s.done, true, 'scenario must finish');
  return states;
}

function verdicts(s: SimState) {
  return Object.fromEntries(s.invariants.map(({ id, ok }) => [id, ok]));
}

test('default run shows renewal, stall, expiry, reclaim, stale rejection and atomic completion', () => {
  const states = run();
  assert.deepEqual(states.map(s => s.t), [0, 0, 10, 15, 40, 41, 51, 61, 63.5, 71, 81, 86]);
  const [enqueued, claimed, renewed, stalled, expired, reclaimed] = states;
  assert.equal(enqueued.job.state, 'queued');
  assert.equal(enqueued.job.attempt, 0);
  assert.deepEqual(claimed.job, {
    id: 1, state: 'running', attempt: 1, leaseOwner: 'A', leaseExpiresAt: 30,
  });
  assert.deepEqual(claimed.attempts, [{ attempt: 1, worker: 'A', claimedAt: 0, outcome: null }]);
  assert.match(claimed.log.at(-1)!.detail!, /FOR UPDATE SKIP LOCKED/);
  assert.equal(renewed.job.leaseExpiresAt, 40);
  assert.equal(stalled.workers.A.status, 'stalled');
  assert.deepEqual(expired.job, stalled.job, 'expiry does not change the database row');
  assert.equal(reclaimed.job.attempt, 2);
  assert.equal(reclaimed.job.leaseOwner, 'B');
  assert.equal(reclaimed.job.leaseExpiresAt, 71);
  assert.equal(reclaimed.attempts[0].outcome, 'abandoned');
  assert.equal(reclaimed.attempts[1].outcome, null);

  const final = states.at(-1)!;
  assert.deepEqual(final.job, {
    id: 1, state: 'succeeded', attempt: 2, leaseOwner: null, leaseExpiresAt: null,
  });
  assert.deepEqual(final.effects, [{ attempt: 2, worker: 'B' }]);
  assert.deepEqual(final.attempts.map(a => a.outcome), ['abandoned', 'succeeded']);
  assert.equal(final.workers.A.status, 'stopped');
  assert.equal(final.workers.B.status, 'idle');
  assert.deepEqual(verdicts(final), { I1: true, I2: true, I5: true, I7: true });
  assert.deepEqual(final.log.map(e => e.kind), [
    'info', 'info', 'ok', 'fault', 'info', 'info', 'ok', 'ok', 'reject', 'ok', 'ok', 'ok',
  ]);
  assert.match(final.log.at(-1)!.detail!, /UPDATE.*INSERT.*COMMIT/s);
});

test('failure injected immediately after claim removes the renewal and shifts recovery', () => {
  const claimed = advance(initialState());
  assert.equal(canInjectFailure(freeze(claimed)), true);
  const stalled = advance(claimed, injectFailure);
  assert.equal(stalled.t, 0);
  assert.equal(stalled.job.leaseExpiresAt, 30);
  const states = run(stalled);
  assert.deepEqual(states.slice(0, 3).map(s => s.t), [0, 30, 31]);
  assert.equal(states[2].job.leaseOwner, 'B');
  assert.equal(states.at(-1)!.t, 76);
  assert.deepEqual(states.at(-1)!.effects, [{ attempt: 2, worker: 'B' }]);
});

test('failure injection is refused before claim, after stall, at expiry, after reclaim and after done', () => {
  const states = run();
  const claimed = structuredClone(states[1]);
  claimed.t = 30;
  for (const s of [states[0], states[3], states[4], states[5], states.at(-1)!, claimed]) {
    assert.equal(canInjectFailure(freeze(s)), false);
    assert.deepEqual(advance(s, injectFailure), s);
  }
  assert.equal(canInjectFailure(states[2]), true, 'A can still stall after a renewal');
  const renewedStall = advance(states[2], injectFailure);
  assert.equal(renewedStall.t, 10);
  assert.equal(run(renewedStall)[2].t, 41);
});

test('step after done is a no-op', () => {
  const final = run().at(-1)!;
  assert.deepEqual(advance(final), final);
});

test('heartbeats cannot renew an expired lease, non-current attempt or terminal job', () => {
  for (const corrupt of [
    (s: SimState) => { s.t = 30; },
    (s: SimState) => { s.workers.A.heldAttempt = 0; },
    (s: SimState) => { s.job.state = 'succeeded'; },
  ]) {
    const claimed = advance(initialState());
    corrupt(claimed);
    const next = advance(claimed);
    assert.equal(next.log.at(-1)!.kind, 'reject');
    assert.deepEqual(next.job, claimed.job);
    assert.deepEqual(next.effects, []);
    assert.ok(next.t >= claimed.t, 'database clock never moves backwards');
  }
});

test('B renewals also fence the held attempt and lease', () => {
  for (const corrupt of [
    (s: SimState) => { s.t = 71; },
    (s: SimState) => { s.workers.B.heldAttempt = 1; },
  ]) {
    const reclaimed = structuredClone(run()[5]);
    corrupt(reclaimed);
    const next = advance(reclaimed);
    assert.equal(next.log.at(-1)!.kind, 'reject');
    assert.deepEqual(next.job, reclaimed.job);
    assert.deepEqual(next.effects, []);
  }
});

test('attempts are monotonic and expiry does not open a new attempt', () => {
  for (const states of [run(), run(advance(advance(initialState()), injectFailure))]) {
    for (let i = 1; i < states.length; i++) {
      assert.ok(states[i].job.attempt >= states[i - 1].job.attempt);
    }
    assert.deepEqual(states.at(-1)!.attempts.map(a => a.attempt), [1, 2]);
  }
});

test('stale completion rejects attempt 1 while B has a valid lease, without writing an effect', () => {
  const states = run();
  const index = states.findIndex(s => s.log.at(-1)?.kind === 'reject');
  assert.ok(index > 0);
  const rejected = states[index];
  assert.equal(rejected.job.state, 'running');
  assert.ok(rejected.job.leaseExpiresAt! > rejected.t);
  assert.deepEqual(rejected.job, states[index - 1].job);
  assert.deepEqual(rejected.attempts, states[index - 1].attempts);
  assert.deepEqual(rejected.effects, []);
  const detail = rejected.log.at(-1)!.detail!;
  assert.match(detail, /attempt\s*=\s*1/);
  assert.match(detail, /lease_expires_at > clock_timestamp\(\)/);
  assert.match(detail, /0 rows/);
  assert.match(detail, /ROLLBACK/);
  assert.match(detail, /CommitFunc/);
  assert.match(detail, /local deadline/i);
  assert.match(detail, /outside.*database/i);
  assert.match(detail, /idempotent/i);
});

test('B completion is fenced too and cannot write a stale effect', () => {
  const beforeComplete = structuredClone(run().at(-2)!);
  beforeComplete.workers.B.heldAttempt = 1;
  const next = advance(beforeComplete);
  assert.equal(next.log.at(-1)!.kind, 'reject');
  assert.deepEqual(next.effects, []);
  assert.deepEqual(next.job, beforeComplete.job);
});

test('invariants are recomputed from data, including corrupted effects and attempt outcomes', () => {
  assert.deepEqual(verdicts(initialState()), { I1: false, I2: true, I5: true, I7: true });
  const bad = structuredClone(run().at(-2)!);
  bad.effects = [{ attempt: 1, worker: 'A' }, { attempt: 1, worker: 'A' }];
  bad.attempts[0].outcome = 'succeeded';
  assert.deepEqual(verdicts(advance(bad)), { I1: true, I2: false, I5: false, I7: false });
  const good = structuredClone(run().at(-2)!);
  for (const invariant of good.invariants) invariant.ok = false;
  assert.deepEqual(verdicts(advance(good)), { I1: true, I2: true, I5: true, I7: true });
});

test('config must be finite and positive, with heartbeat strictly below half the lease', () => {
  for (const key of ['lease', 'heartbeat', 'handlerSeconds'] as const) {
    for (const value of [0, -1, NaN, Infinity, -Infinity, undefined]) {
      const config = { ...defaultConfig, [key]: value } as SimConfig;
      assert.throws(() => initialState(freeze(config)), new RegExp(key));
    }
  }
  for (const heartbeat of [15, 16]) {
    assert.throws(() => initialState({ ...defaultConfig, heartbeat }), /heartbeat.*lease/i);
  }
});

test('runs are deterministic, configs are copied and fractional timings work', () => {
  const config = freeze({ lease: 4.5, heartbeat: 0.75, handlerSeconds: 7.25 });
  const first = initialState(config);
  assert.notEqual(first.config, config);
  assert.deepEqual(run(first), run(initialState(config)));
  assert.deepEqual(run(), run());
  assert.equal(run(initialState({ lease: 30, heartbeat: 10, handlerSeconds: 1 })).at(-1)!.job.state, 'succeeded');
});
