import results from '../data/results.json';

export const bare = results.process;
export const k8s = results.k8s;

export const nf = (n: number, digits = 0): string =>
  n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });

/** k8s matrix rows for one work-per-job setting, ordered by replica count. */
export const rows = (sleepMs: number) =>
  k8s.matrix.filter((r) => r.sleep_ms === sleepMs).sort((a, b) => a.replicas - b.replicas);

/** Throughput at the largest replica count divided by throughput at one replica. */
export const speedup = (sleepMs: number): number => {
  const r = rows(sleepMs);
  return r[r.length - 1].jobs_per_second / r[0].jobs_per_second;
};

export const row = (sleepMs: number, replicas: number) => {
  const r = k8s.matrix.find((m) => m.sleep_ms === sleepMs && m.replicas === replicas);
  if (!r) throw new Error(`no measured cell for ${sleepMs}ms x ${replicas}`);
  return r;
};

export const peakProcess = bare.throughput.reduce((a, b) => (b.jobs_per_sec > a.jobs_per_sec ? b : a));
