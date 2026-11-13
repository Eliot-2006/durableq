import * as echarts from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import { AriaComponent, GridComponent, LegendComponent, MarkLineComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import type { EChartsCoreOption } from 'echarts/core';
import { bare, k8s, nf, rows, speedup } from '../lib/data';

echarts.use([BarChart, LineChart, AriaComponent, GridComponent, LegendComponent, MarkLineComponent, TooltipComponent, CanvasRenderer]);

// ---- theme: resolve CSS tokens (light-dark() included) to concrete colours ECharts can parse ----
const probe = document.createElement('span');
probe.style.display = 'none';
document.body.append(probe);
const tok = (name: string): string => {
  probe.style.color = `var(--${name})`;
  return getComputedStyle(probe).color;
};
const palette = () => ({
  bg: tok('bg'), ink: tok('ink'), ink2: tok('ink-2'), rule: tok('rule'), strong: tok('rule-strong'),
  c1: tok('c1'), c2: tok('c2'), c3: tok('c3'),
});
type C = ReturnType<typeof palette>;

const reduce = matchMedia('(prefers-reduced-motion: reduce)');
const sans = getComputedStyle(document.body).fontFamily;

const state = { work: 0, ref: false };

type P = { seriesName?: string; marker?: string; axisValueLabel?: string; name?: string; data?: any };

const base = (c: C, legend = true): EChartsCoreOption => ({
  animation: !reduce.matches,
  aria: { enabled: true },
  textStyle: { color: c.ink2, fontFamily: sans, fontSize: 12 },
  legend: { show: legend, top: 0, icon: 'roundRect', itemWidth: 14, itemHeight: 8, textStyle: { color: c.ink2, fontSize: 12 } },
  tooltip: { backgroundColor: c.bg, borderColor: c.strong, textStyle: { color: c.ink, fontSize: 12 }, confine: true },
});
const grid = (top = 56) => ({ left: 58, right: 22, top, bottom: 48 });
const axisLine = (c: C) => ({ lineStyle: { color: c.strong } });
const split = (c: C) => ({ lineStyle: { color: c.rule } });
const axisName = (c: C) => ({ nameTextStyle: { color: c.ink2, fontSize: 12 } });

// x axis that places replica counts at their true positions, labelling only the measured ones
const replicaAxis = (c: C) => ({
  type: 'value' as const, min: 0, max: 10.5, interval: 1, name: 'worker replicas', nameLocation: 'middle' as const, nameGap: 30,
  ...axisName(c), axisLine: axisLine(c), splitLine: { show: false },
  axisLabel: { color: c.ink2, formatter: (v: number) => ([1, 2, 5, 10].includes(v) ? String(v) : '') },
});

// ---- charts ----
const throughput = (c: C): EChartsCoreOption => {
  const series: any[] = [];
  const color = { 0: c.c1, 50: c.c2 } as Record<number, string>;
  for (const ms of [0, 50]) {
    const r = rows(ms);
    series.push({
      name: `${ms} ms of work per job`, type: 'line', symbolSize: 9, lineStyle: { width: 2.5 }, itemStyle: { color: color[ms] },
      data: r.map((m) => ({ value: [m.replicas, m.jobs_per_second], m })),
      endLabel: { show: true, color: color[ms], fontWeight: 600, formatter: `${nf(speedup(ms), 1)}× vs 1 replica` },
    });
    if (state.ref) {
      const first = r[0].jobs_per_second;
      series.push({
        name: `${ms} ms · linear reference, not measured`, type: 'line', symbol: 'none', silent: true, tooltip: { show: false },
        lineStyle: { type: 'dashed', width: 1.5, color: color[ms], opacity: 0.6 }, itemStyle: { color: color[ms] },
        data: [[1, first], [10, first * 10]],
      });
    }
  }
  return {
    ...base(c), grid: { ...grid(), right: 110 },
    tooltip: {
      ...(base(c).tooltip as object), trigger: 'item',
      formatter: (p: P) => `${p.marker ?? ''}<b>${p.seriesName}</b><br/>${p.data.m.replicas} replicas: <b>${nf(p.data.m.jobs_per_second, 1)}</b> jobs/s<br/>${nf(p.data.m.jobs)} jobs · ${nf(p.data.m.jobs_per_second / rows(p.data.m.sleep_ms)[0].jobs_per_second, 1)}× the 1-replica rate`,
    },
    xAxis: replicaAxis(c),
    yAxis: { type: 'value', name: 'jobs per second', min: 0, ...axisName(c), axisLine: axisLine(c), splitLine: split(c), axisLabel: { color: c.ink2 } },
    series,
  };
};

const cpu = (ms: number) => (c: C): EChartsCoreOption => {
  const r = rows(ms);
  const line = (name: string, key: 'postgres_cpu_millicores' | 'worker_cpu_millicores', color: string, extra = {}) => ({
    name, type: 'line', symbolSize: 8, lineStyle: { width: 2.5 }, itemStyle: { color },
    data: r.map((m) => ({ value: [m.replicas, m[key]], m })), ...extra,
  });
  return {
    ...base(c), grid: grid(),
    tooltip: {
      ...(base(c).tooltip as object), trigger: 'item',
      formatter: (p: P) => `${p.marker ?? ''}<b>${p.seriesName}</b><br/>${p.data.m.replicas} replicas: <b>${nf(p.data.value[1])}</b> millicores<br/>(${nf(p.data.value[1] / 1000, 2)} cores)`,
    },
    xAxis: replicaAxis(c),
    yAxis: { type: 'value', min: 0, max: 12000, interval: 2000, name: 'CPU, millicores', ...axisName(c), axisLine: axisLine(c), splitLine: split(c), axisLabel: { color: c.ink2, formatter: (v: number) => nf(v) } },
    series: [
      line('Postgres', 'postgres_cpu_millicores', c.c1, {
        markLine: { silent: true, symbol: 'none', lineStyle: { type: 'dashed', color: c.ink2 }, label: { color: c.ink2, formatter: 'Docker VM: 11 CPUs', position: 'insideStartTop' }, data: [{ yAxis: 11000 }] },
      }),
      line('Workers (sum of pods)', 'worker_cpu_millicores', c.c2),
    ],
  };
};

const percentiles = (kind: 'processing_ms' | 'queue_wait_s') => (c: C): EChartsCoreOption => {
  const r = rows(state.work);
  const shades = [0.4, 0.7, 1];
  const unit = kind === 'processing_ms' ? 'ms' : 's';
  return {
    ...base(c), grid: grid(),
    tooltip: { ...(base(c).tooltip as object), trigger: 'axis', axisPointer: { type: 'shadow' }, valueFormatter: (v: number) => `${nf(v, 2)} ${unit}` },
    xAxis: { type: 'category', data: r.map((m) => String(m.replicas)), name: 'worker replicas', nameLocation: 'middle', nameGap: 30, ...axisName(c), axisLine: axisLine(c), axisLabel: { color: c.ink2 } },
    yAxis: { type: 'value', name: unit === 'ms' ? 'milliseconds' : 'seconds', min: 0, ...axisName(c), axisLine: axisLine(c), splitLine: split(c), axisLabel: { color: c.ink2 } },
    series: (['p50', 'p95', 'p99'] as const).map((q, i) => ({
      name: q, type: 'bar', itemStyle: { color: c.c1, opacity: shades[i] }, data: r.map((m) => m[kind][q]),
    })),
  };
};

const recovery = (c: C): EChartsCoreOption => {
  const { min_s: lo, max_s: hi, lease_s: lease, claims, heartbeat_s } = k8s.recovery;
  return {
    ...base(c, false), grid: { left: 24, right: 28, top: 28, bottom: 52 },
    tooltip: {
      ...(base(c).tooltip as object), trigger: 'item',
      formatter: () => `<b>SIGKILL → claimed again</b><br/>${claims} claims: <b>${nf(lo, 3)} s</b> to <b>${nf(hi, 3)} s</b><br/>lease ${lease} s, heartbeat ${heartbeat_s} s`,
    },
    xAxis: { type: 'value', min: 28, max: 31, interval: 0.5, name: 'seconds from kill to next claim (axis starts at 28 s)', nameLocation: 'middle', nameGap: 32, ...axisName(c), axisLine: axisLine(c), splitLine: split(c), axisLabel: { color: c.ink2 } },
    yAxis: { type: 'category', data: ['recovery'], show: false },
    series: [
      { type: 'bar', stack: 'r', silent: true, itemStyle: { color: 'transparent' }, data: [lo], tooltip: { show: false } },
      {
        type: 'bar', stack: 'r', barWidth: 22, itemStyle: { color: c.c1 }, data: [hi - lo],
        markLine: { silent: true, symbol: 'none', lineStyle: { type: 'dashed', color: c.ink }, label: { color: c.ink, formatter: `lease = ${lease} s`, position: 'end' }, data: [{ xAxis: lease }] },
      },
    ],
  };
};

const keda = (c: C): EChartsCoreOption => {
  const [a, b] = k8s.keda;
  const run = (r: typeof a, color: string) => ({
    name: r.run === 'benchmark' ? `Benchmark run (${r.commit}, no pod-rate limit)` : `Final build (${r.commit}, at most ${r.scale_down_limit_pods_per_min} pods/min down)`,
    type: 'bar' as const, itemStyle: { color }, barGap: '10%',
    label: { show: true, position: 'right' as const, color: c.ink, formatter: (p: { value: number }) => `${nf(p.value, 2)} s` },
    data: [r.first_scale_up_s, r.ten_ready_s, r.back_to_min_s],
  });
  return {
    ...base(c), grid: { left: 150, right: 56, top: 78, bottom: 44 },
    legend: { ...(base(c).legend as object), orient: 'vertical', left: 0 },
    tooltip: { ...(base(c).tooltip as object), trigger: 'axis', axisPointer: { type: 'shadow' }, valueFormatter: (v: number) => `${nf(v, 2)} s` },
    xAxis: { type: 'value', min: 0, name: 'seconds after the burst was enqueued (scale-up) or after the queue drained (scale-down)', nameLocation: 'middle', nameGap: 28, ...axisName(c), axisLine: axisLine(c), splitLine: split(c), axisLabel: { color: c.ink2 } },
    yAxis: { type: 'category', inverse: true, data: ['first scale-up decision', '10 workers ready', 'back to 3 workers'], axisLine: axisLine(c), axisLabel: { color: c.ink, width: 140, overflow: 'break' } },
    series: [run(a, c.c1), run(b, c.c2)],
  };
};

const sweep = (c: C): EChartsCoreOption => {
  const concs = [...new Set(bare.throughput.map((t) => t.concurrency))].sort((x, y) => x - y);
  const procs = [...new Set(bare.throughput.map((t) => t.procs))].sort((x, y) => x - y);
  const shades = [0.35, 0.55, 0.78, 1];
  return {
    ...base(c), grid: grid(),
    tooltip: {
      ...(base(c).tooltip as object), trigger: 'item',
      formatter: (p: P) => `${p.marker ?? ''}<b>${p.seriesName} × ${p.name} slots</b><br/><b>${nf(p.data.v.jobs_per_sec, 1)}</b> jobs/s<br/>pool ${p.data.v.pool_per_proc} per process · peak ${p.data.v.max_worker_conns} worker connections<br/>${nf(p.data.v.jobs)} jobs`,
    },
    xAxis: { type: 'category', data: concs.map(String), name: 'slots per process (concurrency)', nameLocation: 'middle', nameGap: 30, ...axisName(c), axisLine: axisLine(c), axisLabel: { color: c.ink2 } },
    yAxis: { type: 'value', name: 'jobs per second', min: 0, ...axisName(c), axisLine: axisLine(c), splitLine: split(c), axisLabel: { color: c.ink2, formatter: (v: number) => nf(v) } },
    series: procs.map((p, i) => ({
      name: `${p} process${p > 1 ? 'es' : ''}`, type: 'bar', itemStyle: { color: c.c1, opacity: shades[i] },
      data: concs.map((conc) => {
        const v = bare.throughput.find((t) => t.procs === p && t.concurrency === conc)!;
        return { value: v.jobs_per_sec, v };
      }),
    })),
  };
};

const makers: Record<string, (c: C) => EChartsCoreOption> = {
  throughput, 'cpu-0': cpu(0), 'cpu-50': cpu(50), processing: percentiles('processing_ms'), 'queue-wait': percentiles('queue_wait_s'), recovery, keda, sweep,
};

const live: { chart: echarts.ECharts; make: (c: C) => EChartsCoreOption }[] = [];
document.querySelectorAll<HTMLElement>('[data-chart]').forEach((el) => {
  const make = makers[el.dataset.chart!];
  const chart = echarts.init(el, undefined, { renderer: 'canvas' });
  new ResizeObserver(() => chart.resize()).observe(el);
  live.push({ chart, make });
});
const draw = () => {
  const c = palette();
  for (const { chart, make } of live) chart.setOption(make(c), true);
};
draw();

window.addEventListener('themechange', draw);
document.querySelectorAll<HTMLInputElement>('input[name="work"]').forEach((i) =>
  i.addEventListener('change', () => { state.work = Number(i.value); draw(); }));
document.querySelector<HTMLInputElement>('#show-ref')?.addEventListener('change', (e) => {
  state.ref = (e.target as HTMLInputElement).checked;
  draw();
});
