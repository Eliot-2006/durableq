//go:build k8s

package k8s

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestResourceWindowsExcludeOtherBatches(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 20, 0, time.UTC)
	end := start.Add(40 * time.Second)
	var samples []sample
	for _, v := range []struct {
		sec                 int
		window, cpu, memory string
	}{{25, "20s", "5000m", "20Mi"}, {50, "15s", "1000m", "30Mi"}, {70, "15s", "500m", "900Mi"}} {
		raw := fmt.Sprintf(`{"items":[{"metadata":{"name":"durableq-worker-a"},"timestamp":"2026-01-01T00:%02d:%02dZ","window":%q,"containers":[{"usage":{"cpu":%q,"memory":%q}}]},{"metadata":{"name":"durableq-postgres-0"},"timestamp":"2026-01-01T00:%02d:%02dZ","window":%q,"containers":[{"usage":{"cpu":%q,"memory":%q}}]}]}`, v.sec/60, v.sec%60, v.window, v.cpu, v.memory, v.sec/60, v.sec%60, v.window, v.cpu, v.memory)
		samples = append(samples, sample{PodMetrics: json.RawMessage(raw)})
	}
	peak, coverage, err := windowPeaks(samples, start.Add(-10*time.Second), start, end, 1)
	if err != nil || peak.WorkerCPU != 1000 || peak.PostgresCPU != 1000 || peak.WorkerMiB != 30 || peak.PostgresMiB != 30 {
		t.Fatalf("foreign windows must not inflate resource peaks: %+v %v", peak, err)
	}
	for k, n := range coverage {
		if n != 2 {
			t.Fatalf("coverage %s=%d, want 2", k, n)
		}
	}
}

type podMetricsList struct {
	Items []struct {
		Metadata   struct{ Name string }
		Timestamp  time.Time
		Window     string
		Containers []struct{ Usage map[string]string }
	}
}

func metricsPublished(raw json.RawMessage, finish time.Time, replicas int) bool {
	var list podMetricsList
	if len(raw) == 0 || json.Unmarshal(raw, &list) != nil {
		return false
	}
	workers, postgres := 0, 0
	for _, p := range list.Items {
		if p.Timestamp.Before(finish) {
			continue
		}
		if strings.HasPrefix(p.Metadata.Name, "durableq-worker-") {
			workers++
		}
		if p.Metadata.Name == "durableq-postgres-0" {
			postgres++
		}
	}
	return workers == replicas && postgres == 1
}

func windowPeaks(samples []sample, caseStart, start, finish time.Time, replicas int) (resourceUsage, map[string]int, error) {
	peak := resourceUsage{Workers: replicas, Postgres: 1}
	coverage := map[string]int{"worker_cpu": 0, "worker_memory": 0, "postgres_cpu": 0, "postgres_memory": 0}
	for _, s := range samples {
		if len(s.PodMetrics) == 0 {
			continue
		}
		var list podMetricsList
		if err := json.Unmarshal(s.PodMetrics, &list); err != nil {
			return peak, coverage, err
		}
		var cpu, mem float64
		cpus, mems := 0, 0
		for _, p := range list.Items {
			worker := strings.HasPrefix(p.Metadata.Name, "durableq-worker-")
			if !worker && p.Metadata.Name != "durableq-postgres-0" {
				continue
			}
			window, err := time.ParseDuration(p.Window)
			if err != nil {
				return peak, coverage, err
			}
			var podCPU, podMem float64
			for _, c := range p.Containers {
				v, err := quantity(c.Usage["cpu"], true)
				if err != nil {
					return peak, coverage, err
				}
				podCPU += v
				v, err = quantity(c.Usage["memory"], false)
				if err != nil {
					return peak, coverage, err
				}
				podMem += v
			}
			windowStart := p.Timestamp.Add(-window)
			// Fresh workers may include this case's idle setup; persistent Postgres
			// must exclude setup/previous batches. Both must overlap the workload.
			cpuStart := start
			if worker {
				cpuStart = caseStart
			}
			if !windowStart.Before(cpuStart) && p.Timestamp.After(start) && windowStart.Before(finish) {
				if worker {
					cpu += podCPU
					cpus++
				} else {
					peak.PostgresCPU = max(peak.PostgresCPU, podCPU)
					coverage["postgres_cpu"]++
				}
			}
			if !p.Timestamp.Before(start) && !p.Timestamp.After(finish) {
				if worker {
					mem += podMem
					mems++
				} else {
					peak.PostgresMiB = max(peak.PostgresMiB, podMem)
					coverage["postgres_memory"]++
				}
			}
		}
		if cpus == replicas {
			peak.WorkerCPU = max(peak.WorkerCPU, cpu)
			coverage["worker_cpu"]++
		}
		if mems == replicas {
			peak.WorkerMiB = max(peak.WorkerMiB, mem)
			coverage["worker_memory"]++
		}
	}
	return peak, coverage, nil
}

func TestFreshWorkerWindowIncludesItsOwnIdleSetup(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 20, 0, time.UTC)
	raw := json.RawMessage(`{"items":[{"metadata":{"name":"durableq-worker-new"},"timestamp":"2026-01-01T00:00:30Z","window":"15s","containers":[{"usage":{"cpu":"100m","memory":"20Mi"}}]},{"metadata":{"name":"durableq-postgres-0"},"timestamp":"2026-01-01T00:00:30Z","window":"15s","containers":[{"usage":{"cpu":"1000m","memory":"30Mi"}}]}]}`)
	peak, coverage, err := windowPeaks([]sample{{PodMetrics: raw}}, start.Add(-10*time.Second), start, start.Add(40*time.Second), 1)
	if err != nil || peak.WorkerCPU != 100 || coverage["worker_cpu"] != 1 || peak.PostgresCPU != 0 || coverage["postgres_cpu"] != 0 {
		t.Fatalf("fresh worker has no previous workload; persistent postgres does: %+v %v %v", peak, coverage, err)
	}
}
