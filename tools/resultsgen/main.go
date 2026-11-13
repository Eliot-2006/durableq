// Command resultsgen regenerates the measured data and README charts from checked-in evidence.
package main

import (
	"bytes"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	benchmarkSource = "bench/k8s/results/20261113T020847Z/report.json"
	processDir      = "bench/results/20261106T050117Z/"
	matrixSource    = "bench/k8s/results/20261113T020847Z/accepted-matrix.csv"
	chaosSource     = "bench/k8s/chaos/20261113T001148Z.json"
	finalSource     = "bench/k8s/chaos/final-build-seed45.json"
)

// Only environment descriptions and explicitly sourced prose summaries live here.
//
//go:embed documented.json
var documented []byte

type row = map[string]any

type results struct {
	Note    string `json:"note"`
	Process row    `json:"process"`
	K8s     row    `json:"k8s"`
}

func main() {
	if err := generate(".", "."); err != nil {
		fmt.Fprintln(os.Stderr, "resultsgen:", err)
		os.Exit(1)
	}
}

// inputRoot and outputRoot are separate so tests never rewrite committed artifacts.
func generate(inputRoot, outputRoot string) error {
	data, matrix, chaos, err := loadResults(inputRoot)
	if err != nil {
		return err
	}
	output, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	files := map[string][]byte{"site/src/data/results.json": append(output, '\n')}
	for _, dark := range []bool{false, true} {
		theme := "light"
		if dark {
			theme = "dark"
		}
		for name, svg := range map[string]string{
			"k8s-throughput":   throughputChart(matrix, dark),
			"k8s-postgres-cpu": cpuChart(matrix, dark),
			"k8s-chaos":        chaosChart(chaos, dark),
		} {
			files["docs/assets/charts/"+name+"-"+theme+".svg"] = []byte(svg)
		}
	}
	for name, contents := range files {
		path := filepath.Join(outputRoot, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(path, contents, 0644); err != nil {
			return err
		}
	}
	return nil
}

func loadResults(root string) (results, []row, []row, error) {
	var data results
	if err := json.Unmarshal(documented, &data); err != nil {
		return data, nil, nil, err
	}
	// Fail rather than silently carry summaries whose evidence has changed.
	evidence := map[string][]string{
		processDir + "env.txt": {"PostgreSQL 17.11 (Homebrew)", "11 CPUs, Apple M3 Pro, 18 GiB RAM", "go1.27.3", "8fa4341"},
		"docs/PROGRESS.md":     {"20 consecutive seeds (1000–1019) × 10,000 jobs", "every run had 100 SIGKILLs", "20/20 passed I1–I7"},
		"docs/results-k8s.md":  {"kind v0.33.0, Kubernetes v1.37.0", "11 CPUs, 8.8 GiB", "concurrency 10, `maxConns` 6, lease 30s, heartbeat 10s", "at least two full runs (seeds 42/43 and 44)", "at most 2 pods per minute", "`0ec3769`", "`23d96c1`"},
	}
	for path, snippets := range evidence {
		b, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return data, nil, nil, err
		}
		for _, snippet := range snippets {
			if !bytes.Contains(b, []byte(snippet)) {
				return data, nil, nil, fmt.Errorf("%s: documented evidence missing %q", path, snippet)
			}
		}
	}
	for _, spec := range []struct{ file, key, fields string }{
		{"throughput.csv", "throughput", "procs:i concurrency:i pool_per_proc:i jobs:i jobs_per_sec:n max_worker_conns:i"},
		{"latency.csv", "latency", "rate_per_sec:i jobs:i p50_ms:n p95_ms:n p99_ms:n"},
		{"recovery.csv", "recovery", "jobs:i lease:s p50_ms:n max_ms:n"},
		{"chaos.csv", "chaos", "seed:i passed:b seconds:n kills:i self_kills:i conn_kills:i stalls:i abandoned:i stale_rejected:i"},
	} {
		rows, err := readCSV(root, processDir+spec.file, spec.fields)
		if err != nil {
			return data, nil, nil, err
		}
		if spec.key == "recovery" {
			if len(rows) != 1 {
				return data, nil, nil, fmt.Errorf("recovery.csv: expected one measurement")
			}
			data.Process[spec.key] = rows[0]
		} else {
			data.Process[spec.key] = rows
		}
	}
	data.Process["chaos_note"] = "Five pre-audit runs from chaos.csv; distinct from the documented 20-run acceptance summary."
	matrix, err := readCSV(root, matrixSource, "replicas:i sleep_ms:i jobs:i jobs_per_second:n processing_p50_ms:n processing_p95_ms:n processing_p99_ms:n queue_wait_p50_ms:n queue_wait_p95_ms:n queue_wait_p99_ms:n peak_postgres_connections:i worker_peak_cpu_millicores:n worker_peak_memory_mib:n postgres_peak_cpu_millicores:n postgres_peak_memory_mib:n passed:b source_report:s")
	if err != nil {
		return data, nil, nil, err
	}
	seen := map[string]bool{}
	for _, r := range matrix {
		key := fmt.Sprintf("%g/%g", number(r, "replicas"), number(r, "sleep_ms"))
		replicas, sleep := number(r, "replicas"), number(r, "sleep_ms")
		if seen[key] || (replicas != 1 && replicas != 2 && replicas != 5 && replicas != 10) || (sleep != 0 && sleep != 50) || !r["passed"].(bool) {
			return data, nil, nil, fmt.Errorf("%s: duplicate, unexpected or failed cell %s", matrixSource, key)
		}
		seen[key] = true
		for _, prefix := range []string{"processing", "queue_wait"} {
			percentiles := row{}
			for _, q := range []string{"p50", "p95", "p99"} {
				field := prefix + "_" + q + "_ms"
				n := number(r, field)
				if prefix == "queue_wait" {
					n /= 1000
				}
				percentiles[q] = n
				delete(r, field)
			}
			unit := "_ms"
			if prefix == "queue_wait" {
				unit = "_s"
			}
			r[prefix+unit] = percentiles
		}
		for _, owner := range []string{"worker", "postgres"} {
			for _, metric := range []string{"cpu_millicores", "memory_mib"} {
				field := owner + "_peak_" + metric
				r[owner+"_"+metric] = r[field]
				delete(r, field)
			}
		}
		r["source"] = r["source_report"]
		delete(r, "source_report")
	}
	if len(matrix) != 8 {
		return data, nil, nil, fmt.Errorf("%s: expected exactly eight measured cells", matrixSource)
	}
	data.K8s["matrix"] = matrix
	recovery, benchmark, err := readBenchmarks(root)
	if err != nil {
		return data, nil, nil, err
	}
	data.K8s["recovery"] = recovery
	keda := data.K8s["keda"].([]any)[0].(map[string]any)
	for key, value := range benchmark {
		keda[key] = value
	}
	chaos, err := readChaos(root, chaosSource)
	if err != nil {
		return data, nil, nil, err
	}
	if len(chaos) != 9 {
		return data, nil, nil, fmt.Errorf("%s: expected nine scenarios", chaosSource)
	}
	data.K8s["chaos"] = chaos
	final, err := readChaos(root, finalSource)
	if err != nil {
		return data, nil, nil, err
	}
	data.K8s["final_build_chaos"] = final
	foundKEDA := false
	for _, r := range final {
		if r["scenario"] == "KEDAAutoscale" {
			foundKEDA = true
			benchmark, ok := r["benchmark"].(map[string]any)
			if !ok {
				return data, nil, nil, fmt.Errorf("%s: missing KEDA benchmark", finalSource)
			}
			keda := data.K8s["keda"].([]any)[1].(map[string]any)
			for out, in := range map[string]string{"first_scale_up_s": "first_scale_up_seconds", "ten_ready_s": "max_ready_seconds", "back_to_min_s": "drain_to_min_seconds"} {
				n, ok := benchmark[in].(float64)
				if !ok || n < 0 {
					return data, nil, nil, fmt.Errorf("%s: missing or invalid %s", finalSource, in)
				}
				keda[out] = n
			}
		}
	}
	if !foundKEDA {
		return data, nil, nil, fmt.Errorf("%s: missing KEDAAutoscale scenario", finalSource)
	}
	return data, matrix, chaos, nil
}

// A small explicit projection keeps unused raw columns out of the site contract.
// i = nonnegative integer, n = nonnegative finite number, b = bool, s = string.
func parseCSV(r io.Reader, fields string) ([]row, error) {
	records, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("CSV has no measurements")
	}
	header := map[string]int{}
	for i, h := range records[0] {
		if _, ok := header[h]; ok {
			return nil, fmt.Errorf("duplicate header %s", h)
		}
		header[h] = i
	}
	var rows []row
	for i, record := range records[1:] {
		result := row{}
		for _, spec := range strings.Fields(fields) {
			name, kind, _ := strings.Cut(spec, ":")
			index, ok := header[name]
			if !ok {
				return nil, fmt.Errorf("missing column %s", name)
			}
			value := record[index]
			var parsed any
			var err error
			switch kind {
			case "i", "n":
				var n float64
				n, err = strconv.ParseFloat(value, 64)
				if err == nil && (n < 0 || math.IsNaN(n) || math.IsInf(n, 0) || (kind == "i" && n != math.Trunc(n))) {
					err = fmt.Errorf("invalid nonnegative %s", kind)
				}
				parsed = n
			case "b":
				parsed, err = strconv.ParseBool(value)
			case "s":
				parsed = value
				if value == "" {
					err = fmt.Errorf("empty string")
				}
			default:
				return nil, fmt.Errorf("unknown column type %s", kind)
			}
			if err != nil {
				return nil, fmt.Errorf("row %d column %s (%q): %w", i+2, name, value, err)
			}
			result[name] = parsed
		}
		rows = append(rows, result)
	}
	return rows, nil
}

func readCSV(root, path, fields string) ([]row, error) {
	b, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return nil, err
	}
	rows, err := parseCSV(bytes.NewReader(b), fields)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return rows, nil
}

func readChaos(root, path string) ([]row, error) {
	b, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return nil, err
	}
	var reports []struct {
		Scenario   string         `json:"scenario"`
		Seed       int            `json:"seed"`
		Drain      *float64       `json:"drain_seconds"`
		Faults     map[string]int `json:"faults_applied"`
		Violations map[string]int `json:"invariant_violations"`
		Metrics    map[string]int `json:"metrics"`
		Benchmark  map[string]any `json:"benchmark"`
		Passed     bool           `json:"passed"`
	}
	if err := json.Unmarshal(b, &reports); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	rows := []row{}
	seen := map[string]bool{}
	for _, r := range reports {
		jobs, jobsOK := r.Metrics["total"]
		reexecuted, reexecutedOK := r.Metrics["duplicate_executions"]
		if r.Scenario == "" || seen[r.Scenario] || r.Seed <= 0 || r.Drain == nil || *r.Drain < 0 || !jobsOK || jobs <= 0 || !reexecutedOK || reexecuted < 0 || !r.Passed || len(r.Violations) != 7 || r.Faults == nil {
			return nil, fmt.Errorf("%s: incomplete, duplicate or failed scenario %q", path, r.Scenario)
		}
		seen[r.Scenario] = true
		for i := 1; i <= 7; i++ {
			n, ok := r.Violations[fmt.Sprintf("I%d", i)]
			if !ok || n != 0 {
				return nil, fmt.Errorf("%s: %s missing or violated I%d", path, r.Scenario, i)
			}
		}
		out := row{"scenario": r.Scenario, "seed": r.Seed, "jobs": jobs, "drain_s": *r.Drain, "faults": r.Faults, "reexecuted_jobs": reexecuted, "invariant_violations": r.Violations, "source": path}
		if r.Benchmark != nil {
			out["benchmark"] = r.Benchmark
		}
		rows = append(rows, out)
	}
	return rows, nil
}

func number(r row, key string) float64 { return r[key].(float64) }

// The benchmark report referenced by the seed contains raw timings as well as the matrix.
func readBenchmarks(root string) (row, row, error) {
	b, err := os.ReadFile(filepath.Join(root, benchmarkSource))
	if err != nil {
		return nil, nil, err
	}
	var reports []struct {
		Scenario  string `json:"scenario"`
		Passed    bool   `json:"passed"`
		Benchmark struct {
			Recovery  []float64 `json:"recovery_seconds"`
			Lease     string    `json:"lease"`
			Heartbeat string    `json:"heartbeat"`
			First     *float64  `json:"first_scale_up_seconds"`
			Ready     *float64  `json:"max_ready_seconds"`
			Min       *float64  `json:"drain_to_min_seconds"`
		} `json:"benchmark"`
	}
	if err := json.Unmarshal(b, &reports); err != nil {
		return nil, nil, err
	}
	var recovery, keda row
	for _, r := range reports {
		switch r.Scenario {
		case "BenchRecovery":
			if !r.Passed || len(r.Benchmark.Recovery) == 0 || recovery != nil {
				return nil, nil, fmt.Errorf("%s: invalid recovery evidence", benchmarkSource)
			}
			lease, err := time.ParseDuration(r.Benchmark.Lease)
			if err != nil || lease <= 0 {
				return nil, nil, fmt.Errorf("%s: invalid recovery lease", benchmarkSource)
			}
			heartbeat, err := time.ParseDuration(r.Benchmark.Heartbeat)
			if err != nil || heartbeat <= 0 {
				return nil, nil, fmt.Errorf("%s: invalid recovery heartbeat", benchmarkSource)
			}
			min, max := r.Benchmark.Recovery[0], r.Benchmark.Recovery[0]
			for _, n := range r.Benchmark.Recovery {
				if n < 0 {
					return nil, nil, fmt.Errorf("%s: negative recovery timing", benchmarkSource)
				}
				if n < min {
					min = n
				}
				if n > max {
					max = n
				}
			}
			recovery = row{"claims": len(r.Benchmark.Recovery), "lease_s": lease.Seconds(), "heartbeat_s": heartbeat.Seconds(), "min_s": min, "max_s": max, "source": benchmarkSource, "raw_data": true}
		case "BenchKEDA":
			if !r.Passed || keda != nil {
				return nil, nil, fmt.Errorf("%s: invalid KEDA evidence", benchmarkSource)
			}
			keda = row{}
			for key, n := range map[string]*float64{"first_scale_up_s": r.Benchmark.First, "ten_ready_s": r.Benchmark.Ready, "back_to_min_s": r.Benchmark.Min} {
				if n == nil || *n < 0 {
					return nil, nil, fmt.Errorf("%s: missing or negative %s", benchmarkSource, key)
				}
				keda[key] = *n
			}
		}
	}
	if recovery == nil || keda == nil {
		return nil, nil, fmt.Errorf("%s: missing recovery or KEDA evidence", benchmarkSource)
	}
	return recovery, keda, nil
}
