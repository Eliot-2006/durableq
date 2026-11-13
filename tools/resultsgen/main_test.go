package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCSV(t *testing.T) {
	rows, err := parseCSV(strings.NewReader("count,rate,passed,label,unused\n2,1.25,true,\"comma, label\",ignored\n"), "count:i rate:n passed:b label:s")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || number(rows[0], "count") != 2 || number(rows[0], "rate") != 1.25 || rows[0]["passed"] != true || rows[0]["label"] != "comma, label" {
		t.Fatalf("unexpected parse: %#v", rows)
	}
	for _, test := range []struct{ name, csv, fields string }{
		{"empty", "", "count:i"},
		{"header only", "count\n", "count:i"},
		{"missing column", "other\n1\n", "count:i"},
		{"duplicate header", "count,count\n1,2\n", "count:i"},
		{"short row", "count,rate\n1\n", "count:i rate:n"},
		{"not a number", "count\nx\n", "count:i"},
		{"negative", "count\n-1\n", "count:i"},
		{"fractional counter", "count\n1.5\n", "count:i"},
		{"NaN", "rate\nNaN\n", "rate:n"},
		{"infinite", "rate\n+Inf\n", "rate:n"},
		{"empty value", "label,other\n,x\n", "label:s"},
		{"invalid bool", "passed\nyes\n", "passed:b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseCSV(strings.NewReader(test.csv), test.fields); err == nil {
				t.Fatal("accepted invalid CSV")
			}
		})
	}
}

func TestChaosRequiresCompleteEvidence(t *testing.T) {
	source := `[{"scenario":"Test","seed":44,"drain_seconds":1.2345,"faults_applied":{},"invariant_violations":{"I1":0,"I2":0,"I3":0,"I4":0,"I5":0,"I6":0,"I7":0},"metrics":{"total":10,"duplicate_executions":2},"passed":true}]`
	root := t.TempDir()
	read := func(s string) ([]row, error) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, "chaos.json"), []byte(s), 0644); err != nil {
			t.Fatal(err)
		}
		return readChaos(root, "chaos.json")
	}
	rows, err := read(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || number(rows[0], "drain_s") != 1.2345 || rows[0]["reexecuted_jobs"] != 2 {
		t.Fatalf("unexpected chaos: %#v", rows)
	}
	for _, change := range [][2]string{
		{`"I7":0`, `"I8":0`},
		{`"I5":0`, `"I5":1`},
		{`"drain_seconds":1.2345,`, ``},
		{`"duplicate_executions":2`, `"other":2`},
		{`"passed":true`, `"passed":false`},
	} {
		if _, err := read(strings.Replace(source, change[0], change[1], 1)); err == nil {
			t.Fatalf("accepted incomplete evidence: %v", change)
		}
	}
}

func TestRegenerationMatchesCommittedOutputs(t *testing.T) {
	root := filepath.Join("..", "..")
	output := t.TempDir()
	if err := generate(root, output); err != nil {
		t.Fatal(err)
	}
	paths := []string{"site/src/data/results.json"}
	for _, name := range []string{"k8s-throughput", "k8s-postgres-cpu", "k8s-chaos"} {
		for _, theme := range []string{"light", "dark"} {
			paths = append(paths, "docs/assets/charts/"+name+"-"+theme+".svg")
		}
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			generated, err := os.ReadFile(filepath.Join(output, path))
			if err != nil {
				t.Fatal(err)
			}
			committed, err := os.ReadFile(filepath.Join(root, path))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(generated, committed) {
				t.Errorf("generated output drift: run go run ./tools/resultsgen from the repository root")
			}
			if strings.HasSuffix(path, ".svg") {
				decoder := xml.NewDecoder(bytes.NewReader(generated))
				for {
					_, err := decoder.Token()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
	var data results
	contents, err := os.ReadFile(filepath.Join(output, paths[0]))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(contents, &data); err != nil {
		t.Fatal(err)
	}
	matrix := data.K8s["matrix"].([]any)
	// Preserve raw precision and unit conversion, rather than copying the seed's rounded values.
	if len(matrix) != 8 || matrix[0].(map[string]any)["jobs_per_second"] != 494.7991292425964 || matrix[0].(map[string]any)["queue_wait_s"].(map[string]any)["p50"] != 69322.5755/1000 {
		t.Fatal("matrix precision or units changed")
	}
	recovery := data.K8s["recovery"].(map[string]any)
	if recovery["claims"] != float64(10) || recovery["min_s"] != 29.461497724 || recovery["max_s"] != 30.229478724 {
		t.Fatal("recovery range was not read from per-claim evidence")
	}
	if data.K8s["keda"].([]any)[0].(map[string]any)["first_scale_up_s"] != 8.736189 {
		t.Fatal("benchmark KEDA timing was not read from raw evidence")
	}
	if len(data.K8s["chaos"].([]any)) != 9 || len(data.K8s["final_build_chaos"].([]any)) != 4 {
		t.Fatal("missing chaos evidence")
	}
	if data.K8s["keda"].([]any)[1].(map[string]any)["first_scale_up_s"] != 13.457327 {
		t.Fatal("final KEDA timing was not read from raw evidence")
	}
}
