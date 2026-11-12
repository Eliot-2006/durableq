//go:build k8s

package k8s

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var seed = flag.Int64("seed", 42, "seed for fault progress and pod selection")
var reportPath = flag.String("report", "", "JSON report path, relative to repository root")
var reports []*report
var root string

const selector = "app.kubernetes.io/instance=durableq,app.kubernetes.io/component=worker"
const pauseKey = "autoscaling.keda.sh/paused-replicas"

type event struct {
	Seconds float64 `json:"seconds"`
	Type    string  `json:"type"`
	Target  string  `json:"target"`
	Detail  string  `json:"detail,omitempty"`
}
type sample struct {
	At          time.Time `json:"at"`
	Desired     int       `json:"desired"`
	Ready       int       `json:"ready"`
	Connections int       `json:"postgres_connections,omitempty"`
	Top         string    `json:"top,omitempty"`
	Error       string    `json:"error,omitempty"`
}
type report struct {
	Scenario       string         `json:"scenario"`
	Seed           int64          `json:"seed"`
	Params         map[string]any `json:"params"`
	Started        time.Time      `json:"started"`
	Duration       float64        `json:"duration_seconds"`
	EnqueueSeconds float64        `json:"enqueue_seconds"`
	DrainSeconds   float64        `json:"drain_seconds"`
	Faults         map[string]int `json:"faults_applied"`
	Events         []event        `json:"events"`
	Invariants     map[string]int `json:"invariant_violations"`
	Metrics        map[string]int `json:"metrics"`
	Samples        []sample       `json:"samples,omitempty"`
	Benchmark      map[string]any `json:"benchmark,omitempty"`
	Passed         bool           `json:"passed"`
	Skipped        string         `json:"skipped,omitempty"`
	Errors         []string       `json:"errors,omitempty"`
}
type harness struct {
	t          *testing.T
	r          *report
	rng        *rand.Rand
	keda       bool
	pause      string
	replicas   int
	enqueued   int
	enqueueEnd time.Time
	delay      string
	config     map[string]string
}
type deployment struct {
	Spec   struct{ Replicas int }
	Status struct{ ReadyReplicas int }
}
type pod struct {
	Metadata struct {
		Name              string
		DeletionTimestamp *time.Time
	}
	Spec   struct{ NodeName string }
	Status struct {
		ContainerStatuses []struct {
			Name        string
			ContainerID string
			Ready       bool
		}
	}
}

type inspection struct {
	Info struct {
		Pid         int
		RuntimeSpec struct{ Process struct{ Args []string } }
	}
	Status struct {
		State      string
		ExitCode   int
		FinishedAt string
		Labels     map[string]string
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	wd, _ := os.Getwd()
	root = filepath.Clean(filepath.Join(wd, "../.."))
	if *reportPath == "" {
		*reportPath = filepath.Join("bench/k8s/chaos", time.Now().UTC().Format("20060102T150405.000000000Z")+".json")
	}
	if !filepath.IsAbs(*reportPath) {
		*reportPath = filepath.Join(root, *reportPath)
	}
	code := m.Run()
	if len(reports) > 0 {
		if err := writeJSON(*reportPath, reports); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
		fmt.Printf("REPORT %s\n", *reportPath)
	}
	os.Exit(code)
}
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}
func command(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = root
	b, err := cmd.CombinedOutput()
	if err != nil {
		return string(b), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, b)
	}
	return strings.TrimSpace(string(b)), nil
}
func kube(args ...string) (string, error) {
	return command("kubectl", append([]string{"--context", "kind-durableq", "-n", "durableq"}, args...)...)
}
func sql(query string) (string, error) {
	return kube("exec", "durableq-postgres-0", "-c", "postgres", "--", "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "durableq", "-d", "durableq", "-c", query)
}
func cli(args ...string) (string, error) {
	return kube(append([]string{"exec", "deploy/durableq-api", "-c", "api", "--", "/durableq"}, args...)...)
}
func (h *harness) must(s string, err error) string {
	h.t.Helper()
	if err != nil {
		h.r.Errors = append(h.r.Errors, err.Error())
		h.t.Fatal(err)
	}
	return s
}
func (h *harness) check(ok bool, msg string) {
	h.t.Helper()
	if !ok {
		h.r.Errors = append(h.r.Errors, msg)
		h.t.Error(msg)
	}
}
func (h *harness) decode(s string, v any) {
	h.t.Helper()
	if err := json.Unmarshal([]byte(s), v); err != nil {
		h.t.Fatalf("decode %T: %v", v, err)
	}
}
func (h *harness) wait(timeout time.Duration, why string, fn func() bool) {
	h.t.Helper()
	until := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(until) {
			h.r.Errors = append(h.r.Errors, "timeout: "+why)
			h.t.Fatal("timeout: " + why)
		}
		time.Sleep(time.Second)
	}
}
func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, rng: rand.New(rand.NewSource(*seed)), r: &report{Scenario: strings.TrimPrefix(t.Name(), "Test"), Seed: *seed, Params: map[string]any{}, Started: time.Now().UTC(), Faults: map[string]int{}}}
	reports = append(reports, h.r)
	lock := filepath.Join(os.TempDir(), "durableq-k8s.lock")
	if err := os.Mkdir(lock, 0700); err != nil {
		t.Fatalf("exclusive cluster lock %s: %v", lock, err)
	}
	t.Cleanup(func() { os.Remove(lock) })
	t.Cleanup(func() {
		h.r.Duration = time.Since(h.r.Started).Seconds()
		if t.Failed() && len(h.r.Errors) == 0 {
			h.r.Errors = append(h.r.Errors, "test failed; see run log")
		}
		h.r.Passed = !t.Failed() && h.r.Skipped == ""
		fmt.Printf("CHAOS %-24s seed=%d pass=%t faults=%v drain=%.2fs duplicates=%d effects_violations=%d report=%s\n", h.r.Scenario, *seed, h.r.Passed, h.r.Faults, h.r.DrainSeconds, h.r.Metrics["duplicate_executions"], h.r.Invariants["I2"], *reportPath)
	})
	var d deployment
	h.decode(h.must(kube("get", "deploy/durableq-worker", "-o", "json")), &d)
	h.replicas = d.Spec.Replicas
	var obj struct {
		Metadata struct{ Annotations map[string]string }
	}
	s, err := kube("get", "scaledobject", "durableq-worker", "--ignore-not-found", "-o", "json")
	if err != nil {
		h.must(s, err)
	}
	if s != "" {
		h.keda = true
		h.decode(s, &obj)
		h.pause = obj.Metadata.Annotations[pauseKey]
	}
	var config struct{ Data map[string]string }
	h.decode(h.must(kube("get", "configmap", "durableq", "-o", "json")), &config)
	h.config = config.Data
	h.r.Params["worker_config"] = config.Data
	t.Cleanup(func() {
		var err error
		if h.keda {
			arg := pauseKey + "-"
			if h.pause != "" {
				arg = pauseKey + "=" + h.pause
			}
			_, err = kube("annotate", "scaledobject", "durableq-worker", arg, "--overwrite")
		} else {
			_, err = kube("scale", "deploy/durableq-worker", fmt.Sprintf("--replicas=%d", h.replicas))
		}
		if err != nil {
			h.r.Errors = append(h.r.Errors, "restore: "+err.Error())
			t.Error(err)
		}
	})
	h.fixed(0)
	h.wait(2*time.Minute, "workers stopped before reset", func() bool { return len(h.pods()) == 0 })
	h.must(sql(qReset))
	h.fixed(3)
	return h
}
func (h *harness) pods() []pod {
	var p struct{ Items []pod }
	h.decode(h.must(kube("get", "pods", "-l", selector, "-o", "json")), &p)
	return p.Items
}
func (h *harness) replicasNow() (int, int) {
	var d deployment
	h.decode(h.must(kube("get", "deploy/durableq-worker", "-o", "json")), &d)
	return d.Spec.Replicas, d.Status.ReadyReplicas
}
func (h *harness) fixed(n int) {
	if h.keda {
		h.must(kube("annotate", "scaledobject", "durableq-worker", pauseKey+"="+strconv.Itoa(n), "--overwrite"))
	}
	h.must(kube("scale", "deploy/durableq-worker", fmt.Sprintf("--replicas=%d", n)))
	h.wait(3*time.Minute, fmt.Sprintf("%d ready workers", n), func() bool { desired, ready := h.replicasNow(); return desired == n && ready == n })
}
func (h *harness) enqueue(n, sleep, budget int, fail float64, permanent bool) {
	payload := fmt.Sprintf(`{"sleep_ms":%d,"fail_rate":%.1f,"permanent":%t}`, sleep, fail, permanent)
	start := time.Now()
	delay := h.delay
	if delay == "" {
		delay = "5s"
	}
	h.must(cli("enqueue", "--kind", "demo.effect", "--count", strconv.Itoa(n), "--max-attempts", strconv.Itoa(budget), "--delay", delay, "--payload", payload))
	h.enqueued += n
	h.enqueueEnd = time.Now()
	h.r.EnqueueSeconds += time.Since(start).Seconds()
	h.r.Params["enqueued"] = h.enqueued
	h.r.Params["payload"] = payload
	h.r.Params["max_attempts"] = budget
}
func (h *harness) progress() (int, int) {
	var p struct{ Total, Done, Running int }
	h.decode(h.must(sql(qProgress)), &p)
	return p.Done, p.Running
}
func (h *harness) at(fraction float64) {
	target := int(float64(h.enqueued) * fraction)
	h.wait(10*time.Minute, fmt.Sprintf("progress %d/%d with active jobs", target, h.enqueued), func() bool {
		done, active := h.progress()
		if done == h.enqueued {
			h.must("", fmt.Errorf("fault shortfall: queue drained before target %d with active jobs", target))
		}
		return done >= target && active > 0
	})
}
func (h *harness) fault(kind, target, detail string) {
	h.r.Faults[kind]++
	h.r.Events = append(h.r.Events, event{time.Since(h.r.Started).Seconds(), kind, target, detail})
}
func (h *harness) activePod() (pod, []claim) {
	ps := h.pods()
	h.rng.Shuffle(len(ps), func(i, j int) { ps[i], ps[j] = ps[j], ps[i] })
	for _, p := range ps {
		if p.Metadata.DeletionTimestamp != nil {
			continue
		}
		for _, c := range p.Status.ContainerStatuses {
			if c.Name == "worker" && c.Ready {
				var jobs []claim
				h.decode(h.must(sql(activeQuery(p.Metadata.Name))), &jobs)
				if len(jobs) > 0 {
					return p, jobs
				}
			}
		}
	}
	h.t.Fatal("no active worker pod: scheduled fault did not land")
	return pod{}, nil
}
func (h *harness) kill(p pod) time.Time {
	var id string
	for _, c := range p.Status.ContainerStatuses {
		if c.Name == "worker" {
			id = strings.TrimPrefix(c.ContainerID, "containerd://")
		}
	}
	var info inspection
	h.decode(h.must(command("docker", "exec", p.Spec.NodeName, "crictl", "inspect", id)), &info)
	labels := info.Status.Labels
	h.check(labels["io.kubernetes.pod.name"] == p.Metadata.Name && labels["io.kubernetes.pod.namespace"] == "durableq" && labels["io.kubernetes.container.name"] == "worker", "SIGKILL target container labels must match")
	args := info.Info.RuntimeSpec.Process.Args
	if h.t.Failed() || info.Info.Pid <= 1 || len(args) < 2 || args[0] != "/durableq" || args[1] != "worker" {
		h.t.Fatal("refusing SIGKILL: invalid worker PID/args")
	}
	pid := info.Info.Pid
	h.must(command("docker", "exec", p.Spec.NodeName, "kill", "-KILL", strconv.Itoa(pid)))
	var killed time.Time
	h.wait(20*time.Second, "container exit 137", func() bool {
		h.decode(h.must(command("docker", "exec", p.Spec.NodeName, "crictl", "inspect", id)), &info)
		if info.Status.State != "CONTAINER_EXITED" {
			return false
		}
		if info.Status.ExitCode != 137 {
			h.must("", fmt.Errorf("SIGKILL exit code=%d", info.Status.ExitCode))
		}
		var err error
		killed, err = containerTime(info.Status.FinishedAt)
		if err != nil {
			h.t.Fatal(err)
		}
		return true
	})
	h.fault("sigkill", p.Metadata.Name, fmt.Sprintf("node=%s container=%s pid=%d exit=137 killed_at=%s", p.Spec.NodeName, id, pid, killed.Format(time.RFC3339Nano)))
	return killed
}

type nextClaim struct {
	ID        int64     `json:"id"`
	ClaimedAt time.Time `json:"claimed_at"`
}
type recovery struct {
	Pending   int         `json:"pending"`
	Abandoned []claim     `json:"abandoned"`
	Claims    []nextClaim `json:"claims"`
}

func (h *harness) nextClaims(jobs []claim) recovery {
	if len(jobs) == 0 {
		h.t.Fatal("no pre-kill claims captured")
	}
	var result recovery
	h.wait(3*time.Minute, "every abandoned job reclaimed", func() bool {
		h.decode(h.must(sql(recoveryQuery(jobs))), &result)
		return result.Pending == 0
	})
	h.check(len(result.Abandoned) > 0, "SIGKILL must abandon at least one captured attempt")
	return result
}
func (h *harness) drain(sampleResources bool) {
	h.wait(12*time.Minute, "queue drain", func() bool {
		if sampleResources {
			h.sample(true)
		}
		s, err := cli("status")
		if err != nil {
			h.t.Logf("status unavailable during recovery: %v", err)
			return false
		}
		n, err := openJobs(s)
		if err != nil {
			h.t.Fatal(err)
		}
		if n == 0 {
			return true
		}
		time.Sleep(time.Second)
		return false
	})
	h.r.DrainSeconds = time.Since(h.enqueueEnd).Seconds()
}
func (h *harness) verify(dead bool) {
	out, err := cli("check")
	checks, parseErr := parseChecks(out)
	h.r.Invariants = checks
	if parseErr != nil {
		h.t.Fatal(parseErr)
	}
	h.check(err == nil, fmt.Sprintf("durableq check error: %v", err))
	for id, n := range checks {
		h.check(n == 0, fmt.Sprintf("%s: %d violations", id, n))
	}
	h.decode(h.must(sql(qMetrics)), &h.r.Metrics)
	h.check(h.r.Metrics["total"] == h.enqueued, fmt.Sprintf("job count %d != enqueued %d", h.r.Metrics["total"], h.enqueued))
	if dead {
		h.check(h.r.Metrics["dead"] == h.enqueued && h.r.Metrics["effects"] == 0, "DLQ workload must all die with no effects")
	} else {
		h.check(h.r.Metrics["succeeded"] == h.enqueued && h.r.Metrics["dead"] == 0, "healthy workload must all succeed")
	}
}

var checkRE = regexp.MustCompile(`(?m)^\s*(I[1-7])\s+(PASS|FAIL \(([0-9]+) violations\))\s+[^\n]+$`)

func parseChecks(s string) (map[string]int, error) {
	checks := map[string]int{}
	for _, m := range checkRE.FindAllStringSubmatch(s, -1) {
		if _, ok := checks[m[1]]; ok {
			return nil, errors.New("duplicate invariant " + m[1])
		}
		n := 0
		if m[3] != "" {
			n, _ = strconv.Atoi(m[3])
		}
		checks[m[1]] = n
	}
	if len(checks) != 7 {
		return nil, fmt.Errorf("expected I1–I7, got %d: %s", len(checks), s)
	}
	return checks, nil
}
func openJobs(s string) (int, error) {
	if strings.TrimSpace(s) == "" {
		return 0, errors.New("empty status")
	}
	open := 0
	for line := range strings.SplitSeq(strings.TrimSpace(s), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			return 0, fmt.Errorf("invalid status line %q", line)
		}
		n, err := strconv.Atoi(f[1])
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid status count %q", line)
		}
		switch f[0] {
		case "queued", "running":
			open += n
		case "succeeded", "dead":
		default:
			return 0, fmt.Errorf("unknown state %q", f[0])
		}
	}
	return open, nil
}
func (h *harness) sample(resources bool) {
	desired, ready := h.replicasNow()
	s := sample{At: time.Now().UTC(), Desired: desired, Ready: ready}
	if resources {
		out, err := sql(qConnections)
		if err == nil {
			s.Connections, err = strconv.Atoi(out)
		}
		if err != nil {
			s.Error = err.Error()
		}
		out, err = kube("top", "pods", "--no-headers")
		if err == nil {
			s.Top = out
		} else {
			s.Error += " top: " + err.Error()
		}
	}
	h.r.Samples = append(h.r.Samples, s)
}

func containerTime(s string) (time.Time, error) {
	if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, ns).UTC(), nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
