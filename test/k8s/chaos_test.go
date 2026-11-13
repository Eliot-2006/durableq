//go:build k8s

package k8s

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func TestRandomPodKill(t *testing.T) {
	h := newHarness(t)
	h.enqueue(1200, 500, 10, 0, false)
	for i := range 3 {
		h.at(.05 + float64(i)*.2 + h.rng.Float64()*.03)
		p, _ := h.activePod()
		h.kill(p)
	}
	h.drain(false)
	h.verify(false)
	h.check(h.r.Faults["sigkill"] == 3, "all three SIGKILLs must land")
	h.check(h.r.Metrics["duplicate_executions"] > 0, "SIGKILL must exercise re-execution")
}
func TestGracefulTermination(t *testing.T) {
	h := newHarness(t)
	h.enqueue(90, 30000, 1, 0, false)
	h.at(0)
	p, _ := h.activePod()
	h.must(kube("delete", "pod", p.Metadata.Name, "--wait=false"))
	h.fault("sigterm_delete", p.Metadata.Name, "")
	h.drain(false)
	h.verify(false)
	h.check(h.r.Metrics["released"] > 0, "graceful deletion must release jobs")
}
func TestRollingUpdate(t *testing.T) {
	h := newHarness(t)
	h.enqueue(2400, 1000, 5, 0, false)
	for i := range 3 {
		h.at(.03 + float64(i)*.15 + h.rng.Float64()*.02)
		h.must(kube("rollout", "restart", "deploy/durableq-worker"))
		h.must(kube("rollout", "status", "deploy/durableq-worker", "--timeout=180s"))
		h.fault("rollout_restart", "deploy/durableq-worker", strconv.Itoa(i+1))
	}
	h.drain(false)
	h.verify(false)
	h.check(h.r.Faults["rollout_restart"] == 3, "all three rolling restarts must complete")
}
func TestScaleUpDown(t *testing.T) {
	h := newHarness(t)
	h.fixed(1)
	h.enqueue(4000, 1000, 5, 0, false)
	for i, n := range []int{10, 2, 8, 1} {
		h.at(.01 + float64(i)*.03 + h.rng.Float64()*.01)
		_, active := h.progress()
		h.check(active > 0, "scale event must occur with active jobs")
		h.fixed(n)
		h.fault("scale", "deploy/durableq-worker", strconv.Itoa(n))
	}
	h.drain(false)
	h.verify(false)
	h.check(h.r.Faults["scale"] == 4, "all four replica changes must apply")
}
func TestPostgresRestart(t *testing.T) {
	h := newHarness(t)
	h.enqueue(900, 500, 10, 0, false)
	h.at(.08 + h.rng.Float64()*.04)
	before := h.must(sql(qPostmaster))
	var p struct{ Total int }
	h.decode(h.must(sql(qProgress)), &p)
	h.must(kube("delete", "pod", "durableq-postgres-0", "--wait=true", "--timeout=120s"))
	h.wait(3*time.Minute, "Postgres restarted", func() bool { s, err := sql(qPostmaster); return err == nil && s != before })
	var after struct{ Total int }
	h.decode(h.must(sql(qProgress)), &after)
	h.check(p.Total == after.Total && after.Total == h.enqueued, "Postgres restart must preserve the exact job count")
	h.r.Params["jobs_before_restart"] = p.Total
	h.r.Params["jobs_after_restart"] = after.Total
	h.fault("postgres_restart", "durableq-postgres-0", before+" -> "+h.must(sql(qPostmaster)))
	h.drain(false)
	h.verify(false)
}
func TestConnectionKill(t *testing.T) {
	h := newHarness(t)
	h.enqueue(900, 500, 10, 0, false)
	for i := range 3 {
		h.at(.03 + float64(i)*.2 + h.rng.Float64()*.03)
		n, err := strconv.Atoi(h.must(sql(qKillConnections)))
		if err != nil {
			t.Fatal(err)
		}
		h.check(n > 0, "connection-kill round must terminate worker backends")
		h.r.Faults["connections_terminated"] += n
		h.fault("connection_kill_round", "durableq-worker-%", fmt.Sprintf("terminated=%d", n))
	}
	h.drain(false)
	h.verify(false)
}
func TestRetryExhaustionDLQ(t *testing.T) {
	h := newHarness(t)
	h.enqueue(180, 200, 3, 1, false)
	h.enqueue(60, 0, 3, 0, true)
	h.r.Params["workloads"] = []map[string]any{{"count": 180, "fail_rate": 1, "sleep_ms": 200, "max_attempts": 3}, {"count": 60, "permanent": true, "max_attempts": 3}}
	h.at(.02)
	p, _ := h.activePod()
	h.kill(p)
	h.drain(false)
	h.verify(true)
}
func TestRepeatedCrashRecovery(t *testing.T) {
	h := newHarness(t)
	h.enqueue(3600, 1000, 10, 0, false)
	for cycle := range 3 {
		h.at(float64(cycle)*.12 + h.rng.Float64()*.01)
		h.wait(3*time.Minute, "all three worker containers ready before crash cycle", func() bool { desired, ready := h.replicasNow(); return desired == 3 && ready == 3 })
		var killed []claim
		ps := h.pods()
		h.rng.Shuffle(len(ps), func(i, j int) { ps[i], ps[j] = ps[j], ps[i] })
		h.check(len(ps) == 3, "crash cycle must target all three worker pods")
		for _, p := range ps {
			var jobs []claim
			h.wait(time.Minute, "active claim on "+p.Metadata.Name, func() bool {
				h.decode(h.must(sql(activeQuery(p.Metadata.Name))), &jobs)
				return len(jobs) > 0
			})
			killedAt := h.kill(p)
			var affected []claim
			h.decode(h.must(sql(killedClaimsQuery(jobs[0], killedAt.Format(time.RFC3339Nano)))), &affected)
			killed = append(killed, affected...)
		}
		recovered := h.nextClaims(killed)
		h.fault("crash_recovery_cycle", "all workers", fmt.Sprintf("cycle=%d recovered_jobs=%d", cycle+1, len(recovered.Claims)))
	}
	h.drain(false)
	h.verify(false)
	h.check(h.r.Faults["sigkill"] == 9 && h.r.Faults["crash_recovery_cycle"] == 3, "three complete all-worker crash/recovery cycles required")
}
func TestKEDAAutoscale(t *testing.T) { h := newHarness(t); h.autoscale() }
func (h *harness) autoscale() {
	if !h.keda {
		h.r.Skipped = "KEDA ScaledObject not installed"
		h.t.Skip(h.r.Skipped)
	}
	var obj struct {
		Spec struct{ MinReplicaCount, MaxReplicaCount, PollingInterval, CooldownPeriod int }
	}
	h.decode(h.must(kube("get", "scaledobject", "durableq-worker", "-o", "json")), &obj)
	min, max := obj.Spec.MinReplicaCount, obj.Spec.MaxReplicaCount
	h.r.Params["autoscaling"] = obj.Spec
	h.fixed(min)
	h.must(kube("annotate", "scaledobject", "durableq-worker", pauseKey+"-"))
	// Wait until KEDA has actually resumed before starting its reaction clock.
	h.must(kube("wait", "scaledobject/durableq-worker", "--for=condition=Paused=false", "--timeout=90s"))
	h.enqueue(3000, 2000, 10, 0, false)
	start := h.enqueueEnd
	var up, maximum, drained time.Time
	h.wait(10*time.Minute, "KEDA scale up, drain, and scale back to min", func() bool {
		h.sample(false)
		s := h.r.Samples[len(h.r.Samples)-1]
		if up.IsZero() && s.Desired > min {
			up = s.At
		}
		if maximum.IsZero() && s.Desired == max && s.Ready == max {
			maximum = s.At
		}
		out, err := cli("status")
		if err != nil {
			return false
		}
		open, err := openJobs(out)
		if err != nil {
			h.t.Fatal(err)
		}
		if open == 0 && drained.IsZero() {
			drained = time.Now()
			h.r.DrainSeconds = drained.Sub(start).Seconds()
		}
		time.Sleep(time.Second)
		return !drained.IsZero() && !maximum.IsZero() && s.Desired == min && s.Ready == min
	})
	var finished time.Time
	h.decode(h.must(sql(qFinished)), &finished)
	last := h.r.Samples[len(h.r.Samples)-1].At
	h.r.Benchmark = map[string]any{"first_scale_up_seconds": up.Sub(start).Seconds(), "max_ready_seconds": maximum.Sub(start).Seconds(), "drain_to_min_seconds": last.Sub(finished).Seconds()}
	h.check(!up.IsZero() && !maximum.IsZero(), "KEDA must scale above min and reach max ready")
	h.verify(false)
	fmt.Printf("KEDA first_up=%.2fs max_ready=%.2fs drain_to_min=%.2fs\n", up.Sub(start).Seconds(), maximum.Sub(start).Seconds(), last.Sub(finished).Seconds())
}
