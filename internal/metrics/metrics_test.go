package metrics

import (
	"github.com/Eliot-2006/durableq"
	"github.com/prometheus/client_golang/prometheus"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWorkerMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	hook := Worker(reg, []string{"known"}, func() int { return 2 }, 3)
	for _, typ := range []string{"claimed", "succeeded", "retried", "dead", "released", "lease_lost", "stale", "error"} {
		hook(durableq.Event{Kind: "known", Type: typ, Reclaimed: true, Duration: time.Second, QueueWait: time.Second})
	}
	hook(durableq.Event{Kind: "untrusted", Type: "claimed"})
	r := httptest.NewRecorder()
	Handler(reg).ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
	body := r.Body.String()
	for _, name := range []string{"job_claims_total counter", "lease_expirations_total counter", "jobs_completed_total counter", "jobs_failed_total counter", "jobs_retried_total counter", "jobs_dead_lettered_total counter", "jobs_released_total counter", "lease_lost_total counter", "stale_transitions_total counter", "job_duration_seconds histogram", "queue_wait_seconds histogram", "worker_active_jobs gauge", "worker_concurrency gauge"} {
		if !strings.Contains(body, "# TYPE durableq_"+name) {
			t.Errorf("missing %s", name)
		}
	}
	for _, bad := range []string{`job=`, `id=`, `kind="untrusted"`} {
		if strings.Contains(body, bad) {
			t.Errorf("bad label %s", bad)
		}
	}
	for _, sample := range []string{`durableq_jobs_failed_total{kind="known"} 2`, `durableq_worker_active_jobs 2`, `durableq_worker_concurrency 3`} {
		if !strings.Contains(body, sample) {
			t.Errorf("missing %s", sample)
		}
	}
}
