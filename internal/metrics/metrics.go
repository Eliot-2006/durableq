// Package metrics wires Prometheus without adding it to the queue library.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/Eliot-2006/durableq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func runtimeCollectors(reg *prometheus.Registry) {
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

// Worker registers process metrics and returns a concurrency-safe event hook.
// kinds must be the registered handler kinds, never values supplied by clients.
func Worker(reg *prometheus.Registry, kinds []string, active func() int, concurrency int) func(durableq.Event) {
	runtimeCollectors(reg)
	counters := map[string]*prometheus.CounterVec{}
	for _, name := range []string{"job_claims", "lease_expirations", "jobs_completed", "jobs_failed", "jobs_retried", "jobs_dead_lettered", "jobs_released", "lease_lost", "stale_transitions"} {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "durableq_" + name + "_total", Help: "Worker " + name + "."}, []string{"kind"})
		reg.MustRegister(c)
		counters[name] = c
	}
	// Durations span milliseconds to minutes; lateness can grow to hours in an outage.
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "durableq_job_duration_seconds", Help: "Claim to attempt outcome.", Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 10, 30, 60, 120, 300, 600}}, []string{"kind", "outcome"})
	wait := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "durableq_queue_wait_seconds", Help: "Claim time minus run_at.", Buckets: []float64{.001, .01, .1, 1, 5, 30, 60, 300, 900, 3600, 14400}}, []string{"kind"})
	reg.MustRegister(duration, wait, prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "durableq_worker_active_jobs", Help: "In-flight attempts."}, func() float64 { return float64(active()) }), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "durableq_worker_concurrency", Help: "Maximum in-flight attempts."}, func() float64 { return float64(concurrency) }))
	known := map[string]bool{}
	for _, kind := range kinds {
		known[kind] = true
		for _, c := range counters {
			c.WithLabelValues(kind)
		}
		wait.WithLabelValues(kind)
		for _, outcome := range []string{"succeeded", "retried", "dead", "released", "lease_lost", "stale", "error"} {
			duration.WithLabelValues(kind, outcome)
		}
	}
	return func(e durableq.Event) {
		if !known[e.Kind] {
			return
		}
		inc := func(name string) { counters[name].WithLabelValues(e.Kind).Inc() }
		switch e.Type {
		case "claimed":
			inc("job_claims")
			if e.Reclaimed {
				inc("lease_expirations")
			}
			wait.WithLabelValues(e.Kind).Observe(e.QueueWait.Seconds())
			return
		case "succeeded":
			inc("jobs_completed")
		case "retried":
			inc("jobs_failed")
			inc("jobs_retried")
		case "dead":
			inc("jobs_failed")
			inc("jobs_dead_lettered")
		case "released":
			inc("jobs_released")
		case "lease_lost":
			inc("lease_lost")
		case "stale":
			inc("stale_transitions")
		case "error":
		default:
			return
		}
		duration.WithLabelValues(e.Kind, e.Type).Observe(e.Duration.Seconds())
	}
}

type APIMetrics struct {
	Enqueued prometheus.Counter
	Requests *prometheus.CounterVec
}

func API(reg *prometheus.Registry, c *durableq.Client) *APIMetrics {
	runtimeCollectors(reg)
	m := &APIMetrics{
		Enqueued: prometheus.NewCounter(prometheus.CounterOpts{Name: "durableq_jobs_enqueued_total", Help: "New jobs submitted through HTTP."}),
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "durableq_http_requests_total", Help: "HTTP requests by matched mux pattern and status."}, []string{"route", "code"}),
	}
	q := &queueCollector{client: c}
	for _, name := range []string{"pending", "scheduled", "running", "lease_expired", "succeeded", "dead"} {
		q.descs = append(q.descs, prometheus.NewDesc("durableq_jobs_"+name, "Queue jobs "+name+".", nil, nil))
	}
	reg.MustRegister(m.Enqueued, m.Requests, q)
	return m
}

type queueCollector struct {
	client *durableq.Client
	descs  []*prometheus.Desc
}

func (q *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range q.descs {
		ch <- d
	}
}
func (q *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := q.client.Stats(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(q.descs[0], err)
		return
	}
	for i, v := range []int64{s.Pending, s.Scheduled, s.Running, s.LeaseExpired, s.Succeeded, s.Dead} {
		ch <- prometheus.MustNewConstMetric(q.descs[i], prometheus.GaugeValue, float64(v))
	}
}
