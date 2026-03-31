package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/your-org/payflow/worker/internal/metrics"
)

// Metrics provides a structured handle for Prometheus instrumentation.
// It reuses the global variables from internal/metrics to avoid double-registration panics.
type Metrics struct {
	TasksTotal          *prometheus.CounterVec
	ActiveTasks         prometheus.Gauge
	BankRequestDuration prometheus.Histogram
	RevokedTasksTotal   prometheus.Counter
}

func NewMetrics() *Metrics {
	return &Metrics{
		TasksTotal:          metrics.TasksTotal,
		ActiveTasks:         metrics.ActiveTasks,
		BankRequestDuration: metrics.BankRequestDuration,
		RevokedTasksTotal:   metrics.RevokedTasksTotal,
	}
}

// RecordTaskSuccess increments the success counter.
func (m *Metrics) RecordTaskSuccess() {
	m.TasksTotal.WithLabelValues("success").Inc()
}

// RecordTaskFailure increments the failure counter.
func (m *Metrics) RecordTaskFailure() {
	m.TasksTotal.WithLabelValues("failure").Inc()
}

// RecordTaskRevoked increments the revoked counter.
func (m *Metrics) RecordTaskRevoked() {
	m.RevokedTasksTotal.Inc()
}
