package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/your-org/payflow/worker/internal/metrics"
)

// Metrics provides a structured handle for Prometheus instrumentation.
// It reuses the global variables from internal/metrics to avoid double-registration panics.
type Metrics struct {
	TasksTotal                *prometheus.CounterVec
	ActiveTasks               prometheus.Gauge
	BankRequestDuration       *prometheus.HistogramVec
	TaskDuration              prometheus.Histogram
	WorkerSaturation          prometheus.Gauge
	RevokedTasksTotal         prometheus.Counter
	OrphanedLeaseCount        prometheus.Gauge
	GRPCServerHandledTotal    *prometheus.CounterVec
	GRPCServerHandlingSeconds *prometheus.HistogramVec
	TaskDeadlineExceededTotal *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	return &Metrics{
		TasksTotal:                metrics.TasksTotal,
		ActiveTasks:               metrics.ActiveTasks,
		BankRequestDuration:       metrics.BankRequestDuration,
		TaskDuration:              metrics.TaskDurationSeconds,
		WorkerSaturation:          metrics.WorkerSaturation,
		RevokedTasksTotal:         metrics.RevokedTasksTotal,
		OrphanedLeaseCount:        metrics.OrphanedLeaseCount,
		GRPCServerHandledTotal:    metrics.GRPCServerHandledTotal,
		GRPCServerHandlingSeconds: metrics.GRPCServerHandlingSeconds,
		TaskDeadlineExceededTotal: metrics.TaskDeadlineExceededTotal,
	}
}


// RecordTaskSuccess increments the success counter with status "ok".
func (m *Metrics) RecordTaskSuccess() {
	m.TasksTotal.WithLabelValues("ok", "success").Inc()
}

// RecordTaskFailure increments the failure counter with status "error".
func (m *Metrics) RecordTaskFailure(errType string) {
	m.TasksTotal.WithLabelValues("error", errType).Inc()
}

// RecordTaskRejectedEpoch increments the counter with status "rejected_epoch".
func (m *Metrics) RecordTaskRejectedEpoch() {
	m.TasksTotal.WithLabelValues("rejected_epoch", "").Inc()
}

// RecordTaskRejectedIdempotent increments the counter with status "rejected_idempotent".
func (m *Metrics) RecordTaskRejectedIdempotent() {
	m.TasksTotal.WithLabelValues("rejected_idempotent", "").Inc()
}

// RecordTaskDuration observes the time taken to process a task.
func (m *Metrics) RecordTaskDuration(seconds float64) {
	m.TaskDuration.Observe(seconds)
}

// RecordBankRequestDuration observes the bank API call duration.
func (m *Metrics) RecordBankRequestDuration(result string, durationMs float64) {
	m.BankRequestDuration.WithLabelValues(result).Observe(durationMs)
}

// RecordTaskRevoked increments the revoked counter.
func (m *Metrics) RecordTaskRevoked() {
	m.RevokedTasksTotal.Inc()
}

// RecordDeadlineExceeded increments the deadline-exceeded counter for the given stage.
// Valid stage values: "bank", "c4_log", "outbox".
func (m *Metrics) RecordDeadlineExceeded(stage string) {
	m.TaskDeadlineExceededTotal.WithLabelValues(stage).Inc()
}
