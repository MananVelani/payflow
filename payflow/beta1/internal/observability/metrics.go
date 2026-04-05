package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/your-org/payflow/worker/internal/metrics"
	"strings"
)

const (
	BankResultSuccess     = "success"
	BankResultFailed      = "failed"
	BankResultTimeout     = "timeout"
	BankResultCircuitOpen = "circuit_open"
	BankResultUnknown     = "unknown"
)

// NormalizeBankResult maps raw bank results to a finite set of label values.
func NormalizeBankResult(raw string) string {
	raw = strings.ToLower(raw)
	switch raw {
	case BankResultSuccess, BankResultFailed, BankResultTimeout, BankResultCircuitOpen:
		return raw
	}

	if strings.Contains(raw, "timeout") || strings.Contains(raw, "deadline") {
		return BankResultTimeout
	}
	if strings.Contains(raw, "circuit") {
		return BankResultCircuitOpen
	}
	return BankResultUnknown
}

// Metrics provides a structured handle for Prometheus instrumentation.
// It reuses the global variables from internal/metrics to avoid double-registration panics.
type Metrics struct {
	TasksTotal                   *prometheus.CounterVec
	ActiveTasks                  prometheus.Gauge
	BankRequestDuration          *prometheus.HistogramVec
	TaskDuration                 prometheus.Histogram
	WorkerSaturation             prometheus.Gauge
	RevokedTasksTotal            prometheus.Counter
	RevokedResultSuppressedTotal prometheus.Counter
	OrphanedLeaseCount           prometheus.Gauge
	GRPCServerHandledTotal       *prometheus.CounterVec
	GRPCServerHandlingSeconds    *prometheus.HistogramVec
	TaskDeadlineExceededTotal    *prometheus.CounterVec
	TaskRetryTotal               *prometheus.CounterVec
	// Week 4
	HeartbeatTotal               *prometheus.CounterVec
	HeartbeatFailuresTotal       prometheus.Counter
	HeartbeatLatencyMs           prometheus.Histogram
	WorkerRevokesTotal           *prometheus.CounterVec
	WorkerCircuitBreakerState    prometheus.Gauge
	WorkerTaskQueueDepth         prometheus.Gauge
	WorkerBankCallDurationSec    prometheus.Histogram
}

func NewMetrics() *Metrics {
	return &Metrics{
		TasksTotal:                   metrics.TasksTotal,
		ActiveTasks:                  metrics.ActiveTasks,
		BankRequestDuration:          metrics.BankRequestDuration,
		TaskDuration:                 metrics.TaskDurationSeconds,
		WorkerSaturation:             metrics.WorkerSaturation,
		RevokedTasksTotal:            metrics.RevokedTasksTotal,
		RevokedResultSuppressedTotal: metrics.RevokedResultSuppressedTotal,
		OrphanedLeaseCount:           metrics.OrphanedLeaseCount,
		GRPCServerHandledTotal:       metrics.GRPCServerHandledTotal,
		GRPCServerHandlingSeconds:    metrics.GRPCServerHandlingSeconds,
		TaskDeadlineExceededTotal:    metrics.TaskDeadlineExceededTotal,
		TaskRetryTotal:               metrics.TaskRetryTotal,
		// Week 4
		HeartbeatTotal:               metrics.HeartbeatTotal,
		HeartbeatFailuresTotal:       metrics.HeartbeatFailuresTotal,
		HeartbeatLatencyMs:           metrics.HeartbeatLatencyMs,
		WorkerRevokesTotal:           metrics.WorkerRevokesTotal,
		WorkerCircuitBreakerState:    metrics.WorkerCircuitBreakerState,
		WorkerTaskQueueDepth:         metrics.WorkerTaskQueueDepth,
		WorkerBankCallDurationSec:    metrics.WorkerBankCallDurationSeconds,
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
	m.BankRequestDuration.WithLabelValues(NormalizeBankResult(result)).Observe(durationMs)
}

// RecordTaskRevoked increments the revoked counter.
func (m *Metrics) RecordTaskRevoked() {
	m.RevokedTasksTotal.Inc()
}

// RecordRevokedTaskSuppressed increments the counter for discarded results.
func (m *Metrics) RecordRevokedTaskSuppressed() {
	m.RevokedResultSuppressedTotal.Inc()
}

// RecordDeadlineExceeded increments the deadline-exceeded counter for the given stage.
// Valid stage values: "semaphore_wait", "bank", "c4_log", "outbox".
func (m *Metrics) RecordDeadlineExceeded(stage string) {
	m.TaskDeadlineExceededTotal.WithLabelValues(stage).Inc()
}

// RecordTaskRetry increments the retry counter with the given attempt label.
func (m *Metrics) RecordTaskRetry(attempt string) {
	m.TaskRetryTotal.WithLabelValues(attempt).Inc()
}

// RecordRevokeOutcome increments the revoke counter with outcome label.
// Valid outcomes: "cancelled", "already_completed", "not_found".
func (m *Metrics) RecordRevokeOutcome(outcome string) {
	m.WorkerRevokesTotal.WithLabelValues(outcome).Inc()
}

// RecordHeartbeatSend increments the heartbeat counter with status label.
func (m *Metrics) RecordHeartbeatSend(status string) {
	m.HeartbeatTotal.WithLabelValues(status).Inc()
}

// RecordHeartbeatFailure increments the heartbeat failure counter.
func (m *Metrics) RecordHeartbeatFailure() {
	m.HeartbeatFailuresTotal.Inc()
}

// RecordHeartbeatLatency observes heartbeat round-trip time.
func (m *Metrics) RecordHeartbeatLatency(ms float64) {
	m.HeartbeatLatencyMs.Observe(ms)
}

// SetCircuitBreakerState sets the circuit breaker state gauge.
func (m *Metrics) SetCircuitBreakerState(state float64) {
	m.WorkerCircuitBreakerState.Set(state)
}

// SetTaskQueueDepth sets the gauge of tasks waiting for a slot.
func (m *Metrics) SetTaskQueueDepth(depth float64) {
	m.WorkerTaskQueueDepth.Set(depth)
}

// RecordBankCallDuration observes bank call duration in seconds.
func (m *Metrics) RecordBankCallDuration(seconds float64) {
	m.WorkerBankCallDurationSec.Observe(seconds)
}
