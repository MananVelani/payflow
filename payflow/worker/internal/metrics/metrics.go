package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	TasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_tasks_total", Help: "Total tasks processed."},
		[]string{"status", "bank_result"},
	)
	ActiveTasks = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "worker_active_tasks", Help: "Tasks currently executing."},
	)
	BankRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "worker_bank_request_duration_ms",
			Help:    "Mock Bank API call duration in milliseconds.",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"bank_result"},
	)
	TaskDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "worker_task_duration_seconds",
			Help:    "Total time taken to process a task.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)
	WorkerSaturation = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "worker_saturation", Help: "Worker saturation (active_tasks / max_tasks)."},
	)
	BankRetriesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "worker_bank_retries_total", Help: "Total bank retry attempts."},
	)
	HeartbeatSentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "worker_heartbeat_sent_total", Help: "Heartbeat pings sent to C2."},
	)
	RevokedTasksTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "worker_revoked_tasks_total", Help: "Tasks abandoned due to REVOKE from C2."},
	)
	RevokedResultSuppressedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "worker_revoked_result_suppressed_total", Help: "Results discarded because the task was revoked before delivery."},
	)
	TaskDeadlineExceededTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_task_deadline_exceeded_total",
			Help: "Tasks that hit context.DeadlineExceeded, by stage (semaphore_wait, bank, c4_log, outbox).",
		},
		[]string{"stage"},
	)
	TaskRetryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_task_retry_total", Help: "Total task-level retry attempts (re-dispatched by C2)."},
		[]string{"attempt"},
	)
	OrphanedLeaseCount = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "worker_orphaned_lease_count", Help: "Count of orphaned task leases recovered at startup."},
	)
	GRPCServerHandledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "grpc_server_handled_total", Help: "Total gRPC calls processed."},
		[]string{"grpc_service", "grpc_method", "grpc_code"},
	)
	GRPCServerHandlingSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "gRPC handling duration in seconds.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"grpc_service", "grpc_method"},
	)

	// ── Week 4 Metrics ──────────────────────────────────────────────────────

	// HeartbeatTotal counts heartbeat sends by status (ok|error).
	HeartbeatTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_heartbeat_total", Help: "Heartbeat pings sent by status."},
		[]string{"status"},
	)
	// HeartbeatFailuresTotal counts consecutive heartbeat failures.
	HeartbeatFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{Name: "worker_heartbeat_failures_total", Help: "Total heartbeat send failures."},
	)
	// HeartbeatLatencyMs tracks heartbeat round-trip latency.
	HeartbeatLatencyMs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "worker_heartbeat_latency_ms",
			Help:    "Heartbeat gRPC call latency in milliseconds.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
	)
	// WorkerRevokesTotal counts hard revocations by outcome.
	WorkerRevokesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_revokes_total", Help: "Hard revocations by outcome."},
		[]string{"outcome"},
	)
	// WorkerCircuitBreakerState tracks the circuit breaker state (0=closed, 1=half-open, 2=open).
	WorkerCircuitBreakerState = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "worker_circuit_breaker_state", Help: "CB state: 0=closed, 1=half-open, 2=open."},
	)
	// WorkerTaskQueueDepth is a gauge of tasks waiting for a semaphore slot.
	WorkerTaskQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "worker_task_queue_depth", Help: "Tasks waiting for a semaphore slot."},
	)
	// WorkerBankCallDurationSeconds is a histogram of bank API call latency.
	WorkerBankCallDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "worker_bank_call_duration_seconds",
			Help:    "Bank API call latency in seconds.",
			Buckets: []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0},
		},
	)
)

func Register() {
	prometheus.MustRegister(
		TasksTotal,
		ActiveTasks,
		BankRequestDuration,
		TaskDurationSeconds,
		WorkerSaturation,
		BankRetriesTotal,
		HeartbeatSentTotal,
		RevokedTasksTotal,
		RevokedResultSuppressedTotal,
		OrphanedLeaseCount,
		GRPCServerHandledTotal,
		GRPCServerHandlingSeconds,
		TaskDeadlineExceededTotal,
		TaskRetryTotal,
		// Week 4
		HeartbeatTotal,
		HeartbeatFailuresTotal,
		HeartbeatLatencyMs,
		WorkerRevokesTotal,
		WorkerCircuitBreakerState,
		WorkerTaskQueueDepth,
		WorkerBankCallDurationSeconds,
	)
}

