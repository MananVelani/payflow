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
	)
}

