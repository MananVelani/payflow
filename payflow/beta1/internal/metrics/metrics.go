package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	TasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "worker_tasks_total", Help: "Total tasks processed."},
		[]string{"status"},
	)
	ActiveTasks = prometheus.NewGauge(
		prometheus.GaugeOpts{Name: "worker_active_tasks", Help: "Tasks currently executing."},
	)
	BankRequestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "worker_bank_request_duration_ms",
			Help:    "Mock Bank API call duration in milliseconds.",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000},
		},
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
)

func Register() {
	prometheus.MustRegister(TasksTotal, ActiveTasks, BankRequestDuration, BankRetriesTotal, HeartbeatSentTotal, RevokedTasksTotal)
}
