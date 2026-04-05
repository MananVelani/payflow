package health

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/your-org/payflow/worker/internal/loadreport"
)

// StatusResponse is the JSON payload for /metrics/health.
type StatusResponse struct {
	WorkerID       string  `json:"worker_id"`
	ActiveTasks    int32   `json:"active_tasks"`
	MaxTasks       int32   `json:"max_tasks"`
	CircuitBreaker string  `json:"circuit_breaker"`
	OutboxPending  int     `json:"outbox_pending"`
	UptimeSeconds  float64 `json:"uptime_seconds"`
	ProcessedTotal int64   `json:"processed_total"`
}

// StatusHandler serves the /metrics/health JSON status endpoint for
// M5's dashboard to display in the status panel.
type StatusHandler struct {
	workerID       string
	startTime      time.Time
	snapshotFn     func() loadreport.LoadSnapshot
	cbStateFn      func() string
	outboxPendingFn func() int
}

// NewStatusHandler creates a handler for the /metrics/health endpoint.
func NewStatusHandler(
	workerID string,
	snapshotFn func() loadreport.LoadSnapshot,
	cbStateFn func() string,
	outboxPendingFn func() int,
) *StatusHandler {
	return &StatusHandler{
		workerID:        workerID,
		startTime:       time.Now(),
		snapshotFn:      snapshotFn,
		cbStateFn:       cbStateFn,
		outboxPendingFn: outboxPendingFn,
	}
}

// ServeHTTP handles GET /metrics/health and returns a JSON summary of
// the worker's current state.
func (s *StatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	snap := s.snapshotFn()

	cbState := "closed"
	if s.cbStateFn != nil {
		cbState = s.cbStateFn()
	}

	outboxPending := 0
	if s.outboxPendingFn != nil {
		outboxPending = s.outboxPendingFn()
	}

	resp := StatusResponse{
		WorkerID:       s.workerID,
		ActiveTasks:    snap.ActiveTasks,
		MaxTasks:       snap.MaxTasks,
		CircuitBreaker: cbState,
		OutboxPending:  outboxPending,
		UptimeSeconds:  time.Since(s.startTime).Seconds(),
		ProcessedTotal: snap.ProcessedTotal,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
