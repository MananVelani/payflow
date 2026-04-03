package health

import (
	"encoding/json"
	"net/http"
)

// Handler handles HTTP health check requests.
type Handler struct {
	outboxRunning func() bool
	streamReady   <-chan struct{}
	grpcReady     func() bool
	redisCheck    func() error // NEW: redis reachability check
}

// NewHandler creates a new health handler.
func NewHandler(outboxRunning func() bool, streamReady <-chan struct{}, grpcReady func() bool, redisCheck func() error) *Handler {
	return &Handler{
		outboxRunning: outboxRunning,
		streamReady:   streamReady,
		grpcReady:     grpcReady,
		redisCheck:    redisCheck,
	}
}

// Healthz returns 200 OK if the process is alive.
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Readyz returns 200 OK if the worker is ready to process tasks.
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. gRPC server Listen() setup finished
	if h.grpcReady != nil && !h.grpcReady() {
		h.fail(w, "grpc_not_ready")
		return
	}

	// 2. Redis reachability (if configured)
	if h.redisCheck != nil {
		if err := h.redisCheck(); err != nil {
			h.fail(w, "redis_unreachable")
			return
		}
	}

	// 3. Outbox goroutine is running
	if h.outboxRunning != nil && !h.outboxRunning() {
		h.fail(w, "outbox not running")
		return
	}

	// 3. C2 stream connection established at least once
	select {
	case <-h.streamReady:
		// Ready
	default:
		h.fail(w, "C2 stream not established")
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) fail(w http.ResponseWriter, reason string) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "unavailable",
		"reason": reason,
	})
}
