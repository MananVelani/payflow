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
}

// NewHandler creates a new health handler.
func NewHandler(outboxRunning func() bool, streamReady <-chan struct{}, grpcReady func() bool) *Handler {
	return &Handler{
		outboxRunning: outboxRunning,
		streamReady:   streamReady,
		grpcReady:     grpcReady,
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
		h.fail(w, "gRPC server not ready")
		return
	}

	// 2. Outbox goroutine is running
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
