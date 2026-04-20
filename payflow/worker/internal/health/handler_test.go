package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHandler_Healthz(t *testing.T) {
	h := NewHandler(nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	h.Healthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("expected status 'ok', got '%s'", resp["status"])
	}
}

func TestHandler_Readyz(t *testing.T) {
	var outboxRunning atomic.Bool
	readyCh := make(chan struct{})
	var grpcReady atomic.Bool

	h := NewHandler(outboxRunning.Load, readyCh, grpcReady.Load, nil)

	// Subtest 1: Initially not ready (gRPC not ready)
	{
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		h.Readyz(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 513 Unavailable, got %d", w.Code)
		}
	}

	// Subtest 2: gRPC ready, outbox not ready
	grpcReady.Store(true)
	{
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		h.Readyz(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 513 Unavailable, got %d", w.Code)
		}
	}

	// Subtest 3: gRPC and outbox ready, stream not ready
	outboxRunning.Store(true)
	{
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		h.Readyz(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("expected 513 Unavailable, got %d", w.Code)
		}
	}

	// Subtest 4: All ready
	close(readyCh)
	{
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		h.Readyz(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 OK, got %d", w.Code)
		}

		var resp map[string]string
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if resp["status"] != "ok" {
			t.Errorf("expected status 'ok', got '%s'", resp["status"])
		}
	}
}
