package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/your-org/payflow/worker/internal/loadreport"
)

func TestStatusHandler_ReturnsValidJSON(t *testing.T) {
	snap := loadreport.LoadSnapshot{
		ActiveTasks:    3,
		MaxTasks:       10,
		ProcessedTotal: 42,
	}
	handler := NewStatusHandler(
		"test-worker-1",
		func() loadreport.LoadSnapshot { return snap },
		func() string { return "closed" },
		func() int { return 0 },
	)

	req := httptest.NewRequest(http.MethodGet, "/metrics/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp StatusResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, "test-worker-1", resp.WorkerID)
	assert.Equal(t, int32(3), resp.ActiveTasks)
	assert.Equal(t, int32(10), resp.MaxTasks)
	assert.Equal(t, "closed", resp.CircuitBreaker)
	assert.Equal(t, 0, resp.OutboxPending)
	assert.Equal(t, int64(42), resp.ProcessedTotal)
	assert.GreaterOrEqual(t, resp.UptimeSeconds, float64(0))
}

func TestStatusHandler_CircuitBreakerOpen(t *testing.T) {
	handler := NewStatusHandler(
		"test-worker-2",
		func() loadreport.LoadSnapshot {
			return loadreport.LoadSnapshot{ActiveTasks: 10, MaxTasks: 10}
		},
		func() string { return "open" },
		func() int { return 5 },
	)

	req := httptest.NewRequest(http.MethodGet, "/metrics/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var resp StatusResponse
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)

	assert.Equal(t, "open", resp.CircuitBreaker)
	assert.Equal(t, 5, resp.OutboxPending)
}
