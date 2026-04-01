package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"github.com/your-org/payflow/worker/internal/observability"
)


func TestMockBankClient_FailsAfterRetries(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := MockBankClientConfig{
		BaseURL: "http://localhost:99999", FailRate: 1.0,
		MaxAttempts: 3, BaseDelayMS: 1, HTTPTimeout: 50 * time.Millisecond,
		CBMaxRequests: 3, CBInterval: 10 * time.Second,
		CBTimeout: 5 * time.Second, CBMinRequests: 1,
	}
	metrics := observability.NewMetrics()
	client := NewProductionMockBankClient(cfg, logger, metrics)
	_, err := client.Charge(context.Background(), "idem-key-001", 100.0, "USD", "merchant-1")
	assert.Error(t, err, "expected failure with 100% fail rate")
}

func TestMockBankClient_CircuitBreakerOpens(t *testing.T) {
	logger := zaptest.NewLogger(t)
	cfg := MockBankClientConfig{
		FailRate: 1.0, MaxAttempts: 1, BaseDelayMS: 1,
		HTTPTimeout: 50 * time.Millisecond,
		CBMaxRequests: 1, CBInterval: time.Second,
		CBTimeout: time.Second, CBMinRequests: 3,
	}
	metrics := observability.NewMetrics()
	client := NewProductionMockBankClient(cfg, logger, metrics)
	for i := 0; i < 10; i++ {

		_, _ = client.Charge(context.Background(), "idem-cb-test", 50.0, "USD", "m001")
	}
	start := time.Now()
	_, err := client.Charge(context.Background(), "idem-cb-test-2", 50.0, "USD", "m001")
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond, "circuit breaker should fail fast — no HTTP latency")
}
