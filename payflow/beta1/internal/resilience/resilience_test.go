package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/resilience"
	"go.uber.org/zap"
)

func TestFullJitterDistribution(t *testing.T) {
	ctx := context.Background()
	baseDelay := 100 * time.Millisecond
	
	for i := 0; i < 50; i++ {
		start := time.Now()
		var count int
		_ = resilience.ExecuteWithRetry(ctx, func() error {
			count++
			if count < 2 {
				return errors.New("fail once")
			}
			return nil
		}, 3, baseDelay)

		elapsed := time.Since(start)
		// Delay should be rand(0, baseDelay*1) = rand(0, 100ms)
		if elapsed > 150*time.Millisecond { // 50ms buffer for execution overhead
			t.Errorf("jitter delay exceeded expected range: %v", elapsed)
		}
	}
}

func TestJitterNeverExceedsMaxDelay(t *testing.T) {
	ctx := context.Background()
	baseDelay := 10 * time.Millisecond
	maxAttempts := 5 // 2^4 = 16 * 10ms = 160ms max jitter delay
	
	count := 0
	start := time.Now()
	_ = resilience.ExecuteWithRetry(ctx, func() error {
		count++
		return errors.New("always fail")
	}, maxAttempts, baseDelay)

	elapsed := time.Since(start)
	
	// Max theoretical delay = sum(base * 2^i) i=0 to 3 = 10*(1+2+4+8) = 150ms
	// Full jitter means each step i is rand(0, base * 2^i)
	maxPossible := 150 * time.Millisecond
	if elapsed > maxPossible + 50*time.Millisecond {
		t.Errorf("total delay %v exceeded max possible %v", elapsed, maxPossible)
	}
}

func TestRetryRespectsContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	
	err := resilience.ExecuteWithRetry(ctx, func() error {
		return nil
	}, 3, 100*time.Millisecond)
	
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetrySucceedsOnSecondAttempt(t *testing.T) {
	ctx := context.Background()
	count := 0
	err := resilience.ExecuteWithRetry(ctx, func() error {
		count++
		if count == 1 {
			return errors.New("first fail")
		}
		return nil
	}, 3, 10*time.Millisecond)
	
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 attempts, got %d", count)
	}
}

func TestCircuitBreakerStaysClosedAt10Percent(t *testing.T) {
	logger := zap.NewNop()
	cb := resilience.NewBankCircuitBreaker(logger)
	
	// 10 requests, 1 failure (10%)
	for i := 0; i < 10; i++ {
		_, _ = cb.Execute(func() (interface{}, error) {
			if i == 0 {
				return nil, errors.New("fail")
			}
			return "ok", nil
		})
	}
	
	// Should still be able to execute
	_, err := cb.Execute(func() (interface{}, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("breaker should be CLOSED at 10%% failure, got error: %v", err)
	}
}

func TestCircuitBreakerOpensAbove50Percent(t *testing.T) {
	logger := zap.NewNop()
	cb := resilience.NewBankCircuitBreaker(logger)
	
	// 4 requests, 3 failures (75% > 50%)
	for i := 0; i < 4; i++ {
		_, _ = cb.Execute(func() (interface{}, error) {
			if i < 3 {
				return nil, errors.New("fail")
			}
			return "ok", nil
		})
	}
	
	// Next request should fail immediately with breaker open
	_, err := cb.Execute(func() (interface{}, error) {
		return "ok", nil
	})
	if err == nil || err.Error() != "circuit breaker is open" {
		t.Fatalf("expected circuit breaker is open, got %v", err)
	}
}
