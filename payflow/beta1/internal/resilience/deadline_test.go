package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/your-org/payflow/worker/internal/resilience"
)

// TestRetry_DeadlineExceeded verifies:
// 1. A taskCtx that expires before the operation finishes causes the retry
//    wrapper to return within a reasonable margin (not burn the full budget).
// 2. The error wraps context.DeadlineExceeded so callers can detect it with errors.Is.
func TestRetry_DeadlineExceeded(t *testing.T) {
	// Context expires in 50ms — well within our test timeout.
	taskCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	// Mock bank that always sleeps 200ms — it will never finish before the deadline.
	slowBank := func() error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-taskCtx.Done():
			return taskCtx.Err()
		}
	}

	start := time.Now()
	err := resilience.ExecuteWithRetry(taskCtx, slowBank, 5, 10*time.Millisecond, 1*time.Second)
	elapsed := time.Since(start)

	// Must return well within 100ms (50ms deadline + small margin).
	assert.Less(t, elapsed, 100*time.Millisecond,
		"retry wrapper should abort promptly on deadline, elapsed=%s", elapsed)

	// Error must not be nil.
	require.Error(t, err)

	// Error must wrap context.DeadlineExceeded.
	assert.True(t, errors.Is(err, context.DeadlineExceeded),
		"expected error wrapping context.DeadlineExceeded, got: %v", err)
}

// TestRetry_MissingHeader_OK verifies that a client sending no version header
// receives codes.OK (the retry wrapper itself is version-agnostic; this test
// exercises the retry path where the operation succeeds immediately).
func TestRetry_SucceedsOnFirstAttempt(t *testing.T) {
	ctx := context.Background()

	called := 0
	op := func() error {
		called++
		return nil
	}

	err := resilience.ExecuteWithRetry(ctx, op, 3, 1*time.Millisecond, 10*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, called, "operation should be called exactly once on success")
}
