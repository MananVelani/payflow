package resilience

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// RetryFunc is the operation to be retried.
type RetryFunc func() error

// ExecuteWithRetry runs an operation with exponential backoff and full jitter.
// logic: delay = rand(0, base * 2^attempt)
// If the context is already expired before an attempt begins, the function
// returns immediately with a wrapped context.DeadlineExceeded — it does NOT
// burn retry budget on a task that has already expired.
func ExecuteWithRetry(ctx context.Context, operation RetryFunc, maxAttempts int, baseDelay, maxDelay time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check for context expiry BEFORE starting each attempt.
		// This prevents burning retry budget on an already-expired task.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("resilience: context done before attempt %d: %w", attempt, err)
		}

		lastErr = operation()
		if lastErr == nil {
			return nil
		}

		// Don't sleep after the final attempt
		if attempt == maxAttempts-1 {
			break
		}

		// Full Jitter: exponential backoff cap + random spread
		// current_delay = min(base * 2^attempt, maxDelay)
		backoff := float64(baseDelay) * float64(int64(1)<<uint(attempt))
		if backoff > float64(maxDelay) {
			backoff = float64(maxDelay)
		}
		// jitter = rand(0, backoff)
		jitterDelay := time.Duration(rand.Float64() * backoff)

		select {
		case <-time.After(jitterDelay):
			continue
		case <-ctx.Done():
			return fmt.Errorf("resilience: context done during backoff after attempt %d: %w", attempt, ctx.Err())
		}
	}
	return fmt.Errorf("resilience: operation failed after %d attempts: %w", maxAttempts, lastErr)
}
