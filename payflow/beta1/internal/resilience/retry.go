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
func ExecuteWithRetry(ctx context.Context, operation RetryFunc, maxAttempts int, baseDelay time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check for context cancellation before starting or retrying
		if err := ctx.Err(); err != nil {
			return err
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
		// current_delay = base * 2^attempt
		backoff := float64(baseDelay) * float64(int64(1)<<uint(attempt))
		// jitter = rand(0, backoff)
		jitterDelay := time.Duration(rand.Float64() * backoff)

		select {
		case <-time.After(jitterDelay):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("resilience: operation failed after %d attempts: %w", maxAttempts, lastErr)
}
