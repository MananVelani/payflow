package service

import (
	"strconv"
	"sync"
)

// RetryTracker tracks the number of task-level retry attempts based on idempotency keys.
type RetryTracker struct {
	mu         sync.Mutex
	counts     map[string]int // idempotency_key → attempt count
	maxRetries int
}

// NewRetryTracker creates a tracker with the given maximum logic retries.
func NewRetryTracker(maxRetries int) *RetryTracker {
	return &RetryTracker{
		counts:     make(map[string]int),
		maxRetries: maxRetries,
	}
}

// ShouldRetry returns true if the current attempt count for the key is below the limit.
func (r *RetryTracker) ShouldRetry(idempotencyKey string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[idempotencyKey] < r.maxRetries
}

// RecordAttempt increments the attempt counter for the given idempotency key.
// Returns the new attempt number as a string for labeling.
func (r *RetryTracker) RecordAttempt(idempotencyKey string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[idempotencyKey]++
	count := r.counts[idempotencyKey]
	if count > r.maxRetries {
		return "max_exceeded"
	}
	return strconv.Itoa(count)
}

// Clear removes the retry tracking for an idempotency key (e.g., on success or final failure).
func (r *RetryTracker) Clear(idempotencyKey string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.counts, idempotencyKey)
}
