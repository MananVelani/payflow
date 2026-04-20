package reservation

import (
	"context"
	"time"
)

// Store defines the logic for acquiring and releasing idempotency key reservations.
type Store interface {
	// Reserve attempts to transition a key to InProgress.
	// Returns true if the reservation was successfully acquired, false otherwise.
	Reserve(ctx context.Context, key string, ttl time.Duration) (bool, error)
	
	// Release removes the reservation, allowing the key to be picked up again.
	// This usually happens on task failure or revocation.
	Release(ctx context.Context, key string) error
}
