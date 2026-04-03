package fence

import (
	"context"
	"fmt"
	"sync/atomic"

	apperrors "github.com/your-org/payflow/worker/internal/errors"
)

// EpochValidator enforces monotonically increasing epoch validation.
// It is safe for concurrent use by multiple goroutines.
// Thread-safety: uses atomic int64 for zero-lock reads on the hot path.
type EpochValidator struct {
	lastSeenEpoch atomic.Int64 // atomically read/written; no mutex needed
}

// NewEpochValidator returns a validator initialised to epoch 0.
// Workers must call UpdateEpoch with C2's initial epoch during RegisterWorker.
func NewEpochValidator() *EpochValidator {
	return &EpochValidator{}
}

// ValidationError is a typed error so callers can distinguish fence failures
// from other errors with errors.As(). It wraps ErrEpochStale so that
// errors.Is(err, apperrors.ErrEpochStale) returns true through any wrap depth.
type ValidationError struct {
	IncomingEpoch int64
	LastSeen      int64
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf(
		"fencing: stale task rejected — incoming epoch %d < last seen %d",
		e.IncomingEpoch, e.LastSeen,
	)
}

// Unwrap links ValidationError to the sentinel so errors.Is propagates.
func (e *ValidationError) Unwrap() error { return apperrors.ErrEpochStale }

// ValidateAndUpdate checks that incomingEpoch >= lastSeenEpoch and atomically
// updates lastSeenEpoch if valid. Returns *ValidationError on failure so callers
// can log the exact epochs without string parsing.
//
// Design note: we allow equal epochs (same leader, same epoch) to handle the
// case where C2 sends multiple tasks before incrementing. We only reject LESS THAN.
func (v *EpochValidator) ValidateAndUpdate(ctx context.Context, incomingEpoch int64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		current := v.lastSeenEpoch.Load()
		if incomingEpoch < current {
			return &ValidationError{
				IncomingEpoch: incomingEpoch,
				LastSeen:      current,
			}
		}
		// CAS: only update if nobody else changed it between our Load and now
		if v.lastSeenEpoch.CompareAndSwap(current, incomingEpoch) {
			return nil
		}
		// Another goroutine updated lastSeenEpoch concurrently — retry the loop
	}
}

// Epoch returns the current last-seen epoch. Used for heartbeat reporting to C2.
func (v *EpochValidator) Epoch() int64 {
	return v.lastSeenEpoch.Load()
}
