// Package errors defines typed sentinel errors and structured error wrappers
// for the PayFlow C3 worker service. All cross-package error discrimination
// must use errors.Is / errors.As against these values — never string inspection.
package errors

import "errors"

// Sentinel errors. Callers should wrap these with fmt.Errorf("context: %w", ErrXxx)
// and unwrap with errors.Is.
var (
	// ErrEpochStale is returned by the fence validator when the incoming epoch
	// is older than the last-seen epoch. Indicates a zombie task from a dead leader.
	ErrEpochStale = errors.New("epoch: stale token")

	// ErrIdempotentKey is returned when a reservation attempt finds the key
	// is already held (locally or in Redis). No retry should be attempted.
	ErrIdempotentKey = errors.New("reservation: key already in flight")

	// ErrSemaphoreFull is returned when the semaphore cannot grant a new slot
	// because the worker is already at MAX_CONCURRENT_TASKS capacity.
	ErrSemaphoreFull = errors.New("concurrency: semaphore at capacity")

	// ErrCircuitOpen is returned when the bank circuit breaker is open and
	// will not forward requests to the bank API.
	ErrCircuitOpen = errors.New("resilience: circuit breaker open")

	// ErrContractVersion is returned when a gRPC peer sends a mismatched
	// x-contract-version header, signalling an incompatible deployment.
	ErrContractVersion = errors.New("version: contract mismatch")
)

// TaskError is a structured error that wraps another error with task-level
// context. It enables log-based alerting on specific error classes without
// parsing message strings.
type TaskError struct {
	// TaskID is the identifier of the task that encountered the error.
	TaskID string

	// Stage identifies where in the pipeline the error occurred.
	// Valid values: "epoch_check" | "reservation" | "bank" | "c4_log" | "outbox"
	Stage string

	// Err is the underlying error (may itself wrap a sentinel).
	Err error
}

func (e *TaskError) Error() string {
	return "task " + e.TaskID + " failed at stage " + e.Stage + ": " + e.Err.Error()
}

// Unwrap allows errors.Is / errors.As to traverse through TaskError.
func (e *TaskError) Unwrap() error {
	return e.Err
}
