package domain

import "errors"

var (
	ErrTaskRevoked       = errors.New("task has been revoked by coordinator")
	ErrStaleEpoch        = errors.New("task epoch is stale — coordinator may have changed")
	ErrIdempotencyExists = errors.New("idempotency key already exists in C4 — returning cached result")
	ErrBankUnavailable   = errors.New("mock bank API is unavailable after all retry attempts")
	ErrCoordinatorLost   = errors.New("lost connection to coordinator — entering reconnect loop")
)
