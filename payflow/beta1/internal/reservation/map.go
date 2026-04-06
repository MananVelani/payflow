package reservation

import (
	"context"
	"sync"
	"time"

	apperrors "github.com/your-org/payflow/worker/internal/errors"
)



// State represents the lifecycle of an idempotency key within this worker instance.
type State int

const (
	StateNotStarted State = iota // zero value; key unknown to this worker
	StateInProgress              // bank call is in flight
	StateCompleted               // bank responded; result reported to C2
)

// entry holds state plus a timestamp for TTL-based cleanup.
type entry struct {
	state     State
	createdAt time.Time
}

// ErrAlreadyInProgress is kept for backward-compat but now wraps the canonical sentinel.
var ErrAlreadyInProgress = apperrors.ErrIdempotentKey

// LocalStore is a thread-safe, in-process idempotency reservation store.
type LocalStore struct {
	mu      sync.Mutex
	entries map[string]*entry
	ttl     time.Duration // how long to keep completed entries
}


// NewLocalStore returns a LocalStore with the given TTL for completed entries.
func NewLocalStore(ttl time.Duration) *LocalStore {
	return &LocalStore{
		entries: make(map[string]*entry),
		ttl:     ttl,
	}
}


// Reserve attempts to transition key from NotStarted → InProgress.
func (m *LocalStore) Reserve(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, exists := m.entries[key]
	if exists {
		switch e.state {
		case StateInProgress:
			return false, nil
		case StateCompleted:
			return true, nil
		}
	}

	m.entries[key] = &entry{state: StateInProgress, createdAt: time.Now()}
	return true, nil
}


// Complete transitions key from InProgress → Completed.
func (m *LocalStore) Complete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		e.state = StateCompleted
	}
}


// Release removes a key reservation.
func (m *LocalStore) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
	return nil
}


// StateOf returns the current state of a key.
func (m *LocalStore) StateOf(key string) State {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return StateNotStarted
	}
	return e.state
}


// Cleanup removes completed entries older than the configured TTL.
func (m *LocalStore) Cleanup() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := time.Now().Add(-m.ttl)
	removed := 0
	for k, e := range m.entries {
		if e.state == StateCompleted && e.createdAt.Before(cutoff) {
			delete(m.entries, k)
			removed++
		}
	}
	return removed
}


// Size returns the current number of tracked keys.
func (m *LocalStore) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

