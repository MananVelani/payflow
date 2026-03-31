package reservation

import (
	"errors"
	"sync"
	"time"
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

// ErrAlreadyInProgress is returned when a goroutine tries to reserve a key
// that another goroutine in this binary is already processing.
var ErrAlreadyInProgress = errors.New("reservation: key already in progress on this worker")

// Map is a thread-safe, in-process idempotency reservation store.
// It is NOT a replacement for C4 — it is a local guard within a single worker binary.
// TTL-based cleanup prevents unbounded memory growth.
type Map struct {
	mu      sync.Mutex
	entries map[string]*entry
	ttl     time.Duration // how long to keep completed entries
}

// New returns a Map with the given TTL for completed entries.
// Recommended TTL: 5 * MAX_TASK_DURATION to cover all retry windows.
func New(ttl time.Duration) *Map {
	m := &Map{
		entries: make(map[string]*entry),
		ttl:     ttl,
	}
	return m
}

// Reserve attempts to transition key from NotStarted → InProgress.
// Returns ErrAlreadyInProgress if the key is already InProgress.
// Returns nil if the reservation was successfully acquired.
// The caller MUST call Complete or Release when done.
func (m *Map) Reserve(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, exists := m.entries[key]
	if exists {
		switch e.state {
		case StateInProgress:
			return ErrAlreadyInProgress
		case StateCompleted:
			// Already done — caller should have hit C4 cache; let it through
			// C4 is the authority; we just surface the local state
			return nil
		}
	}

	m.entries[key] = &entry{state: StateInProgress, createdAt: time.Now()}
	return nil
}

// Complete transitions key from InProgress → Completed.
// Call this after a successful bank response AND after reporting to C2.
func (m *Map) Complete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		e.state = StateCompleted
	}
}

// Release removes a key reservation. Call this on bank failure or task revocation
// so the key can be retried by another goroutine or worker.
func (m *Map) Release(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
}

// StateOf returns the current state of a key. Used for debugging and metrics.
func (m *Map) StateOf(key string) State {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return StateNotStarted
	}
	return e.state
}

// Cleanup removes completed entries older than the configured TTL.
// Call this periodically from a background goroutine (e.g., every 30 seconds).
func (m *Map) Cleanup() int {
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

// Size returns the current number of tracked keys. Used for the Prometheus gauge.
func (m *Map) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}
