// Package heartbeat provides the periodic heartbeat sender and supporting
// data structures (RingBuffer) for maintaining the worker's presence in
// the C2 coordinator's pool.
package heartbeat

import "sync"

// RingBuffer is a fixed-capacity, thread-safe circular buffer of int64
// values used to compute a rolling average of task durations. It is
// written by task goroutines and read by the heartbeat sender goroutine.
type RingBuffer struct {
	mu   sync.Mutex
	data []int64
	pos  int
	full bool
	cap  int
}

// NewRingBuffer creates a RingBuffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 100
	}
	return &RingBuffer{
		data: make([]int64, capacity),
		cap:  capacity,
	}
}

// Push adds a value to the ring, overwriting the oldest entry when full.
func (r *RingBuffer) Push(v int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[r.pos] = v
	r.pos = (r.pos + 1) % r.cap
	if r.pos == 0 || r.full {
		r.full = true
	}
}

// Avg returns the arithmetic mean of all non-zero entries in the ring.
// Returns 0 if the buffer is empty.
func (r *RingBuffer) Avg() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := r.cap
	if !r.full {
		count = r.pos
	}
	if count == 0 {
		return 0
	}

	var sum int64
	var n int
	for i := 0; i < count; i++ {
		if r.data[i] != 0 {
			sum += r.data[i]
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return float64(sum) / float64(n)
}

// Count returns the number of entries currently stored.
func (r *RingBuffer) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.full {
		return r.cap
	}
	return r.pos
}
