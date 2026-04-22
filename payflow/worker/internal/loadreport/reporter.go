// Package loadreport computes and exposes per-worker load metrics for the
// coordinator's weighted load balancer (Advanced Feature A02). It tracks
// task duration via a lock-free ring buffer and derives processing_capacity
// from the configured semaphore size and current occupancy.
package loadreport

import (
	"sync/atomic"
	"time"

	"github.com/your-org/payflow/worker/internal/heartbeat"
)

// LoadSnapshot captures the worker's current resource utilisation at a point
// in time. Used for heartbeat payload and /metrics/health JSON endpoint.
type LoadSnapshot struct {
	ActiveTasks        int32
	MaxTasks           int32
	ProcessedTotal     int64
	AvgDurationMs      float64 // rolling average of last 100 tasks
	ProcessingCapacity float32 // (max - active) / max, range [0.0, 1.0]
	SaturationPct      float32 // active / max × 100
}

// Reporter computes worker-level load metrics from shared atomic counters
// and a ring buffer of recent task durations.
type Reporter struct {
	durationRing  *heartbeat.RingBuffer
	maxConcurrent int64
	activeTasks   *atomic.Int64
	processedTotal *atomic.Int64
}

// NewReporter creates a Reporter wired to the worker's concurrency state.
func NewReporter(
	durationRing *heartbeat.RingBuffer,
	maxConcurrent int,
	activeTasks *atomic.Int64,
	processedTotal *atomic.Int64,
) *Reporter {
	return &Reporter{
		durationRing:   durationRing,
		maxConcurrent:  int64(maxConcurrent),
		activeTasks:    activeTasks,
		processedTotal: processedTotal,
	}
}

// RecordTaskDuration records the wall-clock duration of a completed task.
// Called by WorkerServiceImpl after every task completes (success or failure).
func (r *Reporter) RecordTaskDuration(d time.Duration) {
	r.durationRing.Push(d.Nanoseconds())
}

// Snapshot returns the current load metrics for embedding in a heartbeat.
func (r *Reporter) Snapshot() LoadSnapshot {
	active := r.activeTasks.Load()
	processed := r.processedTotal.Load()
	maxCap := r.maxConcurrent

	var capacity float32
	var saturation float32
	if maxCap > 0 {
		capacity = float32(maxCap-active) / float32(maxCap)
		if capacity < 0 {
			capacity = 0
		}
		saturation = float32(active) / float32(maxCap) * 100
	}

	avgNs := r.durationRing.Avg()
	avgMs := avgNs / float64(time.Millisecond)

	return LoadSnapshot{
		ActiveTasks:        int32(active),
		MaxTasks:           int32(maxCap),
		ProcessedTotal:     processed,
		AvgDurationMs:      avgMs,
		ProcessingCapacity: capacity,
		SaturationPct:      saturation,
	}
}
