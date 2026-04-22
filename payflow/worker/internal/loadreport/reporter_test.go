package loadreport

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/your-org/payflow/worker/internal/heartbeat"
)

func TestSnapshot_ZeroCapacityWhenFull(t *testing.T) {
	active := &atomic.Int64{}
	processed := &atomic.Int64{}
	ring := heartbeat.NewRingBuffer(100)

	r := NewReporter(ring, 10, active, processed)

	// Set active == max
	active.Store(10)
	snap := r.Snapshot()
	assert.Equal(t, float32(0), snap.ProcessingCapacity)
	assert.InDelta(t, float32(100), snap.SaturationPct, 0.1)
}

func TestSnapshot_FullCapacityWhenIdle(t *testing.T) {
	active := &atomic.Int64{}
	processed := &atomic.Int64{}
	ring := heartbeat.NewRingBuffer(100)

	r := NewReporter(ring, 10, active, processed)

	// No active tasks
	active.Store(0)
	snap := r.Snapshot()
	assert.Equal(t, float32(1.0), snap.ProcessingCapacity)
	assert.Equal(t, float32(0), snap.SaturationPct)
}

func TestSnapshot_RollingAverageConverges(t *testing.T) {
	active := &atomic.Int64{}
	processed := &atomic.Int64{}
	ring := heartbeat.NewRingBuffer(100)

	r := NewReporter(ring, 10, active, processed)

	// Push 200 samples — ring wraps at 100
	for i := 1; i <= 200; i++ {
		r.RecordTaskDuration(time.Duration(i) * time.Millisecond)
	}

	snap := r.Snapshot()
	// After 200 pushes of 1ms..200ms, ring holds 101ms..200ms.
	// Average should be ~150.5ms.
	assert.InDelta(t, 150.5, snap.AvgDurationMs, 1.0)
}

func TestSnapshot_ConcurrentRecordTaskDuration(t *testing.T) {
	active := &atomic.Int64{}
	processed := &atomic.Int64{}
	ring := heartbeat.NewRingBuffer(100)

	r := NewReporter(ring, 10, active, processed)

	var wg sync.WaitGroup
	const goroutines = 10
	const pushes = 100

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < pushes; i++ {
				r.RecordTaskDuration(50 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	snap := r.Snapshot()
	// All samples are 50ms, so average should be ~50ms.
	assert.InDelta(t, 50.0, snap.AvgDurationMs, 1.0)
}

func TestSnapshot_ProcessedTotalTracked(t *testing.T) {
	active := &atomic.Int64{}
	processed := &atomic.Int64{}
	ring := heartbeat.NewRingBuffer(100)

	r := NewReporter(ring, 10, active, processed)

	processed.Store(42)
	snap := r.Snapshot()
	assert.Equal(t, int64(42), snap.ProcessedTotal)
}
