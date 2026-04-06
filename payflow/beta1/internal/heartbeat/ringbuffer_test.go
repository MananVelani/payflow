package heartbeat

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingBuffer_Push_And_Avg(t *testing.T) {
	rb := NewRingBuffer(5)

	// Empty buffer should return 0.
	assert.Equal(t, float64(0), rb.Avg())
	assert.Equal(t, 0, rb.Count())

	// Push a single value.
	rb.Push(100)
	assert.Equal(t, float64(100), rb.Avg())
	assert.Equal(t, 1, rb.Count())

	// Push more values and verify average.
	rb.Push(200)
	rb.Push(300)
	assert.InDelta(t, 200.0, rb.Avg(), 0.01) // (100+200+300)/3 = 200
	assert.Equal(t, 3, rb.Count())
}

func TestRingBuffer_WrapAround(t *testing.T) {
	rb := NewRingBuffer(3)

	rb.Push(10)
	rb.Push(20)
	rb.Push(30) // buffer is now full
	assert.Equal(t, 3, rb.Count())
	assert.InDelta(t, 20.0, rb.Avg(), 0.01)

	// Push a 4th value — wraps around, overwrites the 10.
	rb.Push(40)
	assert.Equal(t, 3, rb.Count())
	// Buffer now contains [40, 20, 30]
	assert.InDelta(t, 30.0, rb.Avg(), 0.01) // (20+30+40)/3 = 30
}

func TestRingBuffer_ConvergesAfterWrap(t *testing.T) {
	rb := NewRingBuffer(100)

	// Push 200 samples — ring wraps at 100.
	for i := 1; i <= 200; i++ {
		rb.Push(int64(i * 1000))
	}
	assert.Equal(t, 100, rb.Count())

	// After 200 pushes, the ring holds values 101000..200000.
	// Average should be (101+102+...+200)*1000/100 = 150500.
	avg := rb.Avg()
	assert.InDelta(t, 150500.0, avg, 1.0)
}

func TestRingBuffer_ConcurrentPush(t *testing.T) {
	rb := NewRingBuffer(100)
	const goroutines = 10
	const pushesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < pushesPerGoroutine; i++ {
				rb.Push(int64(offset*pushesPerGoroutine + i + 1))
			}
		}(g)
	}
	wg.Wait()

	// All 1000 values were pushed; ring has capacity 100 so count=100.
	assert.Equal(t, 100, rb.Count())
	// Average must be positive and non-zero.
	assert.Greater(t, rb.Avg(), float64(0))
}

func TestRingBuffer_ZeroCapacity(t *testing.T) {
	// Zero or negative capacity should default to 100.
	rb := NewRingBuffer(0)
	assert.Equal(t, 100, rb.cap)
}

// BenchmarkRingBufferPush measures push throughput.
func BenchmarkRingBufferPush(b *testing.B) {
	rb := NewRingBuffer(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(int64(i))
	}
}

// BenchmarkRingBufferAvg measures avg computation throughput.
func BenchmarkRingBufferAvg(b *testing.B) {
	rb := NewRingBuffer(100)
	for i := 0; i < 100; i++ {
		rb.Push(int64(i * 1000))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rb.Avg()
	}
}

// TestRingBuffer_TimeDurationPush verifies usage with time.Duration values.
func TestRingBuffer_TimeDurationPush(t *testing.T) {
	rb := NewRingBuffer(100)
	durations := []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		150 * time.Millisecond,
	}
	for _, d := range durations {
		rb.Push(d.Nanoseconds())
	}
	avgNs := rb.Avg()
	avgMs := avgNs / float64(time.Millisecond)
	assert.InDelta(t, 100.0, avgMs, 0.1) // avg of 50,100,150 = 100ms
}
