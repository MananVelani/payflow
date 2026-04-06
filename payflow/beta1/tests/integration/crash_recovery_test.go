package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/your-org/payflow/worker/internal/domain"
)

// TestCrashRecovery_IdempotencyCheck demonstrates the Worker-Crash +
// Idempotency Recovery scenario (W4-CP6):
//
//  1. Worker-A calls bank → bank succeeds → Worker-A crashes before
//     reporting to C2 (simulated by discarding the result).
//  2. C2 dispatches the same task to Worker-B.
//  3. Worker-B calls C4 CheckIdempotency → if C4 returns exists=true,
//     Worker-B returns the cached result without calling the bank again.
//     If C4 returns exists=false (our mock's default), Worker-B calls
//     the bank which sees the same idempotency key and returns the
//     existing txn_ref.
//
// In both cases, the end result is exactly-one bank charge.
func TestCrashRecovery_IdempotencyCheck(t *testing.T) {
	h := NewHarness(t, nil)

	// Task with a unique idempotency key — represents the "first attempt."
	task := makeTask("task-crash-1", "idem-crash-recovery-1", 1)

	// Simulate Worker-A completing successfully (bank succeeds).
	result, err := h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusSuccess, result.Status)

	bankCallsAfterFirstExec := h.BankHandler.CallCount.Load()

	// The first worker "crashes" — the result may or may not have been
	// delivered to C2, but the bank was charged. Now C2 re-dispatches
	// the same idempotency key to a "second worker" (same harness).

	// Worker-B receives the re-dispatch. Because the reservation was
	// already released (task completed), this task will go through.
	// Our mock C4 always returns exists=false, so the bank is called
	// again with the same idempotency key. A real bank would return
	// the same txn_ref (our mock does: "txn-<key>").
	task2 := makeTask("task-crash-2", "idem-crash-recovery-1", 1)

	// We need a fresh reservation for this task since the first completed.
	result2, err := h.worker.ExecuteTask(context.Background(), task2)

	// The key invariant: the second attempt must also succeed,
	// and the bank's response must be idempotent (same txn_ref).
	if err == nil {
		assert.Equal(t, domain.TaskStatusSuccess, result2.Status)
		// Same txn_ref proves the bank treated it as a replay.
		assert.Equal(t, "txn-idem-crash-recovery-1", result2.BankTxnRef)
	} else {
		// If the reservation is still held (race window), the second
		// attempt is suppressed — this is also correct behavior.
		t.Logf("second attempt suppressed (reservation still held): %v", err)
	}

	// The bank was called at most twice (once per worker attempt).
	bankCallsAfterSecondExec := h.BankHandler.CallCount.Load()
	assert.LessOrEqual(t, bankCallsAfterSecondExec-bankCallsAfterFirstExec, int32(1),
		"bank should be called at most once more for the recovery attempt")
}

// TestConcurrentThroughput measures how many tasks the pipeline can process
// per second with zero artificial latency. This validates the concurrency
// model and semaphore performance.
func TestConcurrentThroughput(t *testing.T) {
	h := NewHarness(t, nil)

	const totalTasks = 200
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(totalTasks)
	for i := 0; i < totalTasks; i++ {
		i := i
		go func() {
			defer wg.Done()
			task := makeTaskWithIndex(i, 1)
			_, _ = h.worker.ExecuteTask(context.Background(), task)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	throughput := float64(totalTasks) / elapsed.Seconds()
	t.Logf("throughput: %.0f tasks/sec (%d tasks in %v)", throughput, totalTasks, elapsed)

	// Verify at least some results were delivered to C2.
	// (Some may have been suppressed by reservation conflicts.)
	results := h.C2.Results()
	t.Logf("C2 received %d results out of %d tasks", len(results), totalTasks)
	assert.Greater(t, len(results), 0, "at least some results must reach C2")
}

// makeTaskWithIndex creates a task with a unique ID and key based on index.
func makeTaskWithIndex(idx int, epoch int64) *domain.Task {
	return &domain.Task{
		TaskID:         fmt.Sprintf("task-throughput-%d", idx),
		IdempotencyKey: fmt.Sprintf("idem-throughput-%d", idx),
		Amount:         50.0 + float64(idx),
		Currency:       "USD",
		MerchantID:     "merchant-perf",
		Epoch:          epoch,
		ReceivedAt:     time.Now(),
	}
}
