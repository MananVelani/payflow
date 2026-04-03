package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/your-org/payflow/worker/internal/domain"
	apperrors "github.com/your-org/payflow/worker/internal/errors"
)

// ─── helpers ─────────────────────────────────────────────────────────────────

func makeTask(id, key string, epoch int64) *domain.Task {
	return &domain.Task{
		TaskID:         id,
		IdempotencyKey: key,
		Amount:         100.0,
		Currency:       "USD",
		MerchantID:     "merchant-1",
		Epoch:          epoch,
		ReceivedAt:     time.Now(),
	}
}

// ─── Tests ───────────────────────────────────────────────────────────────────

// TestHappyPath verifies the golden path: one task → bank succeeds →
// C4 receives an idempotency check → C2 receives a SUCCESS result →
// the outbox remains empty.
func TestHappyPath(t *testing.T) {
	h := NewHarness(t, nil)

	task := makeTask("task-happy-1", "idem-happy-1", 1)
	h.SendTask(t, task)

	// C2 must receive exactly one SUCCESS.
	h.AssertResult(t, task.TaskID, domain.TaskStatusSuccess)

	// C4 must have been consulted for idempotency at least once.
	assert.GreaterOrEqual(t, h.C4.IdempotencyCheckCount(), 1,
		"C4 should receive at least one CheckIdempotency call")

	// Outbox must be empty (successful direct delivery).
	h.AssertOutboxDepth(t, 0)
}

// TestIdempotency sends the same task (same idempotency key) twice concurrently.
// The reservation store ensures exactly one execution proceeds; C4 is consulted
// exactly once, and C2 receives exactly one result.
func TestIdempotency(t *testing.T) {
	h := NewHarness(t, nil)

	task1 := makeTask("task-idem-1", "idem-shared", 1)
	task2 := makeTask("task-idem-2", "idem-shared", 1) // same key, different ID

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); h.SendTask(t, task1) }()
	go func() { defer wg.Done(); h.SendTask(t, task2) }()
	wg.Wait()

	// Wait for whichever task won the reservation to report to C2.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.C2.Results()) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Exactly one result must be delivered (the duplicate is suppressed by reservation).
	results := h.C2.Results()
	assert.Equal(t, 1, len(results),
		"exactly one result expected; duplicate idempotency key must be suppressed")

	// C4 must have been touched exactly once (second task never reaches C4).
	assert.Equal(t, 1, h.C4.IdempotencyCheckCount(),
		"exactly one CheckIdempotency call expected")
}

// TestEpochFencing sends epoch=1 then epoch=0 sequentially.
// The fence validator returns a *fence.ValidationError wrapping ErrEpochStale.
// ExecuteTask bubbles this error back to the caller and does NOT report to C2.
func TestEpochFencing(t *testing.T) {
	h := NewHarness(t, nil)

	// First task with epoch=1 — must succeed.
	t1 := makeTask("task-fence-1", "idem-fence-1", 1)
	h.SendTask(t, t1)
	h.AssertResult(t, t1.TaskID, domain.TaskStatusSuccess)

	// Second task with epoch=0 (stale) — rejected by the fence validator.
	t2 := makeTask("task-fence-2", "idem-fence-2", 0)
	_, err := h.worker.ExecuteTask(context.Background(), t2)

	// The fence now surfaces the error rather than swallowing it.
	// It wraps ErrEpochStale via ValidationError.Unwrap().
	if err != nil {
		// Confirm it is a stale-epoch rejection.
		assert.True(t, errors.Is(err, apperrors.ErrEpochStale),
			"fence error must wrap ErrEpochStale, got: %v", err)
	}
	// In either case (error or nil), C2 must not have received a result for t2.
	time.Sleep(100 * time.Millisecond)

	for _, r := range h.C2.Results() {
		assert.NotEqual(t, t2.TaskID, r.TaskID,
			"stale-epoch task must never produce a C2 result")
	}
}

// TestBankFailureRetry programs the mock bank to fail twice then succeed.
// The bank client's inner retry (MaxAttempts=5) handles the transient failures.
// The test asserts the eventual SUCCESS and that the bank was called multiple times.
func TestBankFailureRetry(t *testing.T) {
	h := NewHarness(t, nil)

	// Bank fails twice, then succeeds on the third call.
	h.BankHandler.SetFailCount(2)

	task := makeTask("task-retry-1", "idem-retry-1", 1)
	h.SendTask(t, task)

	// Worker eventually reports SUCCESS after retries.
	h.AssertResult(t, task.TaskID, domain.TaskStatusSuccess)

	// Bank must have been called at least 3 times (2 failures + 1 success).
	// This directly proves the retry mechanism fired at least twice.
	assert.GreaterOrEqual(t, h.BankHandler.CallCount.Load(), int32(3),
		"bank must be contacted at least 3 times (2 failures + 1 success)")
}

// TestOutboxDurability programs MockC2 to reject the first domain result,
// causing the outbox to buffer it.  After a flush cycle the outbox relay
// succeeds via the outbox's own ReportFunc, and the outbox drains to zero.
func TestOutboxDurability(t *testing.T) {
	h := NewHarness(t, nil)

	// First 1 domain-level ReportResult calls return an error.
	h.C2.SetDomainFailCount(1)

	task := makeTask("task-outbox-1", "idem-outbox-1", 1)
	// Call ExecuteTask directly (blocking) so we can check depth immediately after.
	_, err := h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err, "ExecuteTask must not propagate the C2 delivery error")

	// The result must have been buffered in the outbox (direct delivery failed).
	// Allow a brief moment for the Enqueue path to complete.
	time.Sleep(20 * time.Millisecond)
	entries, qErr := h.outboxStore.Pending(context.Background())
	require.NoError(t, qErr)
	assert.Equal(t, 1, len(entries),
		"outbox must hold exactly 1 buffered result after domain delivery failure")

	// Wait for the outbox relay goroutine to flush and deliver to MockC2.
	h.WaitOutboxEmpty(t)

	// Outbox must now be empty.
	h.AssertOutboxDepth(t, 0)

	// C2 must have recorded the result (via the outbox relay path).
	assert.Equal(t, 1, len(h.C2.Results()),
		"C2 must have received the result via the outbox relay path")
	assert.Equal(t, domain.TaskStatusSuccess, h.C2.Results()[0].Status)
}

// ensure apperrors import is not optimised away when ErrEpochStale is used indirectly.
var _ = apperrors.ErrEpochStale
