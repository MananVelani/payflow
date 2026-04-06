package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/your-org/payflow/worker/internal/config"
	"github.com/your-org/payflow/worker/internal/domain"
)

func TestLogicalRetry_SuccessAfterTwoFailures(t *testing.T) {
	h := NewHarness(t, nil)

	// Max task retries is 3 in harness config.
	// We'll make the bank fail 2 times. The 3rd attempt (logical retry) should succeed.
	// Wait, the MockBankHandler.failCount is for total HTTP calls.
	// A single ExecuteTask call with 5 inner retries will consume 2 failures and succeed.
	// To test LOGICAL retries (from C2), we need to fail all 5 inner retries.
	
	h.BankHandler.SetFailCount(12) // attempt 1 (5 fails), attempt 2 (5 fails), attempt 3 (succeeds)
	// Wait, attempt 3 first call will be call #11.
	
	task := makeTask("task-logical-retry-1", "idem-logical-1", 1)
	
	// First attempt
	result, err := h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailure, result.Status)
	
	// Second attempt (simulated re-dispatch from C2)
	result, err = h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusFailure, result.Status)
	
	// Third attempt (should succeed)
	result, err = h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusSuccess, result.Status)
}

func TestDeadlineExceeded_SemaphoreWait(t *testing.T) {
	h := NewHarness(t, &config.Config{MaxConcurrentTasks: 1})

	// Occupation task: blocks the semaphore slot manually
	require.NoError(t, h.worker.AcquireSemaphore(context.Background()))
	defer h.worker.ReleaseSemaphore()

	// Second task: should time out waiting for the semaphore
	task := makeTask("task-timeout-1", "idem-timeout-1", 1)
	task.DeadlineUnixMs = time.Now().Add(100 * time.Millisecond).UnixMilli()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := h.worker.ExecuteTask(ctx, task)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestTaskRevocation_Suppression(t *testing.T) {
	h := NewHarness(t, nil)
	
	task := makeTask("task-revoked-1", "idem-revoked-1", 1)
	
	// We mark it revoked BEFORE execution finishes.
	// Since safeReportResult checks the map, it should suppress the result.
	
	h.C2.SetDomainFailCount(0) // don't fail report
	
	// We can't easily wait "mid-execution" without mocks.
	// But we can call RevokeTask and then ExecuteTask.
	
	err := h.worker.RevokeTask(context.Background(), task.TaskID)
	require.NoError(t, err)
	
	result, err := h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusSuccess, result.Status) // ExecuteTask itself returns success
	
	// But C2 must NOT have received it.
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, h.C2.Results(), "Result should have been suppressed")
}
