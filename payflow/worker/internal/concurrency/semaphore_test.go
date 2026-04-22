package concurrency

import (
	"context"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/your-org/payflow/worker/internal/outbox"
)

func TestTaskSemaphore_OrphanedLeaseRecovery(t *testing.T) {
	path := "test_semaphore_badger"
	// Clean up any existing data
	_ = os.RemoveAll(path)
	defer os.RemoveAll(path)

	// Step 1: Pre-populate 3 orphaned leases
	store, err := outbox.NewBadgerStore(path)
	require.NoError(t, err)
	
	ctx := context.Background()
	require.NoError(t, store.SetLease(ctx, "task-1", 60*time.Second))
	require.NoError(t, store.SetLease(ctx, "task-2", 60*time.Second))
	require.NoError(t, store.SetLease(ctx, "task-3", 60*time.Second))
	store.Close()

	// Step 2: Re-open to simulate restart and initialize TaskSemaphore
	store, err = outbox.NewBadgerStore(path)
	require.NoError(t, err)
	defer store.Close()

	logger, _ := zap.NewDevelopment()
	// max=5, 3 orphaned leases
	sem := NewTaskSemaphore(5, store, 60*time.Second, nil, nil, logger)

	// Step 3: Assertions
	assert.Equal(t, 3, sem.OrphanedLeaseCount())
	
	// Capacitity is 5. 3 are occupied. We should be able to acquire 2 more.
	err = sem.Acquire(ctx)
	assert.NoError(t, err)
	err = sem.Acquire(ctx)
	assert.NoError(t, err)
	
	// The 6th acquire should block/timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	err = sem.Acquire(timeoutCtx)
	assert.Error(t, err, "expected timeout as semaphore should be full")
}

func TestTaskSemaphore_LeaseLifecycle(t *testing.T) {
	path := "test_lifecycle_badger"
	_ = os.RemoveAll(path)
	defer os.RemoveAll(path)

	store, err := outbox.NewBadgerStore(path)
	require.NoError(t, err)
	defer store.Close()

	logger, _ := zap.NewDevelopment()
	sem := NewTaskSemaphore(5, store, 1*time.Second, nil, nil, logger)

	ctx := context.Background()
	taskID := "task-123"

	// Lease
	err = sem.LeaseTask(taskID)
	assert.NoError(t, err)

	// Verify in store
	leases, err := store.ListLeases(ctx)
	assert.NoError(t, err)
	assert.Contains(t, leases, taskID)

	// Revoke
	err = sem.RevokeTask(taskID)
	assert.NoError(t, err)

	// Verify removed from store
	leases, err = store.ListLeases(ctx)
	assert.NoError(t, err)
	assert.NotContains(t, leases, taskID)
}
