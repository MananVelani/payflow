package concurrency_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/concurrency"
)

func TestSemaphore_AcquireRelease(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sem := concurrency.NewTaskSemaphore(2, nil, 60*time.Second, nil, nil, logger)
	ctx := context.Background()


	_ = sem.Acquire(ctx)
	_ = sem.Acquire(ctx)

	// Third acquire should block; we test via context timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	if err := sem.Acquire(timeoutCtx); err == nil {
		t.Fatal("expected timeout error")
	}

	sem.Release()
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("should have been able to acquire after release: %v", err)
	}
}

func TestTaskRegistry_Revocation(t *testing.T) {
	reg := concurrency.NewTaskRegistry()
	taskID := "t1"
	ctx, cancel := context.WithCancel(context.Background())

	reg.Register(taskID, cancel)

	if !reg.Revoke(taskID) {
		t.Fatal("revoke should succeed for active task")
	}

	select {
	case <-ctx.Done():
		// Success: context was cancelled by registry
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context NOT cancelled after revoke")
	}

	if reg.Revoke(taskID) {
		t.Fatal("revoke should fail for already deregistered task")
	}
}

func TestTaskRegistry_Deregister(t *testing.T) {
	reg := concurrency.NewTaskRegistry()
	taskID := "t1"
	_, cancel := context.WithCancel(context.Background())
	defer cancel() // clean up

	reg.Register(taskID, cancel)
	reg.Deregister(taskID)

	if reg.Revoke(taskID) {
		t.Fatal("should NOT be able to revoke after deregistration")
	}
}
