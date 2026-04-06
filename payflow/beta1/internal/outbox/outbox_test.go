package outbox_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/your-org/payflow/worker/internal/outbox"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

func TestOutbox_CrashResilience(t *testing.T) {
	// Setup
	store := outbox.NewMemoryStore()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	
	// Track calls to report
	var callCount int
	var shouldFail bool
	
	mockReport := func(ctx context.Context, result *pb.TaskResult) (*pb.ResultAck, error) {
		callCount++
		if shouldFail {
			return nil, errors.New("C2 unavailable")
		}
		return &pb.ResultAck{Acknowledged: true}, nil
	}

	o := outbox.New(mockReport, store, 10*time.Millisecond, 1*time.Millisecond, 100, 0, nil, logger)
	ctx := context.Background()

	// 1. Append 3 entries
	o.Enqueue(&pb.TaskResult{TaskId: "task-1"})
	o.Enqueue(&pb.TaskResult{TaskId: "task-2"})
	o.Enqueue(&pb.TaskResult{TaskId: "task-3"})

	// 2. Simulate flush failure
	shouldFail = true
	o.Start(ctx)
	
	// Wait for a few flush attempts
	time.Sleep(50 * time.Millisecond)
	
	// Assert all 3 entries are still Pending
	pending, err := store.Pending(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(pending), "Entries should remain in store on failure")

	// 3. Simulate successful flush
	shouldFail = false
	
	// Wait for success
	assert.Eventually(t, func() bool {
		pending, _ := store.Pending(ctx)
		return len(pending) == 0
	}, 1*time.Second, 100*time.Millisecond, "Outbox should eventually drain on success")

	assert.GreaterOrEqual(t, callCount, 3, "Should have attempted to send at least 3 messages")
}
