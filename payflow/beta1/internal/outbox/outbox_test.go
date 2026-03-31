package outbox_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/outbox"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

func TestOutbox_Enqueue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	o := outbox.New(nil, logger)

	res := &pb.TaskResult{TaskId: "t1"}
	if !o.Enqueue(res) {
		t.Fatal("failed to enqueue")
	}

	if o.QueueDepth() != 1 {
		t.Fatalf("expected depth 1, got %d", o.QueueDepth())
	}
}

func TestOutbox_RelaySuccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	var called atomic.Int32
	
	reportFn := func(ctx context.Context, result *pb.TaskResult) (*pb.ResultAck, error) {
		called.Add(1)
		return &pb.ResultAck{Acknowledged: true}, nil
	}

	o := outbox.New(reportFn, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o.Enqueue(&pb.TaskResult{TaskId: "t1"})
	o.Start(ctx)

	// Wait for relay (ticker is 2s, but we can't easily speed it up without refactoring)
	// For testing, we might want to exported flush or use a shorter interval.
	// But let's just wait a bit.
	time.Sleep(3 * time.Second)

	if called.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", called.Load())
	}
	if o.QueueDepth() != 0 {
		t.Fatal("queue should be empty")
	}
}

func TestOutbox_RelayRetry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	var called atomic.Int32
	
	reportFn := func(ctx context.Context, result *pb.TaskResult) (*pb.ResultAck, error) {
		called.Add(1)
		return nil, errors.New("temporary failure")
	}

	o := outbox.New(reportFn, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	o.Enqueue(&pb.TaskResult{TaskId: "t1"})
	o.Start(ctx)

	time.Sleep(3 * time.Second)

	// Since it fails, it should stay in queue and retry
	if called.Load() < 1 {
		t.Fatal("should have attempted at least once")
	}
	if o.QueueDepth() != 1 {
		t.Fatal("queue should NOT be empty")
	}
}
