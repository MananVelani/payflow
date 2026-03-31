package outbox

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	pb "github.com/your-org/payflow/worker/proto/worker"
)

const (
	maxQueueSize = 100           // hard cap; prevents unbounded memory on long C2 outage
	maxAttempts  = 10            // give up after this many relay attempts per message
	baseDelay    = 1 * time.Second
)

// ReportFunc is the function signature for calling C2's ReportResult.
// This matches the Week 1 gRPC call so the outbox is decoupled from the transport layer.
type ReportFunc func(ctx context.Context, result *pb.TaskResult) (*pb.ResultAck, error)

// item is an internal queue element.
type item struct {
	result   *pb.TaskResult
	attempts int
	addedAt  time.Time
}

// Outbox buffers ReportResult payloads and retries delivery to C2.
// It is safe for concurrent use. It does NOT persist to disk — on worker crash,
// buffered items are lost, but C4's idempotency check on the next worker prevents
// double-charging. The outbox only covers transient C2 connectivity issues.
type Outbox struct {
	mu      sync.Mutex
	queue   []*item
	report  ReportFunc
	logger  *slog.Logger
}

// New creates an Outbox. Call Start() to begin the relay goroutine.
func New(report ReportFunc, logger *slog.Logger) *Outbox {
	return &Outbox{
		queue:  make([]*item, 0, 16),
		report: report,
		logger: logger,
	}
}

// Enqueue adds a TaskResult to the outbox. Returns false if the queue is full,
// meaning the caller should log a critical alert (payment result may be lost).
func (o *Outbox) Enqueue(result *pb.TaskResult) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.queue) >= maxQueueSize {
		o.logger.Error("outbox: queue full — result may be lost, check C2 connectivity",
			"task_id", result.TaskId,
		)
		return false
	}
	o.queue = append(o.queue, &item{result: result, addedAt: time.Now()})
	o.logger.Warn("outbox: buffered result for retry",
		"task_id", result.TaskId,
		"queue_depth", len(o.queue),
	)
	return true
}

// Start launches the relay goroutine. It stops when ctx is cancelled.
// Call this once from main.go, passing the worker's root context.
func (o *Outbox) Start(ctx context.Context) {
	go o.relay(ctx)
}

// relay is the background worker that attempts to drain the queue.
// SECTION 11: replaced time.After with a persistent ticker to prevent leaks.
func (o *Outbox) relay(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			o.logger.Info("outbox: relay goroutine stopping", "remaining_items", o.size())
			return
		case <-ticker.C:
			o.flush(ctx)
		}
	}
}

func (o *Outbox) flush(ctx context.Context) {
	o.mu.Lock()
	if len(o.queue) == 0 {
		o.mu.Unlock()
		return
	}
	// Snapshot to process; keep the lock short
	snapshot := make([]*item, len(o.queue))
	copy(snapshot, o.queue)
	o.queue = o.queue[:0]
	o.mu.Unlock()

	var remaining []*item
	for _, it := range snapshot {
		if it.attempts >= maxAttempts {
			o.logger.Error("outbox: giving up on result after max attempts",
				"task_id", it.result.TaskId,
				"attempts", it.attempts,
			)
			continue // drop it; operator must reconcile via C4 audit log
		}

		// Exponential backoff: wait before retrying based on attempt count
		backoff := time.Duration(math.Pow(2, float64(it.attempts))) * baseDelay
		if time.Since(it.addedAt) < backoff {
			remaining = append(remaining, it) // not yet due for retry
			continue
		}

		if _, err := o.report(ctx, it.result); err != nil {
			o.logger.Warn("outbox: relay attempt failed",
				"task_id", it.result.TaskId,
				"attempt", it.attempts+1,
				"error", err,
			)
			it.attempts++
			remaining = append(remaining, it)
		} else {
			o.logger.Info("outbox: successfully relayed buffered result",
				"task_id", it.result.TaskId,
				"attempts_needed", it.attempts+1,
			)
		}
	}

	if len(remaining) > 0 {
		o.mu.Lock()
		o.queue = append(remaining, o.queue...) // prepend — older items first
		o.mu.Unlock()
	}
}

func (o *Outbox) size() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.queue)
}

// QueueDepth returns the current number of buffered items. Used for Prometheus gauge.
func (o *Outbox) QueueDepth() int {
	return o.size()
}
