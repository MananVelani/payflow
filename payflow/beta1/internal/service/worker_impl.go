package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"github.com/your-org/payflow/worker/config"
	"github.com/your-org/payflow/worker/internal/domain"
	"github.com/your-org/payflow/worker/internal/metrics"
	"github.com/your-org/payflow/worker/internal/fence"
	"github.com/your-org/payflow/worker/internal/reservation"
	"github.com/your-org/payflow/worker/internal/outbox"
	"github.com/your-org/payflow/worker/internal/concurrency"
	"github.com/your-org/payflow/worker/internal/resilience"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

// WorkerServiceImpl implements the WorkerService interface.
// It owns the exactly-once execution pipeline.
type WorkerServiceImpl struct {
	bankClient    BankClient
	logClient     LogServiceClient
	reportResult  ReportResultFunc
	logger        *zap.Logger
	cfg           *config.Config

	// Concurrency-safe revoke map
	revokedTasks  sync.Map // key: task_id (string), value: true

	// Stats for heartbeat — all atomic for lock-free reads
	activeTasks   atomic.Int64
	processed     atomic.Int64
	totalDuration atomic.Int64 // sum of all task durations in ms

	// --- WEEK 2 ADDITION: Fencing token validator ---
	epochValidator *fence.EpochValidator

	// --- WEEK 2 ADDITION: Pre-flight reservation map ---
	reservationMap *reservation.Map

	// --- WEEK 2 ADDITION: In-memory outbox ---
	outbox *outbox.Outbox

	// --- WEEK 2 ADDITION: Current epoch tracker ---
	epoch int64

	// --- WEEK 2 ADDITION: Concurrency control ---
	sem          *concurrency.TaskSemaphore
	taskRegistry *concurrency.TaskRegistry

	// --- WEEK 2 ADDITION: Graceful shutdown tracking ---
	wg *sync.WaitGroup

	// --- WEEK 2 ADDITION: Resilience ---
	circuitBreaker *resilience.BankCircuitBreaker
}

// NewWorkerServiceImpl constructs and returns a ready WorkerServiceImpl.
func NewWorkerServiceImpl(
	bankClient BankClient,
	logClient LogServiceClient,
	reportFn ReportResultFunc,
	logger *zap.Logger,
	cfg *config.Config,
	reservationMap *reservation.Map, // WEEK 2 ADDITION
	outbox *outbox.Outbox,         // WEEK 2 ADDITION
	sem *concurrency.TaskSemaphore, // WEEK 2 ADDITION
	registry *concurrency.TaskRegistry, // WEEK 2 ADDITION
	wg *sync.WaitGroup,            // WEEK 2 ADDITION
) *WorkerServiceImpl {
	return &WorkerServiceImpl{
		bankClient:   bankClient,
		logClient:    logClient,
		reportResult: reportFn,
		logger:       logger,
		cfg:          cfg,

		// --- WEEK 2 ADDITION: Initialize fencing validator ---
		epochValidator: fence.NewEpochValidator(),

		// --- WEEK 2 ADDITION: Wire dependencies ---
		reservationMap: reservationMap,
		outbox:         outbox,
		sem:            sem,
		taskRegistry:   registry,
		wg:             wg,

		// --- WEEK 2 ADDITION: Initialize circuit breaker ---
		circuitBreaker: resilience.NewBankCircuitBreaker(logger),
	}
}

// ExecuteTask runs the exactly-once pipeline. Called once per task received from C2.
func (w *WorkerServiceImpl) ExecuteTask(ctx context.Context, task *domain.Task) (*domain.PaymentResult, error) {
	// --- WEEK 2 ADDITION: Graceful shutdown tracking ---
	w.wg.Add(1)
	defer w.wg.Done()
	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Concurrency & Revocation control ---
	// 1. Acquire semaphore slot (blocks if full)
	if err := w.sem.Acquire(ctx); err != nil {
		return nil, err // context cancelled (likely shutdown)
	}
	defer w.sem.Release()

	// 2. Create cancellable context for THIS task's lifecycle
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 3. Register for hard revocation
	w.taskRegistry.Register(task.TaskID, cancel)
	defer w.taskRegistry.Deregister(task.TaskID)
	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Fencing token validation ---
	// Placed FIRST: cheaper than a C4 network call; rejects zombie tasks immediately.
	if err := w.epochValidator.ValidateAndUpdate(task.Epoch); err != nil {
		w.logger.Warn("fencing: rejected stale task",
			zap.String("task_id", task.TaskID),
			zap.Error(err),
		)
		return nil, nil // do NOT report to C2; stale result would confuse coordinator
	}
	// Use the task-specific context from here on
	ctx = taskCtx
	w.epoch = task.Epoch // WEEK 2: track current epoch
	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Local pre-flight reservation ---
	if err := w.reservationMap.Reserve(task.IdempotencyKey); err != nil {
		// Another goroutine in THIS binary is processing this key right now.
		// Do not call C4 or the bank. Log and skip.
		w.logger.Warn("reservation: concurrent duplicate suppressed locally",
			zap.String("task_id", task.TaskID),
			zap.String("idempotency_key", task.IdempotencyKey),
		)
		return nil, nil
	}

	paymentSucceeded := false // SECTION 8: track for defer

	// On any exit path (success, failure, revocation), clean up the reservation.
	// We use a named function instead of anonymous defer to make the state explicit.
	defer func() {
		if paymentSucceeded {
			w.reservationMap.Complete(task.IdempotencyKey)
		} else {
			w.reservationMap.Release(task.IdempotencyKey)
		}
	}()
	// --- END WEEK 2 ADDITION ---

	start := time.Now()
	w.activeTasks.Add(1)
	metrics.ActiveTasks.Inc()
	
	defer func() {
		w.activeTasks.Add(-1)
		metrics.ActiveTasks.Dec()
		elapsed := time.Since(start).Milliseconds()
		w.totalDuration.Add(elapsed)
		w.processed.Add(1)
		metrics.BankRequestDuration.Observe(float64(elapsed))
	}()

	w.logger.Info("task received", zap.String("task_id", task.TaskID))

	// ── STEP 2: Check C4 Idempotency ─────────────────────────────────────
	exists, cachedResult, err := w.logClient.CheckIdempotency(ctx, task.IdempotencyKey)
	if err != nil {
		w.logger.Warn("C4 CheckIdempotency failed — will proceed to bank with caution",
			zap.String("task_id", task.TaskID), zap.Error(err))
	}

	if exists && cachedResult != nil {
		w.logger.Info("idempotency hit — returning cached result, skipping bank",
			zap.String("task_id", task.TaskID))
		metrics.TasksTotal.WithLabelValues("success").Inc()
		
		err := w.safeReportResult(ctx, task.TaskID, cachedResult)
		return cachedResult, err
	}

	// ── STEP 3: Call Mock Bank (Resilient Path) ───────────────────────────
	// --- WEEK 2 ADDITION: Resilience (Retry + Circuit Breaker) ---
	var txnRef string
	bankOp := func() error {
		var chargeErr error
		txnRef, chargeErr = w.bankClient.Charge(ctx, task.IdempotencyKey, task.Amount, task.Currency, task.MerchantID)
		return chargeErr
	}

	// 1. Wrap in circuit breaker
	_, err = w.circuitBreaker.Execute(func() (interface{}, error) {
		// 2. Wrap in custom full-jitter retry engine
		// Max 3 attempts, 100ms base delay
		return nil, resilience.ExecuteWithRetry(ctx, bankOp, 3, 100*time.Millisecond)
	})
	// --- END WEEK 2 ADDITION ---
	
	result := &domain.PaymentResult{
		TaskID:         task.TaskID,
		WorkerID:       w.cfg.WorkerID,
		IdempotencyKey: task.IdempotencyKey,
		CompletedAt:    time.Now(),
	}

	if err != nil {
		w.logger.Error("bank call failed after retries",
			zap.String("task_id", task.TaskID), zap.Error(err))
		metrics.TasksTotal.WithLabelValues("failure").Inc()
		result.Status = domain.TaskStatusFailure
	} else {
		w.logger.Info("bank call succeeded", zap.String("task_id", task.TaskID), zap.String("txn_ref", txnRef))
		metrics.TasksTotal.WithLabelValues("success").Inc()
		result.Status = domain.TaskStatusSuccess
		result.BankTxnRef = txnRef
		paymentSucceeded = true // WEEK 2: mark for reservation completion
	}

	// ── STEP 4: Report Result to C2 ───────────────────────────────────────
	err = w.safeReportResult(ctx, task.TaskID, result)
	return result, err
}

// safeReportResult calls ReportResult only if the task has not been revoked.
func (w *WorkerServiceImpl) safeReportResult(
	ctx context.Context,
	taskID string,
	result *domain.PaymentResult,
) error {
	if _, revoked := w.revokedTasks.Load(taskID); revoked {
		w.logger.Warn("task revoked mid-execution — discarding result",
			zap.String("task_id", taskID))
		metrics.TasksTotal.WithLabelValues("revoked").Inc()
		return nil
	}

	// --- WEEK 2 ADDITION: Outbox-backed ReportResult ---
	// Try direct delivery first; fall back to outbox on failure.
	if err := w.reportResult(ctx, result); err != nil {
		w.logger.Warn("direct ReportResult failed, buffering in outbox",
			zap.String("task_id", taskID),
			zap.Error(err),
		)
		// Convert domain result to proto for outbox storage
		pbResult := &pb.TaskResult{
			TaskId:   result.TaskID,
			WorkerId: result.WorkerID,
			Epoch:    w.epoch,
		}
		if result.Status == domain.TaskStatusSuccess {
			pbResult.Status = &pb.TaskResult_Success{Success: true}
		} else {
			pbResult.Status = &pb.TaskResult_ErrorMessage{ErrorMessage: "payment failed"}
		}
		w.outbox.Enqueue(pbResult) // background goroutine will retry
	}
	return nil
	// --- END WEEK 2 ADDITION ---
}

// RevokeTask marks a task as revoked. Called by the gRPC RevokeTask handler.
func (w *WorkerServiceImpl) RevokeTask(ctx context.Context, taskID string) error {
	w.revokedTasks.Store(taskID, true)
	metrics.RevokedTasksTotal.Inc()
	w.logger.Warn("task marked revoked", zap.String("task_id", taskID))

	// --- WEEK 2 ADDITION: Hard cancellation ---
	if w.taskRegistry.Revoke(taskID) {
		w.logger.Info("hard revocation: cancelled active task context", zap.String("task_id", taskID))
	}
	// --- END WEEK 2 ADDITION ---

	return nil
}

// Stats returns current load metrics for heartbeat calculation.
func (w *WorkerServiceImpl) Stats() domain.WorkerStats {
	active := w.activeTasks.Load()
	proc := w.processed.Load()
	totalDur := w.totalDuration.Load()
	maxCap := int64(w.cfg.MaxConcurrentTasks)

	var load float32
	if maxCap > 0 {
		load = float32(active) / float32(maxCap)
	}
	
	var avgDurationMs int64
	if proc > 0 {
		avgDurationMs = totalDur / proc
	}
	
	return domain.WorkerStats{
		WorkerID:            w.cfg.WorkerID,
		Load:                load,
		TasksProcessedCount: proc,
		AvgTaskDurationMS:   avgDurationMs,
		// Epoch will be filled by the heartbeat client from its tracker
	}
}
