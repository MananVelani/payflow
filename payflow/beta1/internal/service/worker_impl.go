package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/your-org/payflow/worker/internal/config"
	"github.com/your-org/payflow/worker/internal/concurrency"
	"github.com/your-org/payflow/worker/internal/domain"
	apperrors "github.com/your-org/payflow/worker/internal/errors"
	"github.com/your-org/payflow/worker/internal/fence"
	"github.com/your-org/payflow/worker/internal/observability"
	"github.com/your-org/payflow/worker/internal/outbox"
	"github.com/your-org/payflow/worker/internal/resilience"
	"github.com/your-org/payflow/worker/internal/reservation"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

// WorkerServiceImpl implements the WorkerService interface.
// It owns the exactly-once execution pipeline.
type WorkerServiceImpl struct {
	bankClient    BankClient
	logClient     LogServiceClient
	reportResult  ReportResultFunc
	// --- WEEK 2 ADDITION: Upgrade to contextual logger ---
	logger        *observability.Logger
	cfg           *config.Config
	metrics       *observability.Metrics // WEEK 2 ADDITION

	// Concurrency-safe revoke map
	revokedTasks  sync.Map // key: task_id (string), value: true

	// Stats for heartbeat — all atomic for lock-free reads
	activeTasks   atomic.Int64
	processed     atomic.Int64
	totalDuration atomic.Int64 // sum of all task durations in ms

	// --- WEEK 2 ADDITION: Fencing token validator ---
	epochValidator *fence.EpochValidator

	// --- WEEK 2 ADDITION: Pre-flight reservation store ---
	reservationStore reservation.Store


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
	logger *observability.Logger, // WEEK 2: upgraded
	cfg *config.Config,
	reservationStore reservation.Store, // WEEK 2 ADDITION
	outbox *outbox.Outbox,              // WEEK 2 ADDITION
	sem *concurrency.TaskSemaphore,    // WEEK 2 ADDITION
	registry *concurrency.TaskRegistry, // WEEK 2 ADDITION
	wg *sync.WaitGroup,                 // WEEK 2 ADDITION
	metrics *observability.Metrics,      // WEEK 2 ADDITION

) *WorkerServiceImpl {

	return &WorkerServiceImpl{
		bankClient:   bankClient,
		logClient:    logClient,
		reportResult: reportFn,
		logger:       logger,
		cfg:          cfg,
		metrics:      metrics, // WEEK 2 ADDITION


		// --- WEEK 2 ADDITION: Initialize fencing validator ---
		epochValidator: fence.NewEpochValidator(),

		// --- WEEK 2 ADDITION: Initialize dependencies ---
		reservationStore: reservationStore,
		outbox:         outbox,
		sem:            sem,
		taskRegistry:   registry,
		wg:             wg,

		// --- WEEK 2 ADDITION: Initialize circuit breaker ---
		// We pass the raw zap logger to the external dependency.
		circuitBreaker: resilience.NewBankCircuitBreaker(
			cfg.CBMaxRequests,
			cfg.CBTimeout,
			cfg.CBFailureThreshold,
			logger.WithRaw(),
		),
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
		if errors.Is(err, apperrors.ErrSemaphoreFull) {
			w.logger.Warn(ctx, "semaphore at capacity", zap.Error(err))
			return nil, fmt.Errorf("execute task %s: %w", task.TaskID, err)
		}
		// context cancelled — likely graceful shutdown
		return nil, err
	}
	defer w.sem.Release()

	// 2. Create cancellable context for THIS task's lifecycle
	// --- WEEK 2 ADDITION: Inject Task ID for tracing ---
	taskCtx, cancel := context.WithCancel(observability.WithTaskID(ctx, task.TaskID))
	defer cancel()

	// 3. Register for hard revocation
	w.taskRegistry.Register(task.TaskID, cancel)
	defer w.taskRegistry.Deregister(task.TaskID)
	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Fencing token validation ---
	// Placed FIRST: cheaper than a C4 network call; rejects zombie tasks immediately.
	if err := w.epochValidator.ValidateAndUpdate(task.Epoch); err != nil {
		var ve *fence.ValidationError
		if errors.As(err, &ve) {
			w.logger.Warn(taskCtx, "fencing: rejected stale task",
				zap.Int64("incoming_epoch", ve.IncomingEpoch),
				zap.Int64("last_seen", ve.LastSeen),
			)
		} else {
			w.logger.Warn(taskCtx, "fencing: rejected stale task", zap.Error(err))
		}
		return nil, &apperrors.TaskError{TaskID: task.TaskID, Stage: "epoch_check", Err: err}
	}
	// Use the task-specific context from here on
	ctx = taskCtx
	w.epoch = task.Epoch // WEEK 2: track current epoch
	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Distributed pre-flight reservation ---
	// Default TTL: 5 minutes
	ok, err := w.reservationStore.Reserve(ctx, task.IdempotencyKey, 5*time.Minute)
	if err != nil {
		w.logger.Error(ctx, "reservation: failure in store",
			zap.Error(fmt.Errorf("%w", &apperrors.TaskError{TaskID: task.TaskID, Stage: "reservation", Err: err})),
		)
		return nil, &apperrors.TaskError{TaskID: task.TaskID, Stage: "reservation", Err: err}
	}
	if !ok {
		w.logger.Warn(ctx, "reservation: concurrent duplicate suppressed",
			zap.String("idempotency_key", task.IdempotencyKey),
		)
		return nil, &apperrors.TaskError{
			TaskID: task.TaskID,
			Stage:  "reservation",
			Err:    fmt.Errorf("key %s: %w", task.IdempotencyKey, apperrors.ErrIdempotentKey),
		}
	}

	// ── Deadline: wire task deadline into context ──────────────────────────
	// Done AFTER reservation so the idempotency key is already held before
	// we spend any of the task's time budget on actual work.
	if task.DeadlineUnixMs > 0 {
		deadline := time.UnixMilli(task.DeadlineUnixMs)
		var deadlineCancel context.CancelFunc
		ctx, deadlineCancel = context.WithDeadline(ctx, deadline)
		defer deadlineCancel()
	}



	paymentSucceeded := false // SECTION 8: track for defer

	// On any exit path (success, failure, revocation), clean up the reservation.
	defer func() {
		if !paymentSucceeded {
			_ = w.reservationStore.Release(context.Background(), task.IdempotencyKey)
		}
		// On success, we let it expire naturally or be cleaned up or persisted in C4.
		// In SET-NX pattern, releasing on success is optional but helps with quick retries if C4 failed.
		// However, the requested implementation of Release is DEL.
	}()

	// --- END WEEK 2 ADDITION ---

	// ── STEP 2: Check C4 Idempotency ─────────────────────────────────────
	exists, cachedResult, err := w.logClient.CheckIdempotency(ctx, task.IdempotencyKey)
	if err != nil {
		if isDeadlineExceeded(err) {
			w.metrics.RecordDeadlineExceeded("c4_log")
			w.logger.Error(ctx, "C4 CheckIdempotency deadline exceeded", zap.Error(err))
			return nil, err
		}
		w.logger.Warn(taskCtx, "C4 CheckIdempotency failed — will proceed to bank with caution", zap.Error(err))
	}

	if exists && cachedResult != nil {
		w.logger.Info(taskCtx, "idempotency hit — returning cached result, skipping bank")
		
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
		return nil, resilience.ExecuteWithRetry(
			ctx,
			bankOp,
			w.cfg.RetryMaxAttempts,
			w.cfg.RetryBaseDelay,
			w.cfg.RetryMaxDelay,
		)
	})
	if err != nil && isDeadlineExceeded(err) {
		w.metrics.RecordDeadlineExceeded("bank")
		w.logger.Error(ctx, "bank call deadline exceeded", zap.Error(err))
	}
	// --- END WEEK 2 ADDITION ---

	
	result := &domain.PaymentResult{
		TaskID:         task.TaskID,
		WorkerID:       w.cfg.WorkerID,
		IdempotencyKey: task.IdempotencyKey,
		CompletedAt:    time.Now(),
	}

	if err != nil {
		var taskErr error
		if errors.Is(err, apperrors.ErrCircuitOpen) {
			w.logger.Error(taskCtx, "bank circuit breaker open",
				zap.Error(fmt.Errorf("%w", &apperrors.TaskError{TaskID: task.TaskID, Stage: "bank", Err: err})),
			)
			taskErr = &apperrors.TaskError{TaskID: task.TaskID, Stage: "bank", Err: err}
		} else {
			w.logger.Error(taskCtx, "bank call failed after retries",
				zap.Error(fmt.Errorf("%w", &apperrors.TaskError{TaskID: task.TaskID, Stage: "bank", Err: err})),
			)
			taskErr = &apperrors.TaskError{TaskID: task.TaskID, Stage: "bank", Err: err}
		}
		_ = taskErr // used for structured logging above; result reflects failure
		result.Status = domain.TaskStatusFailure
	} else {
		w.logger.Info(taskCtx, "bank call succeeded", zap.String("txn_ref", txnRef))
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
		w.logger.Warn(ctx, "task revoked mid-execution — discarding result")
		return nil
	}

	// --- WEEK 2 ADDITION: Outbox-backed ReportResult ---
	// Try direct delivery first; fall back to outbox on failure.
	if err := w.reportResult(ctx, result); err != nil {
		if isDeadlineExceeded(err) {
			w.metrics.RecordDeadlineExceeded("outbox")
			w.logger.Error(ctx, "ReportResult deadline exceeded, buffering in outbox", zap.Error(err))
		} else {
			w.logger.Warn(ctx, "direct ReportResult failed, buffering in outbox", zap.Error(err))
		}
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

func (w *WorkerServiceImpl) RevokeTask(ctx context.Context, taskID string) error {
	w.revokedTasks.Store(taskID, true)
	w.logger.Warn(ctx, "task marked revoked")

	// --- WEEK 2 ADDITION: Hard cancellation ---
	if w.taskRegistry.Revoke(taskID) {
		w.logger.Info(ctx, "hard revocation: cancelled active task context")
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

// isDeadlineExceeded reports whether err wraps context.DeadlineExceeded.
// Used to distinguish timed-out calls from other transient failures.
func isDeadlineExceeded(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

