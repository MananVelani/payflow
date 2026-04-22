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
	"github.com/your-org/payflow/worker/internal/loadreport"
	"github.com/your-org/payflow/worker/internal/observability"
	"github.com/your-org/payflow/worker/internal/outbox"
	"github.com/your-org/payflow/worker/internal/resilience"
	"github.com/your-org/payflow/worker/internal/reservation"
	"github.com/your-org/payflow/worker/internal/tracing"
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
	inFlightIdempotency sync.Map // key: task_id (string), value: idempotency_key (string)

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

	circuitBreaker *resilience.BankCircuitBreaker

	// --- Checkpoint 7: Task-level retry tracker ---
	retryTracker *RetryTracker

	// --- Week 4: Load reporter ---
	loadReporter *loadreport.Reporter
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
	loadReporter *loadreport.Reporter,   // WEEK 4 ADDITION

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

		// We pass the raw zap logger to the external dependency.
		circuitBreaker: resilience.NewBankCircuitBreaker(
			cfg.CBMaxRequests,
			cfg.CBTimeout,
			cfg.CBFailureThreshold,
			logger.WithRaw(),
		),
		retryTracker: NewRetryTracker(cfg.RetryTaskMaxAttempts),
		loadReporter: loadReporter,
	}
}

// ExecuteTask runs the exactly-once pipeline. Called once per task received from C2.
func (w *WorkerServiceImpl) ExecuteTask(ctx context.Context, task *domain.Task) (*domain.PaymentResult, error) {
	taskStart := time.Now()
	// --- WEEK 2 ADDITION: Graceful shutdown tracking ---
	w.wg.Add(1)
	defer w.wg.Done()

	// Track task completion for load reporting
	defer func() {
		elapsed := time.Since(taskStart)
		w.processed.Add(1)
		w.totalDuration.Add(elapsed.Milliseconds())
		if w.loadReporter != nil {
			w.loadReporter.RecordTaskDuration(elapsed)
		}
	}()

	// --- Checkpoint 5: Early deadline derivation ---
	taskCtx := ctx
	if task.DeadlineUnixMs > 0 {
		deadline := time.UnixMilli(task.DeadlineUnixMs)
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	// --- Checkpoint 7: Record retry attempt ---
	attempt := w.retryTracker.RecordAttempt(task.IdempotencyKey)
	w.metrics.RecordTaskRetry(attempt)

	// 1. Acquire semaphore slot (blocks if full)
	if err := w.sem.Acquire(taskCtx); err != nil {
		if taskCtx.Err() != nil && errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
			w.metrics.RecordDeadlineExceeded("semaphore_wait")
			return nil, taskCtx.Err()
		}
		if errors.Is(err, apperrors.ErrSemaphoreFull) {
			w.logger.Warn(ctx, "semaphore at capacity", zap.Error(err))
			return nil, fmt.Errorf("execute task %s: %w", task.TaskID, err)
		}
		// context cancelled — likely graceful shutdown
		return nil, err
	}
	defer w.sem.Release()

	// Track in-flight idempotency for revocation
	w.inFlightIdempotency.Store(task.TaskID, task.IdempotencyKey)
	defer w.inFlightIdempotency.Delete(task.TaskID)


	// 2. Wrap context with task ID for tracing
	taskCtx = observability.WithTaskID(taskCtx, task.TaskID)
	// We no longer need another WithCancel here as taskCtx already has deadline/cancel

	// 3. Register for hard revocation
	w.taskRegistry.Register(task.TaskID, func() { /* revocation cancel handled via taskCtx if possible, but registry needs a cancel func */ })
	// Wait, the taskRegistry expects a cancel function.
	// I should probably keep the WithCancel for revocation specifically if I want atomic cancel.
	// But the prompt says "derive a deadline-bounded context AT THIS POINT".

	// Let's refine the context wrapping.
	revocationCtx, revocationCancel := context.WithCancel(taskCtx)
	defer revocationCancel()
	w.taskRegistry.Register(task.TaskID, revocationCancel)
	defer w.taskRegistry.Deregister(task.TaskID)
	
	ctx = revocationCtx // use revocationCtx (which is taskCtx + revocation)

	// --- Week 4: Trace propagation ---
	ctx = tracing.ExtractFromGRPCMetadata(ctx)
	ctx, span := tracing.StartSpan(ctx, "worker.execute_task")
	defer span.End()

	// 4. Fencing token validation
	if err := w.epochValidator.ValidateAndUpdate(ctx, task.Epoch); err != nil {
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

	// Reservation handles IK-1 before we proceed further.
	w.epoch = task.Epoch
	// --- END CHECKPOINT 5 ---



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

		if !w.retryTracker.ShouldRetry(task.IdempotencyKey) {
			w.logger.Warn(taskCtx, "task max retries exceeded, marking as permanent failure")
			w.retryTracker.Clear(task.IdempotencyKey)
		}
	} else {
		w.logger.Info(taskCtx, "bank call succeeded", zap.String("txn_ref", txnRef))
		result.Status = domain.TaskStatusSuccess
		result.BankTxnRef = txnRef
		paymentSucceeded = true // WEEK 2: mark for reservation completion
		w.retryTracker.Clear(task.IdempotencyKey)
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
		w.logger.Info(ctx, "suppressing result for revoked task", zap.String("task_id", taskID))
		w.metrics.RecordRevokedTaskSuppressed()
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
			TaskId:         result.TaskID,
			WorkerId:       result.WorkerID,
			Epoch:          w.epoch,
			IdempotencyKey: result.IdempotencyKey,
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
	w.metrics.RecordTaskRevoked()

	// Cancel active task via registry
	found := w.taskRegistry.Revoke(taskID)

	if found {
		w.metrics.RecordRevokeOutcome("cancelled")
		w.logger.Info(ctx, "task revoked — context cancelled", zap.String("task_id", taskID))
	} else {
		w.metrics.RecordRevokeOutcome("already_completed")
		w.logger.Info(ctx, "revoke received for unknown task — already completed", zap.String("task_id", taskID))
	}

	// Release reservation if possible
	if ik, ok := w.inFlightIdempotency.Load(taskID); ok {
		w.logger.Info(ctx, "releasing reservation for revoked task", zap.String("task_id", taskID))
		_ = w.reservationStore.Release(ctx, ik.(string))
	}

	w.logger.Warn(ctx, "task revocation sequence completed", zap.String("task_id", taskID))
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


// --- Testing Helpers ---

func (w *WorkerServiceImpl) AcquireSemaphore(ctx context.Context) error {
	return w.sem.Acquire(ctx)
}

func (w *WorkerServiceImpl) ReleaseSemaphore() {
	w.sem.Release()
}

func (w *WorkerServiceImpl) ResetBankBreaker() {
	w.logger.Warn(context.Background(), "manual reset of bank circuit breaker triggered")
	w.circuitBreaker.Reset()
	w.bankClient.ResetBreaker()
}

func (w *WorkerServiceImpl) SetBackpressureMode(mode concurrency.BackpressureMode) {
	w.logger.Warn(context.Background(), "backpressure mode changed", zap.Int32("mode", int32(mode)))
	w.sem.SetMode(mode)
}
