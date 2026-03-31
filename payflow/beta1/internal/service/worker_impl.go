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
}

// NewWorkerServiceImpl constructs and returns a ready WorkerServiceImpl.
func NewWorkerServiceImpl(
	bankClient BankClient,
	logClient LogServiceClient,
	reportFn ReportResultFunc,
	logger *zap.Logger,
	cfg *config.Config,
) *WorkerServiceImpl {
	return &WorkerServiceImpl{
		bankClient:   bankClient,
		logClient:    logClient,
		reportResult: reportFn,
		logger:       logger,
		cfg:          cfg,
	}
}

// ExecuteTask runs the exactly-once pipeline. Called once per task received from C2.
func (w *WorkerServiceImpl) ExecuteTask(ctx context.Context, task *domain.Task) (*domain.PaymentResult, error) {
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

	// ── STEP 3: Call Mock Bank ────────────────────────────────────────────
	txnRef, err := w.bankClient.Charge(ctx, task.IdempotencyKey, task.Amount, task.Currency, task.MerchantID)
	
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
	return w.reportResult(ctx, result)
}

// RevokeTask marks a task as revoked. Called by the gRPC RevokeTask handler.
func (w *WorkerServiceImpl) RevokeTask(ctx context.Context, taskID string) error {
	w.revokedTasks.Store(taskID, true)
	metrics.RevokedTasksTotal.Inc()
	w.logger.Warn("task marked revoked", zap.String("task_id", taskID))
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
