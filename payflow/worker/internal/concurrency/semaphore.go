package concurrency

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"

	apperrors "github.com/your-org/payflow/worker/internal/errors"
)

// LeaseStore defines the subset of persistence needed for task leases.
type LeaseStore interface {
	SetLease(ctx context.Context, taskID string, ttl time.Duration) error
	DeleteLease(ctx context.Context, taskID string) error
	ListLeases(ctx context.Context) ([]string, error)
}

// TaskSemaphore wrap's x/sync/semaphore to provide simple backpressure.
// It ensures the worker never exceeds the MAX_CONCURRENT_TASKS configured by C2.
type TaskSemaphore struct {
	sem                *semaphore.Weighted
	max                int64
	active             atomic.Int64
	saturation         prometheus.Gauge
	orphanedLeaseGauge prometheus.Gauge
	store              LeaseStore
	maxTaskDuration    time.Duration
	orphanedCount      int64
	logger             *slog.Logger
}

func NewTaskSemaphore(max int, store LeaseStore, maxTaskDuration time.Duration, saturation prometheus.Gauge, orphanedLeaseGauge prometheus.Gauge, logger *slog.Logger) *TaskSemaphore {
	if max <= 0 {
		max = 1 // fallback
	}
	
	s := &TaskSemaphore{
		sem:                semaphore.NewWeighted(int64(max)),
		max:                int64(max),
		saturation:         saturation,
		orphanedLeaseGauge: orphanedLeaseGauge,
		store:              store,
		maxTaskDuration:    maxTaskDuration,
		logger:             logger,
	}

	// Recovery of orphaned leases
	if store != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		leases, err := store.ListLeases(ctx)
		if err != nil {
			logger.Error("semaphore: failed to list leases for recovery", "error", err)
		} else {
			for _, taskID := range leases {
				if err := s.sem.Acquire(context.Background(), 1); err != nil {
					logger.Error("semaphore: failed to acquire slot for orphaned lease", "task_id", taskID, "error", err)
					continue
				}
				s.active.Add(1)
				s.orphanedCount++
				logger.Warn("recovering orphaned lease", "task_id", taskID)
			}
			
			if s.orphanedLeaseGauge != nil {
				s.orphanedLeaseGauge.Set(float64(s.orphanedCount))
			}
			
			if s.saturation != nil && s.max > 0 {
				s.saturation.Set(float64(s.active.Load()) / float64(s.max))
			}
		}
	}

	return s
}

// Acquire blocks until a task slot is available or context is cancelled.
// Returns a wrapped ErrSemaphoreFull if the semaphore itself is full
// (distinguishable from shutdown cancellation via errors.Is).
func (s *TaskSemaphore) Acquire(ctx context.Context) error {
	if err := s.sem.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("task slot unavailable: %w", apperrors.ErrSemaphoreFull)
	}
	active := s.active.Add(1)
	if s.saturation != nil {
		s.saturation.Set(float64(active) / float64(s.max))
	}
	return nil
}

// Release frees a task slot.
func (s *TaskSemaphore) Release() {
	s.sem.Release(1)
	active := s.active.Add(-1)
	if s.saturation != nil {
		s.saturation.Set(float64(active) / float64(s.max))
	}
}

// LeaseTask persists the task lease to the store.
func (s *TaskSemaphore) LeaseTask(taskID string) error {
	if s.store == nil {
		return nil
	}
	return s.store.SetLease(context.Background(), taskID, s.maxTaskDuration)
}

// RevokeTask deletes the task lease from the store.
func (s *TaskSemaphore) RevokeTask(taskID string) error {
	if s.store == nil {
		return nil
	}
	return s.store.DeleteLease(context.Background(), taskID)
}

// OrphanedLeaseCount returns the number of leases recovered at startup.
func (s *TaskSemaphore) OrphanedLeaseCount() int {
	return int(atomic.LoadInt64(&s.orphanedCount))
}

