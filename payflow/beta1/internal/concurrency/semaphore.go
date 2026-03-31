package concurrency

import (
	"context"
	"golang.org/x/sync/semaphore"
)

// TaskSemaphore wrap's x/sync/semaphore to provide simple backpressure.
// It ensures the worker never exceeds the MAX_CONCURRENT_TASKS configured by C2.
type TaskSemaphore struct {
	sem *semaphore.Weighted
}

func NewTaskSemaphore(max int64) *TaskSemaphore {
	if max <= 0 {
		max = 1 // fallback
	}
	return &TaskSemaphore{
		sem: semaphore.NewWeighted(max),
	}
}

// Acquire blocks until a task slot is available or context is cancelled.
func (s *TaskSemaphore) Acquire(ctx context.Context) error {
	return s.sem.Acquire(ctx, 1)
}

// Release frees a task slot.
func (s *TaskSemaphore) Release() {
	s.sem.Release(1)
}
