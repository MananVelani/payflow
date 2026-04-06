package concurrency

import (
	"context"
	"sync"
)

// TaskRegistry tracks the lifecycle and cancellation contexts of active tasks.
// Used to support 'Hard Revocation' — immediately cancelling the context of
// a running bank call when C2 sends a RevokeTask gRPC message.
type TaskRegistry struct {
	mu       sync.RWMutex
	active   map[string]context.CancelFunc
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		active: make(map[string]context.CancelFunc),
	}
}

// Register stores the cancellation function of a task.
// Call this before launching the ExecuteTask goroutine.
func (r *TaskRegistry) Register(taskID string, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active[taskID] = cancel
}

// Deregister removes a task. Call this in a defer after execution finishes.
func (r *TaskRegistry) Deregister(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.active, taskID)
}

// Revoke triggers the cancellation function of a running task if it exists.
// This is the C2-initiated remote revocation path.
func (r *TaskRegistry) Revoke(taskID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cancel, exists := r.active[taskID]
	if exists {
		cancel()
		delete(r.active, taskID)
		return true
	}
	return false
}
