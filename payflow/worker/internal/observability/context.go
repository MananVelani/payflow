package observability

import "context"

type contextKey string

const taskIDKey contextKey = "task_id"

// WithTaskID adds the task ID to the context.
func WithTaskID(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, taskIDKey, taskID)
}

// GetTaskID extracts the task ID from the context.
func GetTaskID(ctx context.Context) string {
	if val, ok := ctx.Value(taskIDKey).(string); ok {
		return val
	}
	return "unknown"
}
