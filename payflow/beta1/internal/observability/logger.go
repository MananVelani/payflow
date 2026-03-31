package observability

import (
	"context"

	"go.uber.org/zap"
)

// Logger wraps zap.Logger to provide context-aware structured logging.
// It automatically extracts 'task_id' from the context if present.
type Logger struct {
	zap *zap.Logger
}

func NewLogger(zap *zap.Logger) *Logger {
	return &Logger{zap: zap}
}

// Info logs an informational message with the task ID.
func (l *Logger) Info(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("task_id", GetTaskID(ctx)))
	l.zap.Info(msg, fields...)
}

// Warn logs a warning message with the task ID.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("task_id", GetTaskID(ctx)))
	l.zap.Warn(msg, fields...)
}

// Error logs an error message with the task ID.
func (l *Logger) Error(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("task_id", GetTaskID(ctx)))
	l.zap.Error(msg, fields...)
}

// WithRaw returns the underlying zap logger for low-level or non-contextual calls.
func (l *Logger) WithRaw() *zap.Logger {
	return l.zap
}
