package observability

import (
	"context"
	"errors"

	"go.uber.org/zap"

	apperrors "github.com/your-org/payflow/worker/internal/errors"
)

// Logger wraps zap.Logger to provide context-aware structured logging.
// It automatically extracts 'task_id' from the context if present.
// For error-level logs it also injects an 'error_type' field extracted
// via errors.As so log-based alerting can target specific error classes
// without parsing message strings.
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
// It also injects an 'error_type' field for structured alerting:
//   - If the error (or any wrapped error) is a *TaskError, stage and task_id
//     are promoted to top-level fields.
//   - The 'error_type' field names the innermost sentinel sentinel label.
func (l *Logger) Error(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("task_id", GetTaskID(ctx)))
	fields = append(fields, errorTypeField(fields))
	l.zap.Error(msg, fields...)
}

// WithRaw returns the underlying zap logger for low-level or non-contextual calls.
func (l *Logger) WithRaw() *zap.Logger {
	return l.zap
}

// errorTypeField inspects the zap.Field slice for a field whose key is "error"
// (produced by zap.Error(err)) and extracts a human-readable "error_type" string
// from the error chain via errors.As / errors.Is.
func errorTypeField(fields []zap.Field) zap.Field {
	var err error
	for _, f := range fields {
		if f.Key == "error" && f.Interface != nil {
			if e, ok := f.Interface.(error); ok {
				err = e
				break
			}
		}
	}
	if err == nil {
		return zap.Skip()
	}

	// Prefer structured TaskError — it carries stage context.
	var te *apperrors.TaskError
	if errors.As(err, &te) {
		return zap.String("error_type", "task_error:"+te.Stage)
	}

	// Fall back to sentinel matching in order of specificity.
	switch {
	case errors.Is(err, apperrors.ErrEpochStale):
		return zap.String("error_type", "epoch_stale")
	case errors.Is(err, apperrors.ErrIdempotentKey):
		return zap.String("error_type", "idempotent_key")
	case errors.Is(err, apperrors.ErrSemaphoreFull):
		return zap.String("error_type", "semaphore_full")
	case errors.Is(err, apperrors.ErrCircuitOpen):
		return zap.String("error_type", "circuit_open")
	case errors.Is(err, apperrors.ErrContractVersion):
		return zap.String("error_type", "contract_version")
	default:
		return zap.String("error_type", "unknown")
	}
}
