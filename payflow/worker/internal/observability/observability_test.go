package observability_test

import (
	"context"
	"testing"

	"github.com/your-org/payflow/worker/internal/observability"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestContextHelpers(t *testing.T) {
	taskID := "test-task-123"
	ctx := observability.WithTaskID(context.Background(), taskID)
	
	extracted := observability.GetTaskID(ctx)
	if extracted != taskID {
		t.Fatalf("expected %s, got %s", taskID, extracted)
	}

	if observability.GetTaskID(context.Background()) != "unknown" {
		t.Fatal("expected 'unknown' for empty context")
	}
}

func TestLogger_ContextInjection(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	zapLogger := zap.New(core)
	obsLogger := observability.NewLogger(zapLogger)
	
	taskID := "traced-task"
	ctx := observability.WithTaskID(context.Background(), taskID)
	
	obsLogger.Info(ctx, "test message")
	
	if logs.Len() != 1 {
		t.Fatal("expected 1 log entry")
	}
	
	entry := logs.All()[0]
	found := false
	for _, field := range entry.Context {
		if field.Key == "task_id" && field.String == taskID {
			found = true
			break
		}
	}
	
	if !found {
		t.Fatal("task_id not found in log context fields")
	}
}
