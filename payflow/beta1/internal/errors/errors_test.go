package errors

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelUnwrapTwoLevels(t *testing.T) {
	sentinels := []struct {
		name     string
		sentinel error
	}{
		{"ErrEpochStale", ErrEpochStale},
		{"ErrIdempotentKey", ErrIdempotentKey},
		{"ErrSemaphoreFull", ErrSemaphoreFull},
		{"ErrCircuitOpen", ErrCircuitOpen},
		{"ErrContractVersion", ErrContractVersion},
	}

	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			// Level 1: direct wrap
			level1 := fmt.Errorf("level1: %w", tc.sentinel)
			if !errors.Is(level1, tc.sentinel) {
				t.Errorf("level1 wrap: expected errors.Is to find %v, got false", tc.sentinel)
			}

			// Level 2: wrap of wrap
			level2 := fmt.Errorf("level2: %w", level1)
			if !errors.Is(level2, tc.sentinel) {
				t.Errorf("level2 wrap: expected errors.Is to find %v, got false", tc.sentinel)
			}
		})
	}
}

func TestTaskErrorUnwrap(t *testing.T) {
	te := &TaskError{
		TaskID: "task-123",
		Stage:  "bank",
		Err:    fmt.Errorf("inner: %w", ErrCircuitOpen),
	}

	// errors.Is should traverse TaskError → inner → sentinel
	if !errors.Is(te, ErrCircuitOpen) {
		t.Errorf("expected errors.Is(TaskError, ErrCircuitOpen) = true")
	}

	// errors.As should find the TaskError itself
	var extracted *TaskError
	outer := fmt.Errorf("outer: %w", te)
	if !errors.As(outer, &extracted) {
		t.Errorf("expected errors.As to find *TaskError through one wrap")
	}
	if extracted.TaskID != "task-123" || extracted.Stage != "bank" {
		t.Errorf("unexpected extracted TaskError: %+v", extracted)
	}
}
