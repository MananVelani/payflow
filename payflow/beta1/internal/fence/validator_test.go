package fence_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/your-org/payflow/worker/internal/fence"
)

func TestValidateAndUpdate_AcceptsHigherEpoch(t *testing.T) {
	v := fence.NewEpochValidator()
	if err := v.ValidateAndUpdate(context.Background(), 10); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if got := v.Epoch(); got != 10 {
		t.Fatalf("expected epoch 10, got %d", got)
	}
}

func TestValidateAndUpdate_AcceptsEqualEpoch(t *testing.T) {
	v := fence.NewEpochValidator()
	_ = v.ValidateAndUpdate(context.Background(), 5)
	if err := v.ValidateAndUpdate(context.Background(), 5); err != nil {
		t.Fatalf("equal epoch should be accepted, got %v", err)
	}
}

func TestValidateAndUpdate_RejectsLowerEpoch(t *testing.T) {
	v := fence.NewEpochValidator()
	_ = v.ValidateAndUpdate(context.Background(), 10)
	err := v.ValidateAndUpdate(context.Background(), 9)
	if err == nil {
		t.Fatal("expected error for stale epoch, got nil")
	}
	var ve *fence.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.IncomingEpoch != 9 || ve.LastSeen != 10 {
		t.Fatalf("unexpected error fields: %+v", ve)
	}
}

func TestValidateAndUpdate_ConcurrentSafety(t *testing.T) {
	// 100 goroutines all racing to validate increasing epochs
	// No data race should occur (run with: go test -race ./internal/fence/...)
	v := fence.NewEpochValidator()
	var wg sync.WaitGroup
	for i := int64(0); i < 100; i++ {
		wg.Add(1)
		go func(epoch int64) {
			defer wg.Done()
			_ = v.ValidateAndUpdate(context.Background(), epoch) // some will fail; that is expected
		}(i)
	}
	wg.Wait()
	// Final epoch must be ≤ 99 and ≥ 0; just confirm no panic/race
}
