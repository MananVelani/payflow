package reservation_test

import (
	"context"
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/reservation"
)

func TestLocalStore_Reserve(t *testing.T) {
	m := reservation.NewLocalStore(1 * time.Minute)
	ctx := context.Background()
	key := "test-key"
	ttl := 1 * time.Minute

	// 1. First reservation should succeed
	ok, err := m.Reserve(ctx, key, ttl)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if !ok {
		t.Fatal("expected true, got false")
	}

	// 2. Immediate duplicate should fail
	ok, err = m.Reserve(ctx, key, ttl)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if ok {
		t.Fatal("expected false, got true")
	}

	// 3. Status should be InProgress
	if got := m.StateOf(key); got != reservation.StateInProgress {
		t.Fatalf("expected StateInProgress, got %v", got)
	}
}

func TestLocalStore_Complete(t *testing.T) {
	m := reservation.NewLocalStore(1 * time.Minute)
	ctx := context.Background()
	key := "test-key"
	ttl := 1 * time.Minute

	_, _ = m.Reserve(ctx, key, ttl)
	m.Complete(key)

	// 1. Status should be Completed
	if got := m.StateOf(key); got != reservation.StateCompleted {
		t.Fatalf("expected StateCompleted, got %v", got)
	}

	// 2. Reserve on completed should succeed (passes through to C4)
	ok, err := m.Reserve(ctx, key, ttl)
	if err != nil {
		t.Fatalf("expected nil on completed key, got %v", err)
	}
	if !ok {
		t.Fatal("expected true on completed key, got false")
	}
}

func TestLocalStore_Release(t *testing.T) {
	m := reservation.NewLocalStore(1 * time.Minute)
	ctx := context.Background()
	key := "test-key"
	ttl := 1 * time.Minute

	_, _ = m.Reserve(ctx, key, ttl)
	_ = m.Release(ctx, key)

	// 1. Status should be NotStarted
	if got := m.StateOf(key); got != reservation.StateNotStarted {
		t.Fatalf("expected StateNotStarted, got %v", got)
	}

	// 2. Should be able to reserve again
	ok, err := m.Reserve(ctx, key, ttl)
	if err != nil {
		t.Fatalf("expected nil after release, got %v", err)
	}
	if !ok {
		t.Fatal("expected true after release, got false")
	}
}

func TestLocalStore_Cleanup(t *testing.T) {
	ctx := context.Background()
	ttl := 100 * time.Millisecond
	m := reservation.NewLocalStore(ttl)
	
	_, _ = m.Reserve(ctx, "k1", ttl)
	m.Complete("k1")
	
	_, _ = m.Reserve(ctx, "k2", ttl) // k2 is InProgress, should NOT be cleaned up

	time.Sleep(200 * time.Millisecond)

	n := m.Cleanup()
	if n != 1 {
		t.Fatalf("expected 1 item cleaned up, got %d", n)
	}

	if m.Size() != 1 {
		t.Fatalf("expected map size 1, got %d", m.Size())
	}

	if m.StateOf("k1") != reservation.StateNotStarted {
		t.Fatal("k1 should have been removed")
	}
	if m.StateOf("k2") != reservation.StateInProgress {
		t.Fatal("k2 should still be there")
	}
}

