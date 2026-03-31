package reservation_test

import (
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/reservation"
)

func TestMap_Reserve(t *testing.T) {
	m := reservation.New(1 * time.Minute)
	key := "test-key"

	// 1. First reservation should succeed
	if err := m.Reserve(key); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// 2. Immediate duplicate should fail
	if err := m.Reserve(key); err != reservation.ErrAlreadyInProgress {
		t.Fatalf("expected ErrAlreadyInProgress, got %v", err)
	}

	// 3. Status should be InProgress
	if got := m.StateOf(key); got != reservation.StateInProgress {
		t.Fatalf("expected StateInProgress, got %v", got)
	}
}

func TestMap_Complete(t *testing.T) {
	m := reservation.New(1 * time.Minute)
	key := "test-key"

	_ = m.Reserve(key)
	m.Complete(key)

	// 1. Status should be Completed
	if got := m.StateOf(key); got != reservation.StateCompleted {
		t.Fatalf("expected StateCompleted, got %v", got)
	}

	// 2. Reserve on completed should succeed (passes through to C4)
	if err := m.Reserve(key); err != nil {
		t.Fatalf("expected nil on completed key, got %v", err)
	}
}

func TestMap_Release(t *testing.T) {
	m := reservation.New(1 * time.Minute)
	key := "test-key"

	_ = m.Reserve(key)
	m.Release(key)

	// 1. Status should be NotStarted
	if got := m.StateOf(key); got != reservation.StateNotStarted {
		t.Fatalf("expected StateNotStarted, got %v", got)
	}

	// 2. Should be able to reserve again
	if err := m.Reserve(key); err != nil {
		t.Fatalf("expected nil after release, got %v", err)
	}
}

func TestMap_Cleanup(t *testing.T) {
	ttl := 100 * time.Millisecond
	m := reservation.New(ttl)
	
	m.Reserve("k1")
	m.Complete("k1")
	
	m.Reserve("k2") // k2 is InProgress, should NOT be cleaned up

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
