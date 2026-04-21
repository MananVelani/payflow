package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
)

// MemoryStore is an in-memory implementation of the Store interface.
// It is intended for testing and as a non-durable fallback.
type MemoryStore struct {
	mu     sync.Mutex
	items  []Entry
	leases map[string]bool // taskID -> in-flight
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		items:  make([]Entry, 0),
		leases: make(map[string]bool),
	}
}

func (s *MemoryStore) Append(ctx context.Context, entry Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if entry.ID == "" {
		entry.ID = uuid.New().String()
	}
	s.items = append(s.items, entry)
	return nil
}

func (s *MemoryStore) Pending(ctx context.Context) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Return a copy to avoid data races
	res := make([]Entry, len(s.items))
	copy(res, s.items)
	return res, nil
}

func (s *MemoryStore) Ack(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	newItems := make([]Entry, 0, len(s.items))
	for _, it := range s.items {
		if it.ID != id {
			newItems = append(newItems, it)
		}
	}
	s.items = newItems
	return nil
}

func (s *MemoryStore) SetLease(ctx context.Context, taskID string, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases[taskID] = true
	return nil
}

func (s *MemoryStore) DeleteLease(ctx context.Context, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.leases, taskID)
	return nil
}

func (s *MemoryStore) ListLeases(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	var ids []string
	for id := range s.leases {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *MemoryStore) Close() error {
	return nil
}
