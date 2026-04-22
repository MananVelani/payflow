package reservation

import (
	"context"
	"fmt"
	"time"
)

// TieredStore combines a LocalStore (L1 cache) and a RedisStore (L2 distributed lock).
type TieredStore struct {
	local *LocalStore
	redis Store
}

// NewTieredStore creates a two-layer reservation store.
func NewTieredStore(local *LocalStore, redis Store) *TieredStore {
	return &TieredStore{
		local: local,
		redis: redis,
	}
}

func (s *TieredStore) Reserve(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	// L1: Local check
	// Prevents network calls if the same pod is already processing the key.
	ok, err := s.local.Reserve(ctx, key, ttl)
	if err != nil {
		return false, fmt.Errorf("local reserve: %w", err)
	}
	if !ok {
		return false, nil // already held locally
	}
	
	// L2: Redis check
	ok, err = s.redis.Reserve(ctx, key, ttl)
	if err != nil {
		// Fallback: If Redis is down, we must release the local lock to allow retry
		_ = s.local.Release(ctx, key)
		return false, fmt.Errorf("redis reserve: %w", err)
	}
	
	if !ok {
		// Acquired locally but held globally. Clean up local.
		_ = s.local.Release(ctx, key)
		return false, nil
	}
	
	return true, nil
}

func (s *TieredStore) Release(ctx context.Context, key string) error {
	// Release both layers
	errL := s.local.Release(ctx, key)
	errR := s.redis.Release(ctx, key)
	
	if errL != nil {
		return fmt.Errorf("local release: %w", errL)
	}
	return errR
}

func (s *TieredStore) Close() error {
	// Only Redis needs closing usually
	if closer, ok := s.redis.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}
