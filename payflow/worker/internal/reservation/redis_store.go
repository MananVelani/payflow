package reservation

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore implements the Store interface using Redis SET-NX.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore creates a new Redis mutation store.
func NewRedisStore(url string) (*RedisStore, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url: %w", err)
	}
	
	client := redis.NewClient(opts)
	// Check connection
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}
	
	return &RedisStore{client: client}, nil
}

func (s *RedisStore) Reserve(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	// SET key "" NX EX ttl
	// returns true if key was set, false if it already exists
	success, err := s.client.SetNX(ctx, key, "", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx: %w", err)
	}
	return success, nil
}

func (s *RedisStore) Release(ctx context.Context, key string) error {
	// DEL key
	if err := s.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}
