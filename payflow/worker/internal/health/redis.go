package health

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisHealthCheck attempts to PING Redis and returns an error if it fails.
// It is timeout-bounded to 2 seconds.
func RedisHealthCheck(ctx context.Context, url string) error {
	if url == "" {
		return nil
	}

	opts, err := redis.ParseURL(url)
	if err != nil {
		return fmt.Errorf("invalid redis url: %w", err)
	}

	client := redis.NewClient(opts)
	defer client.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redis unreachable: %w", err)
	}

	return nil
}
