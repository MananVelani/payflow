package reservation_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/your-org/payflow/worker/internal/reservation"
)

func TestTieredStore_Concurrency(t *testing.T) {
	// 1. Setup miniredis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// 2. Setup Stores
	local := reservation.NewLocalStore(1 * time.Minute)
	redis, err := reservation.NewRedisStore("redis://" + mr.Addr())
	require.NoError(t, err)

	tiered := reservation.NewTieredStore(local, redis)
	ctx := context.Background()
	key := "test-idempotency-key"
	ttl := 1 * time.Minute

	// 3. First reservation should succeed
	ok, err := tiered.Reserve(ctx, key, ttl)
	assert.NoError(t, err)
	assert.True(t, ok, "First reservation should be successful")

	// 4. Second concurrent reservation (same key) should fail
	ok, err = tiered.Reserve(ctx, key, ttl)
	assert.NoError(t, err)
	assert.False(t, ok, "Second concurrent reservation should fail")

	// 5. Release the key
	err = tiered.Release(ctx, key)
	assert.NoError(t, err)

	// 6. Reservation after release should succeed
	ok, err = tiered.Reserve(ctx, key, ttl)
	assert.NoError(t, err)
	assert.True(t, ok, "Reservation after release should be successful")
}

func TestTieredStore_L1Bypass(t *testing.T) {
	// Verify that if it's held in Redis but NOT in local, it still fails (L2 works)
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	local := reservation.NewLocalStore(1 * time.Minute)
	redis, err := reservation.NewRedisStore("redis://" + mr.Addr())
	require.NoError(t, err)

	tiered := reservation.NewTieredStore(local, redis)
	ctx := context.Background()
	key := "external-key"
	ttl := 1 * time.Minute

	// Manually set in Redis (simulating another pod)
	mr.Set(key, "")

	ok, err := tiered.Reserve(ctx, key, ttl)
	assert.NoError(t, err)
	assert.False(t, ok, "Should fail because Redis already has the key (L2 guard)")
	
	// Local should still be clean (since L2 failed, L1 should HAVE BEEN released if it was set)
	assert.Equal(t, reservation.StateNotStarted, local.StateOf(key))
}
