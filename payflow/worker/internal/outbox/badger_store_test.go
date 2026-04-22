package outbox

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerStore_AppendPendingAck(t *testing.T) {
	path := "test_badger_db"
	defer os.RemoveAll(path)

	store, err := NewBadgerStore(path)
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	entry := Entry{
		TaskID:    "task-123",
		Payload:   []byte("test-payload"),
		CreatedAt: time.Now(),
	}

	// Append
	err = store.Append(ctx, entry)
	assert.NoError(t, err)

	// Pending
	pending, err := store.Pending(ctx)
	assert.NoError(t, err)
	require.Equal(t, 1, len(pending))
	assert.Equal(t, "task-123", pending[0].TaskID)
	assert.Equal(t, []byte("test-payload"), pending[0].Payload)

	// Ack
	err = store.Ack(ctx, pending[0].ID)
	assert.NoError(t, err)

	// Pending should be empty
	pending, err = store.Pending(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(pending))
}
