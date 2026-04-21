package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/your-org/payflow/worker/internal/version"
)

func TestUnaryServerVersion(t *testing.T) {
	logger := zap.NewNop()
	interceptor := UnaryServerVersion(logger)
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	t.Run("correct version", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-contract-version", version.ContractVersion))
		resp, err := interceptor(ctx, nil, info, handler)
		assert.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("missing version", func(t *testing.T) {
		ctx := context.Background()
		resp, err := interceptor(ctx, nil, info, handler)
		assert.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("wrong version", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-contract-version", "wrong/v1"))
		resp, err := interceptor(ctx, nil, info, handler)
		assert.Error(t, err)
		assert.Nil(t, resp)
		
		st, ok := status.FromError(err)
		assert.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Contains(t, st.Message(), "contract version mismatch")
	})
}
