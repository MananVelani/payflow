package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/your-org/payflow/worker/internal/observability"
)

func TestUnaryServerRecovery(t *testing.T) {
	logger := observability.NewLogger(zap.NewNop())
	interceptor := UnaryServerRecovery(logger)
	
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		panic("test panic")
	}
	
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}
	resp, err := interceptor(context.Background(), nil, info, handler)
	
	assert.Nil(t, resp)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Equal(t, "internal server error", st.Message())
}

func TestStreamServerRecovery(t *testing.T) {
	logger := observability.NewLogger(zap.NewNop())
	interceptor := StreamServerRecovery(logger)
	
	handler := func(srv interface{}, ss grpc.ServerStream) error {
		panic("test stream panic")
	}
	
	info := &grpc.StreamServerInfo{FullMethod: "/test.Service/StreamMethod"}
	err := interceptor(nil, &mockServerStream{}, info, handler)
	
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
}

type mockServerStream struct {
	grpc.ServerStream
}

func (m *mockServerStream) Context() context.Context {
	return context.Background()
}
