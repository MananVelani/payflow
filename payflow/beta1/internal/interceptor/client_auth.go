package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/your-org/payflow/worker/internal/version"
)

// UnaryClientAuth attaches the worker token and contract version to unary gRPC calls.
func UnaryClientAuth(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx,
			"x-worker-id", token,
			"x-contract-version", version.ContractVersion,
		)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientAuth attaches the worker token and contract version to streaming gRPC calls.
func StreamClientAuth(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx,
			"x-worker-id", token,
			"x-contract-version", version.ContractVersion,
		)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
