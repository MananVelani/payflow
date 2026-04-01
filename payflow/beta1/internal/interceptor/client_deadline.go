package interceptor

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// UnaryClientDeadline propagates the context deadline with a buffer.
func UnaryClientDeadline(buffer time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if deadline, ok := ctx.Deadline(); ok {
			newDeadline := deadline.Add(-buffer)
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, newDeadline)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
