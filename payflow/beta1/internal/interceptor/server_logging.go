package interceptor

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"github.com/your-org/payflow/worker/internal/observability"
)

// UnaryServerLogging logs gRPC unary calls and injects task_id into context.
func UnaryServerLogging(logger *observability.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		
		// Extract task_id from metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if ids := md.Get("task_id"); len(ids) > 0 {
				ctx = observability.WithTaskID(ctx, ids[0])
			}
		}
		
		resp, err := handler(ctx, req)
		
		duration := time.Since(start)
		logger.Info(ctx, "grpc unary call",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
		
		return resp, err
	}
}

// StreamServerLogging logs gRPC stream calls and injects task_id into context.
func StreamServerLogging(logger *observability.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		ctx := ss.Context()
		
		// Extract task_id from metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if ids := md.Get("task_id"); len(ids) > 0 {
				ctx = observability.WithTaskID(ctx, ids[0])
			}
		}
		
		// Wrap stream to provide updated context
		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		err := handler(srv, wrapped)
		
		duration := time.Since(start)
		logger.Info(ctx, "grpc stream call",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
		
		return err
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
