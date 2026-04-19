package interceptor

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"github.com/your-org/payflow/worker/internal/observability"
)

// UnaryServerMetrics records RED metrics for unary gRPC calls.
func UnaryServerMetrics(metrics *observability.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)
		
		service, method := parseFullMethod(info.FullMethod)
		code := status.Code(err).String()
		
		metrics.GRPCServerHandledTotal.WithLabelValues(service, method, code).Inc()
		metrics.GRPCServerHandlingSeconds.WithLabelValues(service, method).Observe(duration.Seconds())
		
		return resp, err
	}
}

// StreamServerMetrics records RED metrics for streaming gRPC calls.
func StreamServerMetrics(metrics *observability.Metrics) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		duration := time.Since(start)
		
		service, method := parseFullMethod(info.FullMethod)
		code := status.Code(err).String()
		
		metrics.GRPCServerHandledTotal.WithLabelValues(service, method, code).Inc()
		metrics.GRPCServerHandlingSeconds.WithLabelValues(service, method).Observe(duration.Seconds())
		
		return err
	}
}

func parseFullMethod(fullMethod string) (string, string) {
	if fullMethod == "" || fullMethod[0] != '/' {
		return "unknown", "unknown"
	}
	parts := strings.SplitN(fullMethod[1:], "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return parts[0], "unknown"
}
