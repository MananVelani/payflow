package stream

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var (
	readyOnce sync.Once
	readyCh   = make(chan struct{})
)

// Ready returns a channel that is closed when the first successful connection
// to the coordinator has been established.
func Ready() <-chan struct{} {
	return readyCh
}

// ConnectWithRetry establishes a gRPC connection with infinite retry and jitter.
// Essential for recovery after coordinator reboots.
func ConnectWithRetry(ctx context.Context, addr string, keepaliveTime, keepaliveTimeout, retryDelay time.Duration, log *zap.Logger, dialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
	}
	opts = append(opts, dialOptions...)

	for {
		log.Info("attempting to connect to coordinator", zap.String("addr", addr))
		// Use NewClient (modern gRPC) instead of Dial
		conn, err := grpc.NewClient(addr, opts...)
		if err == nil {
			readyOnce.Do(func() { close(readyCh) })
			return conn, nil
		}

		// Jittered backoff: retryDelay +/- 50%
		jitter := time.Duration(float64(retryDelay) * (0.5 + rand.Float64()))
		select {
		case <-time.After(jitter):
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// RegisterFunc is any function that performs a gRPC registration/heartbeat.
type RegisterFunc func(context.Context) error

// RegisterWithRetry keeps trying a registration function until success.
func RegisterWithRetry(ctx context.Context, name string, fn RegisterFunc, retryDelay time.Duration, log *zap.Logger) {
	for {
		err := fn(ctx)
		if err == nil {
			log.Info("registration successful", zap.String("service", name))
			return
		}

		// Jittered backoff: retryDelay +/- 50%
		jitter := time.Duration(float64(retryDelay) * (0.5 + rand.Float64()))
		select {
		case <-time.After(jitter):
			continue
		case <-ctx.Done():
			return
		}
	}
}
