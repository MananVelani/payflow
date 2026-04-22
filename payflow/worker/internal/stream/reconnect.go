package stream

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/your-org/payflow/worker/internal/resilience"
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

	var conn *grpc.ClientConn
	operation := func() error {
		log.Info("attempting to connect to coordinator", zap.String("addr", addr))
		var err error
		conn, err = grpc.NewClient(addr, opts...)
		if err != nil {
			return err
		}
		readyOnce.Do(func() { close(readyCh) })
		return nil
	}

	// Max delay for coordinator reconnection capped at 30s
	err := resilience.ExecuteInfiniteRetry(ctx, operation, retryDelay, 30*time.Second)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// RegisterFunc is any function that performs a gRPC registration/heartbeat.
type RegisterFunc func(context.Context) error

// RegisterWithRetry keeps trying a registration function until success.
func RegisterWithRetry(ctx context.Context, name string, fn RegisterFunc, retryDelay time.Duration, log *zap.Logger) {
	operation := func() error {
		err := fn(ctx)
		if err != nil {
			return err
		}
		log.Info("registration successful", zap.String("service", name))
		return nil
	}

	_ = resilience.ExecuteInfiniteRetry(ctx, operation, retryDelay, 30*time.Second)
}
