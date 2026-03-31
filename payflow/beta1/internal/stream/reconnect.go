package stream

import (
	"context"
	"math/rand"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ConnectWithRetry establishes a gRPC connection with infinite retry and jitter.
// Essential for recovery after coordinator reboots.
func ConnectWithRetry(ctx context.Context, addr string, log *zap.Logger) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	for {
		log.Info("attempting to connect to coordinator", zap.String("addr", addr))
		// Use NewClient (modern gRPC) instead of Dial
		conn, err := grpc.NewClient(addr, opts...)
		if err == nil {
			return conn, nil
		}

		log.Warn("connection failed — retrying with jitter", zap.Error(err))
		
		// Jittered backoff: 1s to 5s
		select {
		case <-time.After(time.Duration(1000+rand.Intn(4000)) * time.Millisecond):
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// RegisterFunc is any function that performs a gRPC registration/heartbeat.
type RegisterFunc func(context.Context) error

// RegisterWithRetry keeps trying a registration function until success.
func RegisterWithRetry(ctx context.Context, name string, fn RegisterFunc, log *zap.Logger) {
	for {
		err := fn(ctx)
		if err == nil {
			log.Info("registration successful", zap.String("service", name))
			return
		}

		log.Warn("registration failed — retrying with jitter", zap.String("service", name), zap.Error(err))
		
		select {
		case <-time.After(time.Duration(2000+rand.Intn(3000)) * time.Millisecond):
			continue
		case <-ctx.Done():
			return
		}
	}
}
