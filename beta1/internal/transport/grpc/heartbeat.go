package grpctransport

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/your-org/payflow/worker/internal/metrics"
)

type HeartbeatClient struct {
	coordinatorAddr string
	workerID        string
	grpcPort        int
	maxCapacity     int
	interval        time.Duration
	statsProvider   func() (load float32, processed int64, avgDurationMS int64, epoch int64)
	logger          *zap.Logger
}

func NewHeartbeatClient(
	coordinatorAddr, workerID string,
	grpcPort, maxCapacity int,
	interval time.Duration,
	statsProvider func() (float32, int64, int64, int64),
	logger *zap.Logger,
) *HeartbeatClient {
	return &HeartbeatClient{coordinatorAddr, workerID, grpcPort, maxCapacity, interval, statsProvider, logger}
}

// Run loops forever until ctx is cancelled.
// On coordinator disconnect (e.g. leader election), it reconnects automatically.
func (h *HeartbeatClient) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			h.logger.Info("heartbeat loop stopping")
			return
		default:
		}
		h.logger.Info("connecting to coordinator", zap.String("addr", h.coordinatorAddr), zap.String("worker_id", h.workerID))
		if err := h.runSession(ctx); err != nil {
			h.logger.Warn("coordinator session ended — reconnecting in 2s", zap.Error(err))
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (h *HeartbeatClient) runSession(ctx context.Context) error {
	conn, err := grpc.DialContext( //nolint:staticcheck
		ctx, h.coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	// WEEK 1 STUB — replace with generated gRPC client when M3 commits proto stubs:
	//   client := workerv1.NewWorkerServiceClient(conn)
	//   ack, err := client.RegisterWorker(ctx, &workerv1.WorkerInfo{...})
	//   stream, err := client.WorkerHeartbeat(ctx)
	h.logger.Info("registered with coordinator (STUB — wire real gRPC in Week 2)",
		zap.String("worker_id", h.workerID),
		zap.String("coordinator_addr", h.coordinatorAddr),
		zap.Int("grpc_port", h.grpcPort),
	)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			load, processed, avgMS, epoch := h.statsProvider()
			// WEEK 1 STUB — replace with: stream.Send(&workerv1.HeartbeatPing{...})
			h.logger.Info("heartbeat sent (STUB)",
				zap.String("worker_id", h.workerID),
				zap.Float32("load", load),
				zap.Int64("tasks_processed", processed),
				zap.Int64("avg_duration_ms", avgMS),
				zap.Int64("epoch", epoch),
			)
			metrics.HeartbeatSentTotal.Inc()
		}
	}
}
