package grpctransport

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/your-org/payflow/worker/internal/domain"
	"github.com/your-org/payflow/worker/internal/metrics"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

type HeartbeatClient struct {
	coordinatorAddr string
	workerID        string
	grpcPort        int
	maxCapacity     int
	interval        time.Duration
	statsProvider   func() domain.WorkerStats
	logger          *zap.Logger
	localEpoch      int64
}

func NewHeartbeatClient(
	workerID string,
	grpcPort, maxCapacity int,
	interval time.Duration,
	statsProvider func() domain.WorkerStats,
	logger *zap.Logger,
) *HeartbeatClient {
	return &HeartbeatClient{
		workerID:      workerID,
		grpcPort:      grpcPort,
		maxCapacity:   maxCapacity,
		interval:      interval,
		statsProvider: statsProvider,
		logger:        logger,
	}
}

// RunSession keeps the heartbeat alive for a given connection.
// It returns an error if the connection or stream fails.
func (h *HeartbeatClient) RunSession(ctx context.Context, conn *grpc.ClientConn) error {
	client := pb.NewWorkerManagementClient(conn)

	// ── STEP 1: Registration ─────────────────────────────────────────────
	reg, err := client.RegisterWorker(ctx, &pb.RegisterRequest{
		WorkerId:           h.workerID,
		Epoch:              h.localEpoch,
		ProcessingCapacity: int32(h.maxCapacity),
	})
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	if !reg.Success {
		return fmt.Errorf("registration rejected by coordinator")
	}

	h.logger.Info("registered with coordinator",
		zap.String("worker_id", h.workerID),
		zap.Int64("epoch", h.localEpoch))

	// ── STEP 2: Heartbeat Stream ─────────────────────────────────────────
	stream, err := client.WorkerHeartbeat(ctx)
	if err != nil {
		return fmt.Errorf("failed to open heartbeat stream: %w", err)
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	// Goroutine to receive Acks and update LocalEpoch
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				return
			}
			if ack.Epoch > h.localEpoch {
				h.logger.Info("epoch incremented by C2",
					zap.Int64("old", h.localEpoch),
					zap.Int64("new", ack.Epoch))
				h.localEpoch = ack.Epoch
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			stats := h.statsProvider()
			err := stream.Send(&pb.HeartbeatPing{
				WorkerId:            h.workerID,
				Load:                stats.Load,
				TasksProcessedCount: stats.TasksProcessedCount,
				AvgTaskDurationMs:   stats.AvgTaskDurationMS,
				Epoch:               h.localEpoch,
			})
			if err != nil {
				return fmt.Errorf("heartbeat send failed: %w", err)
			}
			metrics.HeartbeatSentTotal.Inc()
		}
	}
}
