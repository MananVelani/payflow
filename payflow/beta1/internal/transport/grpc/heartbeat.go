package grpctransport

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	coordinatorAddr, workerID string,
	grpcPort, maxCapacity int,
	interval time.Duration,
	statsProvider func() domain.WorkerStats,
	logger *zap.Logger,
) *HeartbeatClient {
	return &HeartbeatClient{
		coordinatorAddr: coordinatorAddr,
		workerID:        workerID,
		grpcPort:        grpcPort,
		maxCapacity:     maxCapacity,
		interval:        interval,
		statsProvider:   statsProvider,
		logger:          logger,
	}
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
	conn, err := grpc.DialContext(ctx, h.coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

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
