package grpctransport

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/your-org/payflow/worker/internal/domain"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

// C2Client manages the gRPC connection to the C2 Coordinator.
type C2Client struct {
	conn   *grpc.ClientConn
	client pb.WorkerManagementClient
	logger *zap.Logger
}

// NewC2Client dials the coordinator and returns a ready C2Client.
// addr is from config: COORDINATOR_ADDR
func NewC2Client(addr string, logger *zap.Logger) (*C2Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial coordinator %s: %w", addr, err)
	}
	return &C2Client{
		conn:   conn,
		client: pb.NewWorkerManagementClient(conn),
		logger: logger,
	}, nil
}

// ReportResult sends the payment outcome to C2.
// This satisfies the service.ReportResultFunc signature when wrapped.
func (c *C2Client) ReportResult(ctx context.Context, result *domain.PaymentResult) error {
	req := &pb.TaskResult{
		TaskId:   result.TaskID,
		WorkerId: result.WorkerID,
		// Epoch is filled by the service layer now
	}

	if result.Status == domain.TaskStatusSuccess {
		req.Status = &pb.TaskResult_Success{Success: true}
	} else {
		req.Status = &pb.TaskResult_ErrorMessage{ErrorMessage: "payment failed"}
	}

	_, err := c.ReportRawResult(ctx, req)
	return err
}

// ReportRawResult sends a pre-built proto result to C2. Used by the outbox relay.
func (c *C2Client) ReportRawResult(ctx context.Context, req *pb.TaskResult) (*pb.ResultAck, error) {
	resp, err := c.client.ReportResult(ctx, req)
	if err != nil {
		c.logger.Error("ReportRawResult to C2 failed", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// Close releases the gRPC connection.
func (c *C2Client) Close() error {
	return c.conn.Close()
}
