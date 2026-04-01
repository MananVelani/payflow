package service

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/your-org/payflow/worker/internal/domain"
	pb "github.com/your-org/payflow/worker/proto/log"
)

// LogClientImpl implements LogServiceClient by calling the C4 gRPC service.
type LogClientImpl struct {
	conn   *grpc.ClientConn
	client pb.PaymentLogServiceClient
}

// NewLogClientImpl dials C4 and returns a ready LogClientImpl.
func NewLogClientImpl(addr string, dialOptions ...grpc.DialOption) (*LogClientImpl, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	opts = append(opts, dialOptions...)
	
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial C4 at %s: %w", addr, err)
	}
	return &LogClientImpl{
		conn:   conn,
		client: pb.NewPaymentLogServiceClient(conn),
	}, nil
}

// CheckIdempotency queries C4 to see if this key has already been processed successfully.
func (c *LogClientImpl) CheckIdempotency(ctx context.Context, key string) (bool, *domain.PaymentResult, error) {
	resp, err := c.client.CheckIdempotency(ctx, &pb.IdempotencyRequest{
		IdempotencyKey: key,
	})
	if err != nil {
		return false, nil, err
	}

	if !resp.Exists {
		return false, nil, nil
	}

	// If it exists, we construct a PaymentResult based on the response.
	// C4 only returns success/fail status for idempotency.
	result := &domain.PaymentResult{
		IdempotencyKey: key,
		BankTxnRef:     resp.TxnId,
	}
	if resp.Success {
		result.Status = domain.TaskStatusSuccess
	} else {
		result.Status = domain.TaskStatusFailure
	}

	return true, result, nil
}

// Close releases the gRPC connection.
func (c *LogClientImpl) Close() error {
	return c.conn.Close()
}
