package grpctransport

import (
	"context"
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/your-org/payflow/worker/internal/domain"
	"github.com/your-org/payflow/worker/internal/service"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

type TaskPoller struct {
	workerID string
	client   pb.WorkerManagementClient
	svc      service.WorkerService
	logger   *zap.Logger
}

func NewTaskPoller(workerID string, conn *grpc.ClientConn, svc service.WorkerService, logger *zap.Logger) *TaskPoller {
	return &TaskPoller{
		workerID: workerID,
		client:   pb.NewWorkerManagementClient(conn),
		svc:      svc,
		logger:   logger,
	}
}

func (p *TaskPoller) Run(ctx context.Context) error {
	p.logger.Info("task poller starting", zap.String("worker_id", p.workerID))
	return p.poll(ctx)
}

func (p *TaskPoller) poll(ctx context.Context) error {
	stream, err := p.client.PollTasks(ctx, &pb.PollRequest{WorkerId: p.workerID})
	if err != nil {
		return err
	}

	p.logger.Info("task stream established with coordinator")

	for {
		assignment, err := stream.Recv()
		if err == io.EOF {
			p.logger.Warn("coordinator closed the task stream")
			return nil
		}
		if err != nil {
			return err
		}

		p.logger.Info("received task assignment",
			zap.String("task_id", assignment.TaskId),
			zap.Int64("epoch", assignment.Epoch))

		// Map proto to domain task
		task := &domain.Task{
			TaskID:         assignment.TaskId,
			IdempotencyKey: assignment.IdempotencyKey,
			Amount:         assignment.Amount,
			Currency:       assignment.Currency,
			MerchantID:     assignment.MerchantId,
			Epoch:          assignment.Epoch,
		}

		// Execute task asynchronously
		go func(t *domain.Task) {
			_, err := p.svc.ExecuteTask(context.Background(), t)
			if err != nil {
				p.logger.Error("task execution failed", zap.String("task_id", t.TaskID), zap.Error(err))
			}
		}(task)
	}
}
