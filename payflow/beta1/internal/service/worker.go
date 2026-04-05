package service

import (
	"context"
	"github.com/your-org/payflow/worker/internal/domain"
)

type WorkerService interface {
	ExecuteTask(ctx context.Context, task *domain.Task) (*domain.PaymentResult, error)
	RevokeTask(ctx context.Context, taskID string) error
	Stats() domain.WorkerStats
}

type BankClient interface {
	// idempotency_key MUST be identical across all retry attempts — never generate a new one per retry
	Charge(ctx context.Context, idempotencyKey string, amount float64, currency string, merchantID string) (txnRef string, err error)
}

type LogServiceClient interface {
	CheckIdempotency(ctx context.Context, idempotencyKey string) (exists bool, result *domain.PaymentResult, err error)
}

// ReportResultFunc is a function type so C2 client can be injected without a circular import.
type ReportResultFunc func(ctx context.Context, result *domain.PaymentResult) error
