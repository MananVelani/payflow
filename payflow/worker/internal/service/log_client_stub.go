package service

import (
	"context"
	"github.com/your-org/payflow/worker/internal/domain"
	"go.uber.org/zap"
)

// LogServiceClientStub is a no-op for Week 1.
// Replace with a real gRPC client to C4 in Week 2.
type LogServiceClientStub struct{ logger *zap.Logger }

func NewLogServiceClientStub(logger *zap.Logger) *LogServiceClientStub {
	return &LogServiceClientStub{logger: logger}
}

func (s *LogServiceClientStub) CheckIdempotency(ctx context.Context, key string) (bool, *domain.PaymentResult, error) {
	s.logger.Debug("CheckIdempotency stub — always returns false in Week 1", zap.String("idempotency_key", key))
	return false, nil, nil
}
