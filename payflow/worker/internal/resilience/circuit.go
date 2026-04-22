package resilience

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"

	apperrors "github.com/your-org/payflow/worker/internal/errors"
)

type BankCircuitBreaker struct {
	cb       *gobreaker.CircuitBreaker
	settings gobreaker.Settings
	mu       sync.Mutex
}

func NewBankCircuitBreaker(maxRequests uint32, timeout time.Duration, threshold float64, logger *zap.Logger) *BankCircuitBreaker {
	settings := gobreaker.Settings{
		Name:        "BankAPI",
		MaxRequests: maxRequests,
		Interval:    10 * time.Second,
		Timeout:     timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= threshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Warn("circuit breaker state changed",
				zap.String("name", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()),
			)
		},
	}

	return &BankCircuitBreaker{
		cb:       gobreaker.NewCircuitBreaker(settings),
		settings: settings,
	}
}

func (b *BankCircuitBreaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cb = gobreaker.NewCircuitBreaker(b.settings)
}

// Execute wraps an operation with circuit breaker protection.
// If the breaker is open, it returns a wrapped ErrCircuitOpen so callers
// can use errors.Is(err, apperrors.ErrCircuitOpen) without string inspection.
func (b *BankCircuitBreaker) Execute(op func() (interface{}, error)) (interface{}, error) {
	result, err := b.cb.Execute(op)
	if err != nil && errors.Is(err, gobreaker.ErrOpenState) {
		return nil, fmt.Errorf("bank circuit breaker: %w", apperrors.ErrCircuitOpen)
	}
	return result, err
}
