package resilience

import (
	"errors"
	"time"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"
)

// BankCircuitBreaker wraps sony/gobreaker to protect the bank API.
// It opens when the failure rate exceeds 50% over a 10s interval.
type BankCircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

func NewBankCircuitBreaker(logger *zap.Logger) *BankCircuitBreaker {
	settings := gobreaker.Settings{
		Name:        "BankAPI",
		MaxRequests: 5,               // allow few probes in half-open state
		Interval:    10 * time.Second, // clear counts every 10s
		Timeout:     5 * time.Second,  // stay open for 5s before half-open
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.5
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
		cb: gobreaker.NewCircuitBreaker(settings),
	}
}

// Execute wraps an operation with circuit breaker protection.
func (b *BankCircuitBreaker) Execute(op func() (interface{}, error)) (interface{}, error) {
	return b.cb.Execute(op)
}

// ErrCircuitOpen is returned when the breaker is open.
var ErrCircuitOpen = errors.New("resilience: circuit breaker is open")
