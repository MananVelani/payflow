package concurrency

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// GracefulShutdown waits for tasks to finish before exiting.
// It is called in main.go to handle SIGTERM/SIGINT.
func GracefulShutdown(ctx context.Context, wg *sync.WaitGroup, timeout time.Duration, logger *slog.Logger) {
	// Root context is already cancelled by the signal handler.
	// We just wait for the waitgroup to drain.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("graceful shutdown: all active tasks finished")
	case <-time.After(timeout): // hard deadline
		logger.Warn("graceful shutdown: timed out waiting for tasks")
	}
}
