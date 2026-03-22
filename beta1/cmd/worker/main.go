package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/your-org/payflow/worker/config"
	"github.com/your-org/payflow/worker/internal/logger"
	"github.com/your-org/payflow/worker/internal/metrics"
	grpctransport "github.com/your-org/payflow/worker/internal/transport/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log, err := logger.New(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	defer log.Sync() //nolint:errcheck

	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = "worker-" + uuid.New().String()[:8]
	}

	log.Info("worker service starting",
		zap.String("worker_id", workerID),
		zap.String("coordinator_addr", cfg.CoordinatorAddr),
		zap.String("log_service_addr", cfg.LogServiceAddr),
		zap.String("bank_api_addr", cfg.BankAPIAddr),
	)

	metrics.Register()

	// Start Prometheus metrics server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: fmt.Sprintf(":%d", cfg.MetricsPort), Handler: metricsMux}
	go func() {
		log.Info("prometheus metrics server starting", zap.Int("port", cfg.MetricsPort))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// Start gRPC server
	grpcServer, err := grpctransport.NewServer(log)
	if err != nil {
		return fmt.Errorf("gRPC server init: %w", err)
	}
	assignedPort, err := grpcServer.Listen(cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("gRPC listen: %w", err)
	}
	log.Info("gRPC server bound", zap.Int("port", assignedPort))

	grpcErrCh := make(chan error, 1)
	go func() { grpcErrCh <- grpcServer.Serve() }()

	// Start heartbeat loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	statsProvider := func() (float32, int64, int64, int64) { return 0.0, 0, 0, 0 }
	heartbeat := grpctransport.NewHeartbeatClient(
		cfg.CoordinatorAddr, workerID, assignedPort, cfg.MaxConcurrentTasks,
		cfg.HeartbeatInterval, statsProvider, log,
	)
	go heartbeat.Run(ctx)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		log.Info("received shutdown signal", zap.String("signal", sig.String()))
	case err := <-grpcErrCh:
		if err != nil {
			return fmt.Errorf("gRPC server: %w", err)
		}
	}

	// Graceful shutdown
	log.Info("beginning graceful shutdown")
	cancel()
	grpcServer.Stop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("metrics server shutdown", zap.Error(err))
	}

	log.Info("worker service stopped cleanly", zap.String("worker_id", workerID))
	return nil
}
