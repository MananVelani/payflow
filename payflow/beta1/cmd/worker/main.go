package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"log/slog"

	"github.com/your-org/payflow/worker/config"
	"github.com/your-org/payflow/worker/internal/logger"
	"github.com/your-org/payflow/worker/internal/metrics"
	"github.com/your-org/payflow/worker/internal/reservation"
	"github.com/your-org/payflow/worker/internal/outbox"
	"github.com/your-org/payflow/worker/internal/service"
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

	// WEEK 2: Real Dependency Injection
	c2Client, err := grpctransport.NewC2Client(cfg.CoordinatorAddr, log)
	if err != nil {
		return fmt.Errorf("failed to init C2 client: %w", err)
	}
	defer c2Client.Close()

	c4Client, err := service.NewLogClientImpl(cfg.LogServiceAddr)
	if err != nil {
		return fmt.Errorf("failed to init C4 client: %w", err)
	}
	defer c4Client.Close()

	bankCfg := service.MockBankClientConfig{
		BaseURL:       cfg.BankAPIAddr,
		FailRate:      cfg.BankFailRate,
		LatencyMinMS:  cfg.BankLatencyMinMS,
		LatencyMaxMS:  cfg.BankLatencyMaxMS,
		MaxAttempts:   uint(cfg.RetryMaxAttempts),
		BaseDelayMS:   cfg.RetryBaseDelayMS,
		HTTPTimeout:   10 * time.Second,
		CBMaxRequests: 5,
		CBInterval:    10 * time.Second,
		CBTimeout:     5 * time.Second,
		CBMinRequests: 3,
	}
	bankClient := service.NewProductionMockBankClient(bankCfg, log)

	// --- WEEK 2 ADDITION: Reservation map ---
	reservationMap := reservation.New(5 * time.Minute)
 
	// --- WEEK 2 ADDITION: In-memory outbox ---
	// Wrap slog for the outbox (Section 7: will be upgraded to observability.Logger in CP-6)
	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	outboxBuf := outbox.New(c2Client.ReportRawResult, slogLogger)
	// --- END WEEK 2 ADDITION ---

	workerSvc := service.NewWorkerServiceImpl(
		bankClient,
		c4Client,
		c2Client.ReportResult,
		log,
		cfg,
		reservationMap, // WEEK 2 ADDITION
		outboxBuf,      // WEEK 2 ADDITION
	)

	// Start gRPC server
	grpcServer, err := grpctransport.NewServer(workerSvc, log)
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

	heartbeat := grpctransport.NewHeartbeatClient(
		cfg.CoordinatorAddr, workerID, assignedPort, cfg.MaxConcurrentTasks,
		cfg.HeartbeatInterval, workerSvc.Stats, log,
	)
	go heartbeat.Run(ctx)

	// --- WEEK 2 ADDITION: Outbox relay ---
	outboxBuf.Start(ctx)
	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Reservation map TTL cleanup ---
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				n := reservationMap.Cleanup()
				if n > 0 {
					slog.Debug("reservation: cleaned up completed entries", "count", n)
				}
			case <-ctx.Done(): // ctx is the worker's root context
				return
			}
		}
	}()
	// --- END WEEK 2 ADDITION ---

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
