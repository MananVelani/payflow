package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"log/slog"

	"github.com/your-org/payflow/worker/internal/config"
	"github.com/your-org/payflow/worker/internal/concurrency"
	"github.com/your-org/payflow/worker/internal/health"
	"github.com/your-org/payflow/worker/internal/heartbeat"
	"github.com/your-org/payflow/worker/internal/interceptor"
	"github.com/your-org/payflow/worker/internal/loadreport"
	"github.com/your-org/payflow/worker/internal/logger"
	"github.com/your-org/payflow/worker/internal/metrics"
	"github.com/your-org/payflow/worker/internal/observability"
	"github.com/your-org/payflow/worker/internal/outbox"
	"github.com/your-org/payflow/worker/internal/reservation"
	"github.com/your-org/payflow/worker/internal/service"
	"github.com/your-org/payflow/worker/internal/stream"
	"github.com/your-org/payflow/worker/internal/tracing"
	grpctransport "github.com/your-org/payflow/worker/internal/transport/grpc"
	"google.golang.org/grpc"
)

var configPath string

func main() {
	rootCmd := &cobra.Command{
		Use:   "worker",
		Short: "PayFlow C3 Worker Service",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run()
		},
	}

	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "", "path to config file (optional)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(configPath)
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

	// --- Week 4: pprof endpoint ---
	go func() {
		log.Info("pprof enabled on :6060")
		if ppErr := http.ListenAndServe(":6060", nil); ppErr != nil {
			log.Debug("pprof server stopped", zap.Error(ppErr))
		}
	}()

	// --- Week 4: Tracing init ---
	tracingShutdown, tErr := tracing.Init(context.Background(), tracing.Config{
		Enabled:     true,
		ServiceName: "c3-worker",
		Endpoint:    "jaeger:4317",
	})
	if tErr != nil {
		log.Warn("tracing init failed — running without traces", zap.Error(tErr))
	}
	defer tracingShutdown(context.Background())

	// --- WEEK 2 ADDITION: Interceptors ---
	obsLog := observability.NewLogger(log)
	obsMetrics := observability.NewMetrics()

	clientUnaryChain := grpc.WithChainUnaryInterceptor(
		interceptor.UnaryClientAuth(cfg.WorkerToken),
		interceptor.UnaryClientDeadline(200*time.Millisecond),
	)
	clientStreamChain := grpc.WithChainStreamInterceptor(
		interceptor.StreamClientAuth(cfg.WorkerToken),
	)

	// --- CP-7: Connect with Retry + Keepalive ──────────────────
	rootCtx := context.Background()
	conn, err := stream.ConnectWithRetry(
		rootCtx,
		cfg.CoordinatorAddr,
		cfg.KeepaliveTime,
		cfg.KeepaliveTimeout,
		cfg.ConnectRetryDelay,
		log,
		clientUnaryChain,
		clientStreamChain,
	)
	if err != nil {
		return fmt.Errorf("coordinator connection failed: %w", err)
	}
	defer conn.Close()

	c2Client := grpctransport.NewC2Client(conn, log)

	c4Client, err := service.NewLogClientImpl(cfg.LogServiceAddr, clientUnaryChain)
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
		BaseDelayMS:   int(cfg.RetryBaseDelay.Milliseconds()),
		HTTPTimeout:   10 * time.Second,
		CBMaxRequests: 5,
		CBInterval:    10 * time.Second,
		CBTimeout:     5 * time.Second,
		CBMinRequests: 3,
	}
	bankClient := service.NewProductionMockBankClient(bankCfg, log, obsMetrics)


	// Log full configuration at startup for audit trail
	obsLog.WithRaw().Info("worker configuration loaded",
		zap.Int("max_concurrent_tasks", cfg.MaxConcurrentTasks),
		zap.Duration("shutdown_timeout", cfg.ShutdownTimeout),
		zap.Int("retry_max_attempts", cfg.RetryMaxAttempts),
		zap.Duration("retry_base_delay", cfg.RetryBaseDelay),
		zap.Duration("retry_max_delay", cfg.RetryMaxDelay),
		zap.Uint32("cb_max_requests", cfg.CBMaxRequests),
		zap.Float64("cb_failure_threshold", cfg.CBFailureThreshold),
		zap.Duration("cb_timeout", cfg.CBTimeout),
		zap.Duration("keepalive_time", cfg.KeepaliveTime),
		zap.Duration("keepalive_timeout", cfg.KeepaliveTimeout),
		zap.Duration("connect_retry_delay", cfg.ConnectRetryDelay),
		zap.Duration("outbox_flush_interval", cfg.OutboxFlushInterval),
		zap.Int("outbox_max_size", cfg.OutboxMaxSize),
		zap.String("health_port", cfg.HealthPort),
		zap.String("coordinator_addr", cfg.CoordinatorAddr),
		zap.Int("grpc_port", cfg.GRPCPort),
	)

	// --- Checkpoint 2: Redis startup validation ---
	if cfg.RequireRedis && cfg.ReservationRedisURL == "" {
		return fmt.Errorf("REQUIRE_REDIS=true but RESERVATION_REDIS_URL is not set — refusing to start in multi-replica mode")
	}

	// --- WEEK 2 ADDITION: Reservation Store initialization ---
	var resStore reservation.Store
	localRes := reservation.NewLocalStore(cfg.RetryMaxDelay * 5)
	
	if cfg.ReservationRedisURL != "" {
		redisRes, err := reservation.NewRedisStore(cfg.ReservationRedisURL)
		if err != nil {
			if cfg.RequireRedis {
				return fmt.Errorf("reservation: failed to connect to Redis: %w", err)
			}
			log.Error("reservation: failed to connect to Redis, falling back to LocalStore only", zap.Error(err))
			resStore = localRes
		} else {
			log.Info("reservation: using TieredStore (L1: Local, L2: Redis)")
			resStore = reservation.NewTieredStore(localRes, redisRes)
			defer resStore.(interface{ Close() error }).Close()
		}
	} else {
		log.Warn("reservation: RESERVATION_REDIS_URL not set! running without distributed reservation — not safe for multi-replica deployments")
		resStore = localRes
	}


	// --- WEEK 2 ADDITION: Outbox store initialization ---
	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var outboxStore outbox.Store
	if cfg.OutboxDBPath != "" {
		if err := os.MkdirAll(cfg.OutboxDBPath, 0750); err != nil {
			return fmt.Errorf("outbox: failed to create data directory: %w", err)
		}
		bs, err := outbox.NewBadgerStore(cfg.OutboxDBPath)
		if err != nil {
			return fmt.Errorf("outbox: failed to initialize BadgerStore: %w", err)
		}
		outboxStore = bs
	} else {
		return fmt.Errorf("outbox: OUTBOX_DB_PATH is empty; durable storage is required for production")
	}
	defer outboxStore.Close()

	outboxBuf := outbox.New(c2Client.ReportRawResult, outboxStore, cfg.OutboxFlushInterval, cfg.RetryBaseDelay, cfg.OutboxMaxSize, cfg.MaxTaskDuration, metrics.TaskDeadlineExceededTotal, slogLogger)



	// --- WEEK 2 ADDITION: Concurrency ---
	sem := concurrency.NewTaskSemaphore(
		cfg.MaxConcurrentTasks,
		outboxStore,
		cfg.MaxTaskDuration,
		obsMetrics.WorkerSaturation,
		obsMetrics.OrphanedLeaseCount,
		log,
	)

	registry := concurrency.NewTaskRegistry()
	var wg sync.WaitGroup // for task draining

	// --- Week 4: Load reporter ---
	durationRing := heartbeat.NewRingBuffer(100)
	var workerActive atomic.Int64
	var workerProcessed atomic.Int64
	loadRep := loadreport.NewReporter(durationRing, cfg.MaxConcurrentTasks, &workerActive, &workerProcessed)

	workerSvc := service.NewWorkerServiceImpl(
		bankClient,
		c4Client,
		c2Client.ReportResult,
		obsLog, // WEEK 2: upgraded to contextual logger
		cfg,
		resStore,
		outboxBuf,
		sem,
		registry,
		&wg,
		obsMetrics,
		loadRep,
	)



	// --- Server Interceptors ---
	recoveryU := interceptor.UnaryServerRecovery(obsLog)
	loggingU := interceptor.UnaryServerLogging(obsLog)
	metricsU := interceptor.UnaryServerMetrics(obsMetrics)

	recoveryS := interceptor.StreamServerRecovery(obsLog)
	loggingS := interceptor.StreamServerLogging(obsLog)

	// Start gRPC server
	grpcServer, err := grpctransport.NewServer(
		workerSvc,
		log,
		grpc.ChainUnaryInterceptor(recoveryU, loggingU, metricsU),
		grpc.ChainStreamInterceptor(recoveryS, loggingS),
	)
	if err != nil {
		return fmt.Errorf("gRPC server init: %w", err)
	}
	assignedPort, err := grpcServer.Listen(cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("gRPC listen: %w", err)
	}
	log.Info("gRPC server bound", zap.Int("port", assignedPort))

	var grpcReady atomic.Bool
	grpcReady.Store(true)

	// --- CP-8: Health Checks ──────────────────
	healthH := health.NewHandler(outboxBuf.IsRunning, stream.Ready(), grpcReady.Load, func() error {
		if cfg.ReservationRedisURL != "" {
			return health.RedisHealthCheck(rootCtx, cfg.ReservationRedisURL)
		}
		return nil
	}, workerSvc)
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", healthH.Healthz)
	healthMux.HandleFunc("/readyz", healthH.Readyz)
	healthMux.HandleFunc("/demo/reset-breaker", healthH.HandleResetBreaker)
	healthMux.HandleFunc("/demo/backpressure", healthH.HandleSetBackpressureMode)
	healthServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.HealthPort),
		Handler: healthMux,
	}
	go func() {
		log.Info("health server starting", zap.String("port", cfg.HealthPort))
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("health server error", zap.Error(err))
		}
	}()

	// --- Week 4: /metrics/health status endpoint ---
	statusH := health.NewStatusHandler(
		workerID,
		loadRep.Snapshot,
		func() string { return "closed" }, // simplified; real version reads CB state
		func() int {
			entries, _ := outboxStore.Pending(context.Background())
			return len(entries)
		},
	)
	healthMux.Handle("/metrics/health", statusH)
	// --- END CP-8 ---

	grpcErrCh := make(chan error, 1)
	go func() { grpcErrCh <- grpcServer.Serve() }()

	// ── CP-7: Heartbeat with Keepalive ───────────────────────
	// Root context for long-lived components
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	heartbeat := grpctransport.NewHeartbeatClient(
		workerID, assignedPort, cfg.MaxConcurrentTasks,
		cfg.HeartbeatInterval, workerSvc.Stats, log,
	)
	
	registerFn := func(sCtx context.Context) error {
		return heartbeat.RunSession(sCtx, conn)
	}
	
	// Run the heartbeat loop with infinite retry
	go stream.RegisterWithRetry(ctx, "heartbeat", registerFn, cfg.ConnectRetryDelay, log)

	// --- DEMO ADDITION: Task Polling ---
	poller := grpctransport.NewTaskPoller(workerID, conn, workerSvc, log)
	pollFn := func(sCtx context.Context) error {
		return poller.Run(sCtx)
	}
	go stream.RegisterWithRetry(ctx, "poller", pollFn, cfg.ConnectRetryDelay, log)

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
				n := localRes.Cleanup()
				if n > 0 {
					log.Debug("reservation: cleaned up local entries", zap.Int("count", n))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// --- END WEEK 2 ADDITION ---

	// --- WEEK 2 ADDITION: Graceful Shutdown ---
	go concurrency.GracefulShutdown(ctx, &wg, cfg.ShutdownTimeout, slogLogger)
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

	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("health server shutdown", zap.Error(err))
	}

	log.Info("worker service stopped cleanly", zap.String("worker_id", workerID))
	return nil
}
