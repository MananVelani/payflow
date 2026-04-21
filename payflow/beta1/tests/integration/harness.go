// Package integration provides an in-process test harness for the PayFlow
// C3 worker service.  It boots the full pipeline (WorkerServiceImpl →
// BankClient → C4 log service → C2 coordinator) using only in-memory or
// test-server replacements so the suite runs with zero external dependencies.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/your-org/payflow/worker/internal/config"
	"github.com/your-org/payflow/worker/internal/concurrency"
	grpctransport "github.com/your-org/payflow/worker/internal/transport/grpc"
	"github.com/your-org/payflow/worker/internal/domain"
	"github.com/your-org/payflow/worker/internal/heartbeat"
	"github.com/your-org/payflow/worker/internal/loadreport"
	"github.com/your-org/payflow/worker/internal/metrics"
	"github.com/your-org/payflow/worker/internal/observability"
	"github.com/your-org/payflow/worker/internal/outbox"
	"github.com/your-org/payflow/worker/internal/reservation"
	"github.com/your-org/payflow/worker/internal/service"
	pblog "github.com/your-org/payflow/worker/proto/log"
	pbworker "github.com/your-org/payflow/worker/proto/worker"
)

const bufSize = 1 << 20 // 1 MiB

// registerMetricsOnce guards prometheus.MustRegister so multiple harness
// instances (e.g. -count=3) never trigger a double-registration panic.
var registerMetricsOnce sync.Once

// ─── Mock C4 ──────────────────────────────────────────────────────────────────

// MockC4 is an in-process implementation of PaymentLogService (C4).
// It records every write and idempotency-check call for test assertions.
type MockC4 struct {
	pblog.UnimplementedPaymentLogServiceServer

	mu             sync.Mutex
	appendEntries  []*pblog.LogEntry
	writeResults   []*pblog.WriteResultRequest
	idempChecks    []string // idempotency keys checked
}

func (m *MockC4) AppendEntry(_ context.Context, e *pblog.LogEntry) (*pblog.AppendResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendEntries = append(m.appendEntries, e)
	return &pblog.AppendResponse{Success: true, LogIndex: int64(len(m.appendEntries))}, nil
}

func (m *MockC4) CheckIdempotency(_ context.Context, req *pblog.IdempotencyRequest) (*pblog.IdempotencyResponse, error) {
	m.mu.Lock()
	m.idempChecks = append(m.idempChecks, req.GetIdempotencyKey())
	m.mu.Unlock()
	// Always report not-found so the pipeline proceeds to the bank.
	return &pblog.IdempotencyResponse{Exists: false}, nil
}

func (m *MockC4) WriteResult(_ context.Context, req *pblog.WriteResultRequest) (*pblog.WriteResultAck, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeResults = append(m.writeResults, req)
	return &pblog.WriteResultAck{Acknowledged: true}, nil
}

// IdempotencyCheckCount returns how many times CheckIdempotency was called.
func (m *MockC4) IdempotencyCheckCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.idempChecks)
}

// ─── Mock C2 ──────────────────────────────────────────────────────────────────

// MockC2 records results delivered by WorkerServiceImpl.
//
// Two delivery paths exist:
//  1. Direct domain path  — WorkerServiceImpl calls ReportDomain (injected as
//     service.ReportResultFunc).  Fails the first domainFailCount calls so
//     that results fall into the outbox.
//  2. Outbox relay path   — after C2 becomes available the outbox relay calls
//     ReportResult (which records the proto-level result).
type MockC2 struct {
	pbworker.UnimplementedWorkerManagementServer

	mu              sync.Mutex
	results         []*domain.PaymentResult // written by both paths
	domainFailCount atomic.Int32
	logger          *zap.Logger
}

// ReportDomain is used as service.ReportResultFunc.
func (m *MockC2) ReportDomain(_ context.Context, r *domain.PaymentResult) error {
	if m.domainFailCount.Load() > 0 {
		m.domainFailCount.Add(-1)
		return fmt.Errorf("mock C2: injected failure")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, r)
	return nil
}

// ReportResult implements the gRPC WorkerManagement server method so the
// outbox relay (which calls it directly) can also record results.
func (m *MockC2) ReportResult(_ context.Context, in *pbworker.TaskResult) (*pbworker.ResultAck, error) {
	r := &domain.PaymentResult{
		TaskID:   in.GetTaskId(),
		WorkerID: in.GetWorkerId(),
	}
	switch s := in.Status.(type) {
	case *pbworker.TaskResult_Success:
		if s.Success {
			r.Status = domain.TaskStatusSuccess
		} else {
			r.Status = domain.TaskStatusFailure
		}
	case *pbworker.TaskResult_ErrorMessage:
		r.Status = domain.TaskStatusFailure
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, r)
	return &pbworker.ResultAck{Acknowledged: true}, nil
}

// RegisterWorker implements the gRPC WorkerManagement server method.
func (m *MockC2) RegisterWorker(ctx context.Context, req *pbworker.RegisterRequest) (*pbworker.RegisterResponse, error) {
	m.logger.Info("C2 received registration",
		zap.String("worker_id", req.WorkerId),
		zap.Int32("capacity", req.ProcessingCapacity))
	return &pbworker.RegisterResponse{Success: true}, nil
}

// WorkerHeartbeat implements the bidi stream for heartbeats and logs pings for the demo.
func (m *MockC2) WorkerHeartbeat(stream pbworker.WorkerManagement_WorkerHeartbeatServer) error {
	for {
		ping, err := stream.Recv()
		if err != nil {
			return err
		}
		m.logger.Warn("C2 received heartbeat",
			zap.String("worker_id", ping.WorkerId),
			zap.Float32("load", ping.Load),
			zap.Int64("tasks", ping.TasksProcessedCount),
			zap.Int64("epoch", ping.Epoch))
		
		err = stream.Send(&pbworker.HeartbeatAck{
			Epoch: ping.Epoch,
		})
		if err != nil {
			return err
		}
	}
}

// SetDomainFailCount programs the mock to return an error for the next n
// domain-level ReportDomain calls, forcing those results into the outbox.
func (m *MockC2) SetDomainFailCount(n int) {
	m.domainFailCount.Store(int32(n))
}

// Results returns a snapshot of all received PaymentResults (both paths).
func (m *MockC2) Results() []*domain.PaymentResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*domain.PaymentResult, len(m.results))
	copy(cp, m.results)
	return cp
}

// ─── Mock Bank HTTP server ─────────────────────────────────────────────────────

// MockBankHandler is an httptest.Server handler that responds to /charge.
// The first failCount requests return HTTP 500; thereafter it returns SUCCESS.
type MockBankHandler struct {
	failCount atomic.Int32
	CallCount atomic.Int32 // exported so tests can read it directly
}

// SetFailCount programs the handler to fail the next n requests.
func (h *MockBankHandler) SetFailCount(n int32) {
	h.failCount.Store(n)
}

func (h *MockBankHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.CallCount.Add(1)
	if h.failCount.Load() > 0 {
		h.failCount.Add(-1)
		http.Error(w, `{"status":"ERROR","message":"injected bank failure"}`, http.StatusInternalServerError)
		return
	}
	key := r.Header.Get("X-Idempotency-Key")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "SUCCESS",
		"txn_ref": "txn-" + key,
	})
}

// ─── Harness ──────────────────────────────────────────────────────────────────

// Harness wires the full C3 worker pipeline in-process and exposes helpers
// for injecting tasks and asserting observable side-effects.
type Harness struct {
	// Exported mocks — tests can interrogate them directly.
	C2          *MockC2
	C4          *MockC4
	BankHandler *MockBankHandler
	Config      *config.Config

	worker      *service.WorkerServiceImpl
	outboxStore outbox.Store
	outboxInst  *outbox.Outbox
	grpcServer  *grpc.Server
	bankServer  *httptest.Server
	lis         *bufconn.Listener
	cancel      context.CancelFunc
	wg          *sync.WaitGroup
	dbPath      string // temporary directory for BadgerDB
}

// NewHarness builds a fully-wired test harness. If cfg is nil, defaults are used.
func NewHarness(t *testing.T, customCfg *config.Config) *Harness {
	t.Helper()

	// Register Prometheus metrics exactly once per process.
	registerMetricsOnce.Do(metrics.Register)

	// ── 1. In-process gRPC layer (bufconn) ───────────────────────────────────
	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()

	// ── 3. Observability (single logger for both C3 and Mocks) ────────────────
	zapLogger, _ := zap.NewDevelopment() 
	
	c2 := &MockC2{logger: zapLogger}
	c4 := &MockC4{}
	pbworker.RegisterWorkerManagementServer(grpcSrv, c2)
	pblog.RegisterPaymentLogServiceServer(grpcSrv, c4)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			// ErrServerStopped is expected on h.Close().
			t.Logf("harness: gRPC server exited: %v", err)
		}
	}()

	// ── Dial bufconn for C4 Log Service ───────────────────────────────────────
	dialFn := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	logClient, err := service.NewLogClientImpl(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialFn),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "dial bufconn for C4")

	// ── Dial bufconn for C2 Heartbeat ──────────────────────────────────────────
	c2Conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialFn),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "dial bufconn for C2 heartbeat")

	// ── 2. Mock bank HTTP server ──────────────────────────────────────────────
	bankHandler := &MockBankHandler{}
	bankServer := httptest.NewServer(bankHandler)

	// ── 3. Observability ─────────────────────────────────────────────────────
	obsLogger := observability.NewLogger(zapLogger)
	obsMetrics := observability.NewMetrics()

	// ── 4. CI-safe Config ─────────────────────────────────────────────────────
	cfg := &config.Config{
		WorkerID:         "test-worker-1",
		BankAPIAddr:      bankServer.URL,
		BankFailRate:     0,   // random failures disabled
		BankLatencyMinMS: 0,   // no artificial latency
		BankLatencyMaxMS: 0,
		MaxConcurrentTasks: 10,
		HeartbeatInterval:  2 * time.Second,
		MaxTaskDuration:    30 * time.Second,
		ShutdownTimeout:    5 * time.Second,
		// Outer retry (ExecuteWithRetry inside WorkerServiceImpl)
		RetryMaxAttempts: 1,               // no outer retry; bank client retries internally
		RetryBaseDelay:   10 * time.Millisecond,
		RetryMaxDelay:    50 * time.Millisecond,
		// Circuit breaker — never trips in tests (require 100 requests first)
		CBMaxRequests:      1000,
		CBFailureThreshold: 1.0,
		CBTimeout:          1 * time.Second,
		// Outbox
		OutboxFlushInterval: 50 * time.Millisecond,
		OutboxMaxSize:       100,
		OutboxDBPath:        "", // Default to memory for most tests
		// Week 3
		RetryTaskMaxAttempts: 3,
		RequireRedis:         false,
	}

	if customCfg != nil {
		if customCfg.MaxConcurrentTasks > 0 {
			cfg.MaxConcurrentTasks = customCfg.MaxConcurrentTasks
		}
		if customCfg.HeartbeatInterval > 0 {
			cfg.HeartbeatInterval = customCfg.HeartbeatInterval
		}
		// ... add more as needed
	}

	// ── 5. Bank client ────────────────────────────────────────────────────────
	bankClientCfg := service.MockBankClientConfig{
		BaseURL:       bankServer.URL,
		FailRate:      0,
		LatencyMinMS:  0,
		LatencyMaxMS:  0,
		MaxAttempts:   5, // inner retry: up to 5 HTTP attempts
		BaseDelayMS:   5,
		HTTPTimeout:   5 * time.Second,
		CBMaxRequests: 1000,
		CBInterval:    1 * time.Second,
		CBTimeout:     1 * time.Second,
		CBMinRequests: 1000, // never trip
	}
	bankClient := service.NewProductionMockBankClient(bankClientCfg, zapLogger, obsMetrics)

	// ── 6. Storage dependencies ───────────────────────────────────────────────
	resStore := reservation.NewLocalStore(5 * time.Minute)

	// Support BadgerDB if requested via environment or a specialized constructor
	var outboxStore outbox.Store
	var dbPath string
	if os.Getenv("USE_BADGER_TEST") == "true" {
		dbPath, _ = os.MkdirTemp("", "c3-outbox-test-*")
		store, err := outbox.NewBadgerStore(dbPath)
		require.NoError(t, err)
		outboxStore = store
	} else {
		outboxStore = outbox.NewMemoryStore()
	}

	// ── 7. Concurrency primitives ─────────────────────────────────────────────
	taskWg := &sync.WaitGroup{}
	sem := concurrency.NewTaskSemaphore(
		cfg.MaxConcurrentTasks,
		outboxStore.(concurrency.LeaseStore), // Both Memory and Badger satisfy this
		cfg.MaxTaskDuration,
		obsMetrics.WorkerSaturation,
		obsMetrics.OrphanedLeaseCount,
		slog.Default(),
	)
	registry := concurrency.NewTaskRegistry()

	// ── 8. Outbox: relay goes directly to MockC2.ReportResult ─────────────────
	ctx, cancel := context.WithCancel(context.Background())

	outboxRelayFn := func(ictx context.Context, r *pbworker.TaskResult) (*pbworker.ResultAck, error) {
		return c2.ReportResult(ictx, r)
	}
	outboxInst := outbox.New(
		outboxRelayFn,
		outboxStore,
		cfg.OutboxFlushInterval,
		cfg.RetryBaseDelay,
		cfg.OutboxMaxSize,
		cfg.MaxTaskDuration,       // maxTaskDuration
		obsMetrics.TasksTotal,     // deadlineCounter (reuse existing CounterVec)
		slog.Default(),
	)
	outboxInst.Start(ctx)

	// --- Week 4: Load reporter ---
	durationRing := heartbeat.NewRingBuffer(100)
	var workerActive atomic.Int64
	var workerProcessed atomic.Int64
	loadRep := loadreport.NewReporter(durationRing, cfg.MaxConcurrentTasks, &workerActive, &workerProcessed)

	// ── 9. Wire WorkerServiceImpl ─────────────────────────────────────────────
	workerImpl := service.NewWorkerServiceImpl(
		bankClient,
		logClient,
		c2.ReportDomain, // domain-level reporter; falls back to outbox on error
		obsLogger,
		cfg,
		resStore,
		outboxInst,
		sem,
		registry,
		taskWg,
		obsMetrics,
		loadRep,
	)

	// ── 10. Start Heartbeat (for live demo) ──────────────────────────────────
	heartbeat := grpctransport.NewHeartbeatClient(
		cfg.WorkerID, 50051, cfg.MaxConcurrentTasks,
		cfg.HeartbeatInterval, workerImpl.Stats, zapLogger,
	)
	go func() {
		// Infinite retry loop similar to main.go
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := heartbeat.RunSession(ctx, c2Conn); err != nil {
					t.Logf("heartbeat session failed: %v", err)
					time.Sleep(1 * time.Second)
				}
			}
		}
	}()

	h := &Harness{
		C2:          c2,
		C4:          c4,
		BankHandler: bankHandler,
		worker:      workerImpl,
		Config:      cfg,
		outboxStore: outboxStore,
		outboxInst:  outboxInst,
		grpcServer:  grpcSrv,
		bankServer:  bankServer,
		lis:         lis,
		cancel:      cancel,
		wg:          taskWg,
		dbPath:      dbPath,
	}
	t.Cleanup(h.Close)
	return h
}

// ─── Harness helpers ──────────────────────────────────────────────────────────

// SendTask calls workerImpl.ExecuteTask in a goroutine, mimicking C2 dispatch.
// Errors are logged but not fatal; use AssertResult to verify outcomes.
func (h *Harness) SendTask(t *testing.T, task *domain.Task) {
	t.Helper()
	go func() {
		if _, err := h.worker.ExecuteTask(context.Background(), task); err != nil {
			t.Logf("SendTask(%s): ExecuteTask returned error: %v", task.TaskID, err)
		}
	}()
}

// AssertResult polls MockC2.Results() until a result matching taskID+status
// appears, or the 5-second deadline passes.
func (h *Harness) AssertResult(t *testing.T, taskID string, status domain.TaskStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range h.C2.Results() {
			if r.TaskID == taskID && r.Status == status {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("AssertResult: timed out waiting for task=%s status=%s (got: %v)",
		taskID, status, h.C2.Results())
}

// AssertResultCount polls until MockC2 holds exactly n results, or times out.
func (h *Harness) AssertResultCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.C2.Results()) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("AssertResultCount: timed out waiting for %d results (got %d)", want, len(h.C2.Results()))
}

// AssertOutboxDepth checks — without polling — that the MemoryStore has exactly
// n pending entries at the moment of the call.
func (h *Harness) AssertOutboxDepth(t *testing.T, want int) {
	t.Helper()
	entries, err := h.outboxStore.Pending(context.Background())
	require.NoError(t, err)
	if len(entries) != want {
		t.Errorf("AssertOutboxDepth: want %d pending entries, got %d", want, len(entries))
	}
}

// WaitOutboxEmpty polls until the MemoryStore has zero pending entries, or the
// 5-second deadline passes.
func (h *Harness) WaitOutboxEmpty(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := h.outboxStore.Pending(context.Background())
		if len(entries) == 0 {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	entries, _ := h.outboxStore.Pending(context.Background())
	t.Errorf("WaitOutboxEmpty: timed out; %d entries remain", len(entries))
}

// TriggerOutboxFlush waits for 3 flush cycles (flushInterval=50ms → 200ms),
// ensuring at least one relay pass has completed.
func (h *Harness) TriggerOutboxFlush(_ *testing.T) {
	time.Sleep(200 * time.Millisecond)
}

// Close tears down the harness: cancels the outbox relay context, stops the
// gRPC server and httptest bank server, then waits for in-flight tasks.
func (h *Harness) Close() {
	h.cancel()
	h.grpcServer.GracefulStop()
	h.bankServer.Close()

	if h.dbPath != "" {
		os.RemoveAll(h.dbPath)
	}

	// Give in-flight ExecuteTask goroutines a moment to finish.
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
	}
}
