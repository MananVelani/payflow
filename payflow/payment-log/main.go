// Package main implements a canonical placeholder HTTP server for PayFlow services.
// Members 1–4 copy this into their service directories and replace with real code in Weeks 2–3.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	pb "payflow/proto/log"
)

var (
	metricLogAppendTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payflow_log_append_total",
		Help: "Total number of log entries appended to C4.",
	})
	metricIdempotencyHit = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payflow_idempotency_hit_total",
		Help: "Total idempotency cache hits (duplicate submissions).",
	})
	metricIdempotencyMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payflow_idempotency_miss_total",
		Help: "Total idempotency cache misses (new submissions).",
	})
	metric2PCPrepared = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payflow_2pc_prepared_total",
		Help: "Total 2PC PREPARE calls (high-value payments).",
	})
	metric2PCCommitted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payflow_2pc_committed_total",
		Help: "Total 2PC COMMIT calls.",
	})
	metric2PCRolledBack = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payflow_2pc_rolledback_total",
		Help: "Total 2PC ROLLBACK calls.",
	})
)

type LogServer struct {
	pb.UnimplementedPaymentLogServiceServer
	store *Store
}

// -------------------- AppendEntry --------------------

func (s *LogServer) AppendEntry(ctx context.Context, req *pb.LogEntry) (*pb.AppendResponse, error) {
	log.Println("[AppendEntry] Saving txn:", req.TxnId)
	seq := s.store.Save(req.TxnId, req)
	metricLogAppendTotal.Inc()
	return &pb.AppendResponse{
		LogIndex: int64(seq),
		Success:  true,
	}, nil
}

// -------------------- Idempotency Check --------------------

func (s *LogServer) CheckIdempotency(ctx context.Context, req *pb.IdempotencyRequest) (*pb.IdempotencyResponse, error) {
	log.Println("[CheckIdempotency] Key:", req.IdempotencyKey)
	exists, txnID, success := s.store.CheckIdempotency(req.IdempotencyKey)
	if exists {
		log.Println("[CheckIdempotency] FOUND -> txn:", txnID)
		metricIdempotencyHit.Inc()
	} else {
		log.Println("[CheckIdempotency] NOT FOUND")
		metricIdempotencyMiss.Inc()
	}
	return &pb.IdempotencyResponse{
		Exists:  exists,
		TxnId:   txnID,
		Success: success,
	}, nil
}

// -------------------- Write Result --------------------

func (s *LogServer) WriteResult(ctx context.Context, req *pb.WriteResultRequest) (*pb.WriteResultAck, error) {
	log.Println("[WriteResult] txn:", req.TxnId, "success:", req.Success)
	s.store.WriteResult(req.IdempotencyKey, req.TxnId, req.Success)
	return &pb.WriteResultAck{Acknowledged: true}, nil
}

// -------------------- 2-Phase Commit --------------------

// HandlePrepare locks a high-value transaction (>$10,000) in the prepared bucket.
func (s *LogServer) HandlePrepare(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	log.Printf("[2PC] PREPARE txn:%s epoch:%d", req.TxnId, req.Epoch)
	ok := s.store.Prepare(req.TxnId)
	if ok {
		metric2PCPrepared.Inc()
	}
	return &pb.TxnResponse{Success: ok}, nil
}

// HandleCommit finalises a prepared high-value transaction.
func (s *LogServer) HandleCommit(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	log.Printf("[2PC] COMMIT txn:%s epoch:%d", req.TxnId, req.Epoch)
	ok := s.store.Commit(req.TxnId)
	if ok {
		metric2PCCommitted.Inc()
	}
	return &pb.TxnResponse{Success: ok}, nil
}

// HandleRollback aborts a prepared high-value transaction.
func (s *LogServer) HandleRollback(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	log.Printf("[2PC] ROLLBACK txn:%s epoch:%d", req.TxnId, req.Epoch)
	ok := s.store.Rollback(req.TxnId)
	if ok {
		metric2PCRolledBack.Inc()
	}
	return &pb.TxnResponse{Success: ok}, nil
}

// -------------------- Get All Pending --------------------

func (s *LogServer) GetAllPending(req *pb.PendingRequest, stream pb.PaymentLogService_GetAllPendingServer) error {
	log.Println("[GetAllPending] Epoch:", req.Epoch)
	results := s.store.GetAllPending(req.Epoch)
	for _, r := range results {
		data, _ := json.Marshal(r)
		var entry pb.LogEntry
		json.Unmarshal(data, &entry)
		if err := stream.Send(&entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *LogServer) GetEpoch(ctx context.Context, req *pb.EmptyRequest) (*pb.EpochResponse, error) {
	epoch := s.store.GetEpoch()
	log.Printf("[GetEpoch] Returned Epoch: %d", epoch)
	return &pb.EpochResponse{Epoch: epoch}, nil
}

func (s *LogServer) SaveEpoch(ctx context.Context, req *pb.EpochRequest) (*pb.WriteResultAck, error) {
	s.store.SaveEpoch(req.Epoch)
	log.Printf("[SaveEpoch] Persisted Epoch: %d", req.Epoch)
	return &pb.WriteResultAck{Acknowledged: true}, nil
}

// -------------------- Main --------------------

func main() {
	log.Println("Starting C4 Payment Log Service...")

	store := NewStore()

	// Start Prometheus metrics endpoint on :2112
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Println("[C4] Prometheus metrics at :2112/metrics")
		if err := http.ListenAndServe(":2112", mux); err != nil {
			log.Printf("[C4] metrics server error: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", ":50054")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPaymentLogServiceServer(grpcServer, &LogServer{store: store})

	log.Println("C4 Payment Log running on :50054")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
