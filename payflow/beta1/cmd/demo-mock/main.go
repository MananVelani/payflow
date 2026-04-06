package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pblog "github.com/your-org/payflow/worker/proto/log"
	pbworker "github.com/your-org/payflow/worker/proto/worker"
)

// --- Color Constants for Evaluator Visuals ---
const (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	Gray   = "\033[37m"
	White  = "\033[97m"
)

func main() {
	fmt.Printf("%s================================================================%s\n", Blue, Reset)
	fmt.Printf("%s   PAYFLOW C3 WORKER - LIVE EVALUATOR DEMO (UNIFIED MOCK)   %s\n", Blue, Reset)
	fmt.Printf("%s================================================================%s\n\n", Blue, Reset)

	mock := &UnifiedMockServer{}

	// 1. Start gRPC Server (Coordinator + Log Service)
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Printf("%s[FATAL]%s Failed to listen on :50051: %v\n", Red, Reset, err)
		return
	}
	s := grpc.NewServer()
	pbworker.RegisterWorkerManagementServer(s, mock)
	pblog.RegisterPaymentLogServiceServer(s, mock)
	reflection.Register(s)

	fmt.Printf("%s[INFO]%s gRPC Coordinator/Log server listening on :50051\n", Green, Reset)
	go s.Serve(lis)

	// 2. Start HTTP Bank Server
	http.HandleFunc("/charge", mock.HandleBankCharge)
	fmt.Printf("%s[INFO]%s HTTP Mock Bank API listening on :8080\n", Green, Reset)
	go http.ListenAndServe(":8080", nil)

	fmt.Printf("\n%s[READY]%s Waiting for Worker to register...%s\n\n", Yellow, White, Reset)
	
	// Keep main alive
	select {}
}

// UnifiedMockServer implements everything the worker needs.
type UnifiedMockServer struct {
	pbworker.UnimplementedWorkerManagementServer
	pblog.UnimplementedPaymentLogServiceServer

	mu sync.Mutex
}

// --- Coordinator (C2) Logic ---

func (s *UnifiedMockServer) RegisterWorker(ctx context.Context, req *pbworker.RegisterRequest) (*pbworker.RegisterResponse, error) {
	fmt.Printf("%s[C2]%s %sNew Worker Registration!%s\n", Purple, Reset, Green, Reset)
	fmt.Printf("    ID: %s%s%s | Capacity: %d | Epoch: %d\n", Cyan, req.WorkerId, Reset, req.ProcessingCapacity, req.Epoch)
	
	return &pbworker.RegisterResponse{Success: true}, nil
}

func (s *UnifiedMockServer) WorkerHeartbeat(stream pbworker.WorkerManagement_WorkerHeartbeatServer) error {
	fmt.Printf("%s[C2]%s %sHeartbeat stream established.%s\n", Purple, Reset, Green, Reset)
	for {
		ping, err := stream.Recv()
		if err != nil {
			fmt.Printf("%s[C2]%s Heartbeat stream closed: %v\n", Purple, Reset, err)
			return err
		}
		
		fmt.Printf("%s[PING]%s from %s%s%s | Load: %.2f | Tasks: %d | Avg: %dms\n", 
			Gray, Reset, Cyan, ping.WorkerId, Reset, ping.Load, ping.TasksProcessedCount, ping.AvgTaskDurationMs)
		
		err = stream.Send(&pbworker.HeartbeatAck{
			Epoch:    ping.Epoch,
		})
		if err != nil {
			return err
		}
	}
}

func (s *UnifiedMockServer) ReportResult(ctx context.Context, res *pbworker.TaskResult) (*pbworker.ResultAck, error) {
	status := "SUCCESS"
	color := Green
	if _, ok := res.Status.(*pbworker.TaskResult_ErrorMessage); ok {
		status = "FAILURE"
		color = Red
	}

	fmt.Printf("\n%s[C2]%s %sRESULT RECEIVED!%s for Task: %s%s%s Status: %s%s%s\n", 
		Purple, Reset, Yellow, Reset, Cyan, res.TaskId, Reset, color, status, Reset)
	return &pbworker.ResultAck{Acknowledged: true}, nil
}

// --- Payment Log (C4) Logic ---

func (s *UnifiedMockServer) CheckIdempotency(ctx context.Context, req *pblog.IdempotencyRequest) (*pblog.IdempotencyResponse, error) {
	fmt.Printf("%s[C4]%s Idempotency check for key: %s%s%s -> %sNOT_FOUND%s\n", 
		Blue, Reset, Yellow, req.IdempotencyKey, Reset, Green, Reset)
	// Always return not found for demo to force bank call
	return &pblog.IdempotencyResponse{Exists: false}, nil
}

// --- Mock Bank Logic ---

func (s *UnifiedMockServer) HandleBankCharge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		IdempotencyKey string  `json:"idempotency_key"`
		Amount         float64 `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	fmt.Printf("%s[BANK]%s %sReceived Charge Request!%s Key: %s%s%s Amount: $%.2f\n", 
		Red, Reset, Cyan, Reset, Yellow, payload.IdempotencyKey, Reset, payload.Amount)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "SUCCESS",
		"txn_ref": "TXN-" + strings.ToUpper(payload.IdempotencyKey[:8]),
	})
}
