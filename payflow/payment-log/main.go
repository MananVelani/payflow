package main

import (
	"context"
	"encoding/json"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "payflow/proto/log"
)

type LogServer struct {
	pb.UnimplementedPaymentLogServiceServer
	store *Store
}

// -------------------- AppendEntry --------------------

func (s *LogServer) AppendEntry(ctx context.Context, req *pb.LogEntry) (*pb.AppendResponse, error) {
	log.Println("[AppendEntry] Saving txn:", req.TxnId)

	// Save transaction log
	s.store.Save(req.TxnId, req)

	return &pb.AppendResponse{
		LogIndex: 1, // TODO: replace with real index later
		Success:  true,
	}, nil
}

// -------------------- Idempotency Check --------------------

func (s *LogServer) CheckIdempotency(ctx context.Context, req *pb.IdempotencyRequest) (*pb.IdempotencyResponse, error) {
	log.Println("[CheckIdempotency] Key:", req.IdempotencyKey)

	exists, txnID, success := s.store.CheckIdempotency(req.IdempotencyKey)

	if exists {
		log.Println("[CheckIdempotency] FOUND -> txn:", txnID)
	} else {
		log.Println("[CheckIdempotency] NOT FOUND")
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

	return &pb.WriteResultAck{
		Acknowledged: true,
	}, nil
}

// -------------------- Get All Pending --------------------
func (s *LogServer) GetAllPending(req *pb.PendingRequest, stream pb.PaymentLogService_GetAllPendingServer) error {

	log.Println("[GetAllPending] Epoch:", req.Epoch)

	results := s.store.GetAllPending(req.Epoch)

	for _, r := range results {
		// Convert map → LogEntry
		data, _ := json.Marshal(r)

		var entry pb.LogEntry
		json.Unmarshal(data, &entry)

		if err := stream.Send(&entry); err != nil {
			return err
		}
	}

	return nil
}

// -------------------- Main --------------------

func main() {
	log.Println("Starting C4 Payment Log Service...")

	// Initialize storage
	store := NewStore()

	// Start listener
	lis, err := net.Listen("tcp", ":50054")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register service
	pb.RegisterPaymentLogServiceServer(grpcServer, &LogServer{
		store: store,
	})

	log.Println("C4 Payment Log running on :50054")

	// Start server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}