package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "payflow/proto/log"
)

type LogServer struct {
	pb.UnimplementedPaymentLogServiceServer
	store *Store
}

func (s *LogServer) AppendEntry(ctx context.Context, req *pb.LogEntry) (*pb.AppendResponse, error) {

	log.Println("Saving txn:", req.TxnId)

	// Save to DB
	s.store.Save(req.TxnId, req)

	return &pb.AppendResponse{
		LogIndex: 1,
		Success:  true,
	}, nil
}

func (s *LogServer) CheckIdempotency(ctx context.Context, req *pb.IdempotencyRequest) (*pb.IdempotencyResponse, error) {

	exists, txnID, success := s.store.CheckIdempotency(req.IdempotencyKey)

	return &pb.IdempotencyResponse{
		Exists:  exists,
		TxnId:   txnID,
		Success: success,
	}, nil
}

func (s *LogServer) WriteResult(ctx context.Context, req *pb.WriteResultRequest) (*pb.WriteResultAck, error) {

	s.store.WriteResult(req.IdempotencyKey, req.TxnId, req.Success)

	return &pb.WriteResultAck{
		Acknowledged: true,
	}, nil
}

func main() {
	// ✅ FIX: move here
	store := NewStore()

	lis, err := net.Listen("tcp", ":50054")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterPaymentLogServiceServer(grpcServer, &LogServer{
		store: store,
	})

	log.Println("C4 Payment Log running on :50054")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}