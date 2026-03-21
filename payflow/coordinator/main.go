package main

import (
	"context"
	"log"
	"net"
	"sync"

	coordPb "payflow/proto/coordinator"
	paymentPb "payflow/proto/payment"
	workerPb "payflow/proto/worker"
	logPb "payflow/proto/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// CoordinatorNode implements all 3 required server interfaces
type CoordinatorNode struct {
	coordPb.UnimplementedCoordinatorClusterServer
	paymentPb.UnimplementedPaymentGatewayServer
	workerPb.UnimplementedWorkerManagementServer

	ID      string
	State   string
	Epoch   int
	Workers map[string]bool
	mu      sync.Mutex
}

// ---------------------------------------------------------
// 1. COORDINATOR CLUSTER SERVICE (Bully Algorithm)
// ---------------------------------------------------------

func (c *CoordinatorNode) Election(ctx context.Context, req *coordPb.ElectionMessage) (*coordPb.ElectionResponse, error) {
	log.Printf("[Node %s] Received ELECTION from %s with epoch %d", c.ID, req.CandidateId, req.Epoch)
	return &coordPb.ElectionResponse{
		Epoch: int64(c.Epoch),
		Ok:    true,
	}, nil
}

func (c *CoordinatorNode) AnnounceCoordinator(ctx context.Context, req *coordPb.CoordinatorMessage) (*coordPb.AckResponse, error) {
	log.Printf("[Node %s] Received COORDINATOR announcement from %s", c.ID, req.LeaderId)
	return &coordPb.AckResponse{
		Acknowledged: true,
	}, nil
}

// ---------------------------------------------------------
// 2. PAYMENT GATEWAY SERVICE (From C1 Gateway)
// ---------------------------------------------------------

func (c *CoordinatorNode) SubmitTask(ctx context.Context, req *paymentPb.SubmitTaskRequest) (*paymentPb.SubmitTaskResponse, error) {
	log.Printf("[LEADER %s] Received task for amount: %f", c.ID, req.Amount)
	
	// --- NEW: Forward the task to C4 (Payment Log Service) ---
	
	// 1. Dial the C4 container (Member 5 should map this to port 50054 in docker-compose)
	conn, err := grpc.NewClient("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to Payment Log Service: %v", err)
	} else {
		defer conn.Close()
		logClient := logPb.NewPaymentLogServiceClient(conn)

		// 2. Create the LogEntry and call AppendEntry
		logRes, err := logClient.AppendEntry(ctx, &logPb.LogEntry{
			Epoch:   int64(c.Epoch),
			TxnId:   "dummy-txn-12345",
			State:   "QUEUED",
			Payload: "dummy payment payload",
		})

		if err != nil {
			log.Printf("C4 AppendEntry failed: %v", err)
		} else {
			log.Printf("Successfully wrote to C4 log! Index: %d", logRes.LogIndex)
		}
	}
	// ---------------------------------------------------------

	// 3. Return response to Gateway
	return &paymentPb.SubmitTaskResponse{
		TxnId: "dummy-txn-12345",
		Result: &paymentPb.SubmitTaskResponse_Success{
			Success: true,
		},
	}, nil
}

// ---------------------------------------------------------
// 3. WORKER MANAGEMENT SERVICE (From C3 Workers)
// ---------------------------------------------------------

func (c *CoordinatorNode) RegisterWorker(ctx context.Context, req *workerPb.RegisterRequest) (*workerPb.RegisterResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Workers[req.WorkerId] = true
	log.Printf("[LEADER %s] Registered worker: %s", c.ID, req.WorkerId)

	return &workerPb.RegisterResponse{
		Success: true,
	}, nil
}

// Note: You will eventually need to implement Heartbeat, PollTasks, and ReportResult here too!

func main() {
	node := &CoordinatorNode{
		ID:      "coordinator-1",
		State:   "LEADER",
		Epoch:   1,
		Workers: make(map[string]bool),
	}

	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register all three services to the same physical gRPC server
	coordPb.RegisterCoordinatorClusterServer(grpcServer, node)
	paymentPb.RegisterPaymentGatewayServer(grpcServer, node)
	workerPb.RegisterWorkerManagementServer(grpcServer, node)

	log.Printf("Coordinator %s starting in %s state on port %s...", node.ID, node.State, port)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}