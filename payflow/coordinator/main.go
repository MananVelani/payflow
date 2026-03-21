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
	"google.golang.org/grpc/reflection"
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

// Heartbeat receives periodic health checks from workers 
func (c *CoordinatorNode) Heartbeat(ctx context.Context, req *workerPb.HeartbeatRequest) (*workerPb.HeartbeatResponse, error) {
	// In Week 2, we will use this to reset a timeout timer. 
	// For now, just log it.
	log.Printf("[LEADER %s] Received heartbeat from worker %s. Load: %d", c.ID, req.WorkerId, req.CurrentLoad)
	
	return &workerPb.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// ReportResult handles the outcome of a payment from a worker 
func (c *CoordinatorNode) ReportResult(ctx context.Context, req *workerPb.TaskResult) (*workerPb.ResultAck, error) {
	// In Week 2, we will forward this result to C4 (Payment Log Service).
	log.Printf("[LEADER %s] Received result for task %s from worker %s", c.ID, req.TaskId, req.WorkerId)
	
	return &workerPb.ResultAck{
		Acknowledged: true,
	}, nil
}

// PollTasks is a SERVER STREAMING endpoint where the coordinator pushes tasks to the worker 
// Notice there is no return object, only an error return.
func (c *CoordinatorNode) PollTasks(req *workerPb.PollRequest, stream workerPb.WorkerManagement_PollTasksServer) error {
	log.Printf("[LEADER %s] Worker %s started polling for tasks", c.ID, req.WorkerId)
	
	// For Week 1 Integration, we just send one dummy task and close the stream
	dummyTask := &workerPb.TaskAssignment{
		Epoch:          int64(c.Epoch),
		TaskId:         "dummy-txn-12345",
		IdempotencyKey: "idem-999",
		Amount:         150.00,
	}
	
	// stream.Send() pushes the data down the open TCP connection
	if err := stream.Send(dummyTask); err != nil {
		log.Printf("Failed to send task to worker %s: %v", req.WorkerId, err)
		return err
	}
	
	// Returning nil tells gRPC "I am done streaming, you can close the connection"
	// In Week 2, this will be an infinite loop listening to a Go channel!
	return nil 
}

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

	reflection.Register(grpcServer)

	log.Printf("Coordinator %s starting in %s state on port %s...", node.ID, node.State, port)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}