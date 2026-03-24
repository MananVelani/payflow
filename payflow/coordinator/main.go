package main

import (
	"context"
	"flag"
	"log"
	"fmt"
	"net"
	"sync"
	"time"

	coordPb "payflow/proto/coordinator"
	paymentPb "payflow/proto/payment"
	workerPb "payflow/proto/worker"
	logPb "payflow/proto/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

// Hardcoded cluster addresses for local testing
// Hardcoded cluster addresses for local IPv4 testing
var clusterPeers = map[string]string{
	"coordinator-1": "127.0.0.1:50051",
	"coordinator-2": "127.0.0.1:50052",
	"coordinator-3": "127.0.0.1:50053",
}

// CoordinatorNode implements all 3 required server interfaces
type CoordinatorNode struct {
	coordPb.UnimplementedCoordinatorClusterServer
	paymentPb.UnimplementedPaymentGatewayServer
	workerPb.UnimplementedWorkerManagementServer

	ID      string
	State   string
	Epoch   int
	Workers map[string]time.Time

	leaderTimeout	time.Duration
	resetTimer		chan bool

	TaskQueue		chan *workerPb.TaskAssignment

	mu      sync.Mutex
}

// ---------------------------------------------------------
// 1. COORDINATOR CLUSTER SERVICE (Bully Algorithm)
// ---------------------------------------------------------

func (c *CoordinatorNode) Election(ctx context.Context, req *coordPb.ElectionMessage) (*coordPb.ElectionResponse, error) {
	log.Printf("[Node %s] Received ELECTION from %s with epoch %d", c.ID, req.CandidateId, req.Epoch)

	if req.CandidateId < c.ID {
		go c.triggerElection() // Step down and start our own election if we have a higher ID
	}

	return &coordPb.ElectionResponse{
		Epoch: int64(c.Epoch),
		Ok:    true,
	}, nil
}

// AnnounceCoordinator handles the COORDINATOR victory message from a higher node
func (c *CoordinatorNode) AnnounceCoordinator(ctx context.Context, req *coordPb.CoordinatorMessage) (*coordPb.AckResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// log.Printf("[Node %s] Received COORDINATOR announcement from %s for Epoch %d", c.ID, req.LeaderId, req.Epoch)

	if c.State != "FOLLOWER" || int(req.Epoch) > c.Epoch {
		log.Printf(("[Node %s] Bending knee  to Leader %s for Epoch %d") , c.ID, req.LeaderId, req.Epoch)
	}

	if req.LeaderId < c.ID {
		log.Printf("[Node %s] 😤 Leader %s is smaller than me! Initiating takeover...", c.ID, req.LeaderId)
		// We launch this in a goroutine so we don't block the current request
		go c.triggerElection()
		
		return &coordPb.AckResponse{
			Acknowledged: false, // Do NOT bend the knee!
		}, nil
	}
	
	// Step down and accept the new leader
	c.State = "FOLLOWER"
	
	// Sync our epoch with the leader's epoch
	if int(req.Epoch) > c.Epoch {
		c.Epoch = int(req.Epoch)
	}
	
	// Reset the timeout timer so we don't accidentally trigger another election
	// (The non-blocking channel send we built earlier!)
	select {
		case c.resetTimer <- true:
		default:
	}

	return &coordPb.AckResponse{
		Acknowledged: true,
	}, nil
}

// ---------------------------------------------------------
// 2. PAYMENT GATEWAY SERVICE (From C1 Gateway)
// ---------------------------------------------------------

func (c *CoordinatorNode) SubmitTask(ctx context.Context, req *paymentPb.SubmitTaskRequest) (*paymentPb.SubmitTaskResponse, error) {
	c.mu.Lock()
	isLeader := c.State == "LEADER"
	c.mu.Unlock()

	// 1. Only the LEADER is allowed to accept new tasks
	if !isLeader {
		return nil, fmt.Errorf("I am not the leader. Please route to the current leader")
	}

	log.Printf("[LEADER %s] Received task for amount: %f", c.ID, req.Amount)

	// 2. Generate a dynamic Transaction ID
	txnID := "txn-" + fmt.Sprintf("%d", time.Now().UnixMilli())

	// 3. --- FORWARD TO C4 (Payment Log Service) ---
	conn, err := grpc.NewClient("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to Payment Log Service: %v", err)
	} else {
		defer conn.Close()
		logClient := logPb.NewPaymentLogServiceClient(conn)

		logRes, err := logClient.AppendEntry(ctx, &logPb.LogEntry{
			Epoch:   int64(c.Epoch),
			TxnId:   txnID,
			State:   "QUEUED",
			Payload: fmt.Sprintf(`{"amount": %f, "merchant": "%s"}`, req.Amount, req.MerchantId),
		})

		if err != nil {
			log.Printf("C4 AppendEntry failed: %v", err)
		} else {
			log.Printf("Successfully wrote to C4 log! Index: %d", logRes.LogIndex)
		}
	}

	// 4. --- PUSH TO WORKER QUEUE ---
	newTask := &workerPb.TaskAssignment{
		Epoch:          int64(c.Epoch),
		TaskId:         txnID,
		IdempotencyKey: req.IdempotencyKey,
		Amount:         req.Amount,
	}

	c.TaskQueue <- newTask
	log.Printf("[LEADER %s] Task %s added to queue. Queue size: %d", c.ID, newTask.TaskId, len(c.TaskQueue))

	// 5. Return success to the Gateway
	return &paymentPb.SubmitTaskResponse{
		TxnId: txnID,
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

	c.Workers[req.WorkerId] = time.Now()
	log.Printf("[LEADER %s] Registered worker: %s", c.ID, req.WorkerId)

	return &workerPb.RegisterResponse{
		Success: true,
	}, nil
}

// Note: You will eventually need to implement Heartbeat, PollTasks, and ReportResult here too!

// Heartbeat receives periodic health checks from workers 
func (c *CoordinatorNode) Heartbeat(ctx context.Context, req *workerPb.HeartbeatRequest) (*workerPb.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.State == "LEADER" {
		_, exists := c.Workers[req.WorkerId]
		if !exists {
			log.Printf("[LEADER %s] 👋 New worker joined the pool: %s", c.ID, req.WorkerId)
		}
		c.Workers[req.WorkerId] = time.Now() // Update last seen timestamp
	}

	return &workerPb.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// startLeaderHeartbeat continuously suppresses follower elections
func (c *CoordinatorNode) startLeaderHeartbeat() {
	log.Printf("[LEADER %s] 🫀 Starting heartbeat broadcaster...", c.ID)
	
	for {
		c.mu.Lock()
		state := c.State
		c.mu.Unlock()

		// If we ever step down (e.g., a bigger node joins), kill this loop!
		if state != "LEADER" {
			log.Printf("[Node %s] No longer leader. Stopping heartbeats.", c.ID)
			return 
		}

		// Broadcast our leadership to reset all follower timers
		c.broadcastCoordinator()

		// Sleep for 2 seconds (must be strictly less than the 5-second follower timeout!)
		time.Sleep(2 * time.Second)
	}
}

// ReportResult handles the outcome of a payment from a worker 
// ReportResult handles the outcome of a payment from a worker
func (c *CoordinatorNode) ReportResult(ctx context.Context, req *workerPb.TaskResult) (*workerPb.ResultAck, error) {
	c.mu.Lock()
	isLeader := c.State == "LEADER"
	c.mu.Unlock()

	if !isLeader {
		return nil, fmt.Errorf("I am not the leader. Please route to the current leader")
	}

	// 1. Determine if the worker succeeded or failed
	isSuccess := req.GetSuccess()
	if !isSuccess {
		log.Printf("[LEADER %s] ❌ Worker %s failed task %s. Error: %s", c.ID, req.WorkerId, req.TaskId, req.GetErrorMessage())
	} else {
		log.Printf("[LEADER %s] ✅ Worker %s successfully completed task %s", c.ID, req.WorkerId, req.TaskId)
	}

	// 2. --- FORWARD RESULT TO C4 (Payment Log Service) ---
	conn, err := grpc.NewClient("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[LEADER %s] Failed to connect to Payment Log Service to record result: %v", c.ID, err)
	} else {
		defer conn.Close()
		logClient := logPb.NewPaymentLogServiceClient(conn)

		// Call the WriteResult RPC defined in log.proto
		_, err := logClient.WriteResult(ctx, &logPb.WriteResultRequest{
			TxnId:   req.TaskId,
			Success: isSuccess,
		})

		if err != nil {
			log.Printf("[LEADER %s] C4 WriteResult failed: %v", c.ID, err)
		} else {
			log.Printf("[LEADER %s] Permanently recorded task %s outcome to C4 database.", c.ID, req.TaskId)
		}
	}

	// 3. Acknowledge the worker
	return &workerPb.ResultAck{
		Acknowledged: true,
	}, nil
}

// PollTasks is a SERVER STREAMING endpoint where the coordinator pushes tasks to the worker 
// Notice there is no return object, only an error return.
func (c *CoordinatorNode) PollTasks(req *workerPb.PollRequest, stream workerPb.WorkerManagement_PollTasksServer) error {
	log.Printf("[LEADER %s] Worker %s started polling for tasks", c.ID, req.WorkerId)
	
	// For Week 1 Integration, we just send one dummy task and close the stream
	for task := range c.TaskQueue {
		log.Printf("[LEADER %s] Dispatching task %s to worker %s", c.ID, task.TaskId, req.WorkerId)
		
		// stream.Send() pushes the data down the open TCP connection
		if err := stream.Send(task); err != nil {
			log.Printf("Failed to send task to worker %s: %v", req.WorkerId, err)
			c.TaskQueue <- task // Re-enqueue the task for another worker to pick up
			return err
		}
	}
	
	// Returning nil tells gRPC "I am done streaming, you can close the connection"
	// In Week 2, this will be an infinite loop listening to a Go channel!
	return nil 
}

// StartLeaderMonitor runs continuously in the background.
func (c *CoordinatorNode) StartLeaderMonitor() {
	// The 'go' keyword spins this off into its own concurrent thread
	go func() {
		timer := time.NewTimer(c.leaderTimeout)
		for {
			select {
			case <-timer.C:
				// The timer hit 0. The leader is presumed DEAD.
				c.triggerElection()
				
				// Reset the timer so we don't spam elections endlessly
				timer.Reset(c.leaderTimeout)
				
			case <-c.resetTimer:
				// We got a ping from the leader! Reset the countdown.
				if !timer.Stop() {
					// Drain the channel to prevent memory leaks if it already fired
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(c.leaderTimeout)
			}
		}
	}()
}

// triggerElection safely updates the state and prepares to fight for leadership
func (c *CoordinatorNode) triggerElection() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If we are already the leader, we don't need to overthrow ourselves
	if c.State == "LEADER" {
		return
	}

	// Transition to CANDIDATE and increment our Epoch [cite: 174]
	c.State = "CANDIDATE"
	c.Epoch++ 
	
	log.Printf("[Node %s] 🚨 Leader timeout! Transitioned to CANDIDATE for Epoch %d", c.ID, c.Epoch)
	
	// Next step: Broadcast ELECTION message to higher IDs!
	go c.broadcastElection()
}

// broadcastElection sends an ELECTION message to all nodes with a higher ID
func (c *CoordinatorNode) broadcastElection() {
	higherNodes := make(map[string]string)
	
	// 1. Find nodes with a strictly higher ID string
	for id, addr := range clusterPeers {
		if id > c.ID {
			higherNodes[id] = addr
		}
	}

	// 2. Base Case: If there are no higher nodes, we win instantly!
	if len(higherNodes) == 0 {
		log.Printf("[Node %s] I have the highest ID. I win the election instantly!", c.ID)
		c.becomeLeader()
		return
	}

	log.Printf("[Node %s] Broadcasting ELECTION to %d higher nodes...", c.ID, len(higherNodes))

	// 3. Concurrent RPC calls using a Go channel to collect responses
	responses := make(chan bool, len(higherNodes))

	for id, addr := range higherNodes {
		go func(peerID, peerAddr string) {
			// 2-second timeout so a dead node doesn't freeze the election
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				responses <- false // Could not connect
				return
			}
			defer conn.Close()

			client := coordPb.NewCoordinatorClusterClient(conn)
			
			res, err := client.Election(ctx, &coordPb.ElectionMessage{
				Epoch:       int64(c.Epoch),
				CandidateId: c.ID,
			})

			// If the RPC succeeded and the node said OK
			if err == nil && res.Ok {
				log.Printf("[Node %s] Received OK from higher node: %s", c.ID, peerID)
				responses <- true
			} else {
				responses <- false
			}
		}(id, addr)
	}

	// 4. Wait for all responses
	gotOk := false
	for i := 0; i < len(higherNodes); i++ {
		if <-responses {
			gotOk = true // Someone higher is alive!
		}
	}

	// 5. Evaluate the Election Results
	if !gotOk {
		log.Printf("[Node %s] No higher nodes responded OK. I am the new LEADER!", c.ID)
		c.becomeLeader()
	} else {
		log.Printf("[Node %s] A higher node is alive. Stepping down to FOLLOWER.", c.ID)
		c.mu.Lock()
		c.State = "FOLLOWER"
		c.mu.Unlock()
	}
}

// broadcastCoordinator tells all other nodes to bend the knee
func (c *CoordinatorNode) broadcastCoordinator() {
	// log.Printf("[LEADER %s] 👑 Announcing victory to the cluster for Epoch %d...", c.ID, c.Epoch)

	for id, addr := range clusterPeers {
		// Don't send the announcement to ourselves
		if id == c.ID {
			continue 
		}

		// Fire off a concurrent gRPC call to each peer
		go func(peerID, peerAddr string) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return // Peer is likely down, which is fine
			}
			defer conn.Close()

			client := coordPb.NewCoordinatorClusterClient(conn)
			
			_, err = client.AnnounceCoordinator(ctx, &coordPb.CoordinatorMessage{
				Epoch:    int64(c.Epoch),
				LeaderId: c.ID,
			})

			/*
			if err == nil {
				log.Printf("[LEADER %s] Node %s acknowledged our leadership.", c.ID, peerID)
			} else {
				// We added '%v' and 'err' here to expose the exact failure reason
				log.Printf("[LEADER %s] Node %s unreachable during announcement: %v", c.ID, peerID, err)
			}
			*/
		}(id, addr)
	}
}

// startWorkerMonitor sweeps the worker pool to detect crashed containers
func (c *CoordinatorNode) startWorkerMonitor() {
	log.Printf("[LEADER %s] 🕵️ Starting worker heartbeat monitor...", c.ID)
	
	for {
		c.mu.Lock()
		state := c.State
		c.mu.Unlock()

		if state != "LEADER" {
			log.Printf("[Node %s] Stepped down. Stopping worker monitor.", c.ID)
			return 
		}

		c.mu.Lock()
		now := time.Now()
		
		for workerID, lastSeen := range c.Workers {
			// If it has been more than 6 seconds since their last ping...
			if now.Sub(lastSeen) > 6*time.Second {
				log.Printf("[LEADER %s] 💀 Worker %s missed 3 heartbeats! Marking as DEAD.", c.ID, workerID)
				
				delete(c.Workers, workerID)
				
				log.Printf("[LEADER %s] TODO: Reassign incomplete tasks for %s", c.ID, workerID)
			}
		}
		c.mu.Unlock()

		time.Sleep(2 * time.Second)
	}
}

// becomeLeader locks in the victory and changes the state
func (c *CoordinatorNode) becomeLeader() {
	c.mu.Lock()
	c.State = "LEADER"
	c.mu.Unlock()
	
	c.broadcastCoordinator()

	log.Printf("[LEADER %s] 👑 Won the election! Operating in Epoch %d", c.ID, c.Epoch)
	log.Printf("[LEADER %s] TODO: Call C4.GetAllPending() to rebuild the task queue", c.ID)

	go c.startLeaderHeartbeat()
	go c.startWorkerMonitor()
}

func main() {
	idFlag := flag.String("id", "coordinator-1", "The ID of the coordinator node")
	portFlag := flag.String("port", ":50051", "The port to run the gRPC server on")
	flag.Parse() // Parse the flags from the terminal

	node := &CoordinatorNode{
		ID:      *idFlag,
		State:   "FOLLOWER",
		Epoch:   1,
		Workers: make(map[string]time.Time),
		leaderTimeout: 5 * time.Second,
		resetTimer: make(chan bool, 1),
		TaskQueue:	make(chan *workerPb.TaskAssignment, 100),
	}

	node.StartLeaderMonitor()

	lis, err := net.Listen("tcp", *portFlag)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register all three services to the same physical gRPC server
	coordPb.RegisterCoordinatorClusterServer(grpcServer, node)
	paymentPb.RegisterPaymentGatewayServer(grpcServer, node)
	workerPb.RegisterWorkerManagementServer(grpcServer, node)

	reflection.Register(grpcServer)

	log.Printf("Coordinator %s starting in %s state on port %s...", node.ID, node.State, *portFlag)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}