package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	coordPb "payflow/proto/coordinator"
	logPb "payflow/proto/log"
	paymentPb "payflow/proto/payment"
	workerPb "payflow/proto/worker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

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
	Epoch   int64
	Workers map[string]time.Time

	leaderTimeout time.Duration
	resetTimer    chan bool

	TaskQueue chan *workerPb.TaskAssignment
	InFlight  map[string]*workerPb.TaskAssignment

	mu sync.Mutex

	c4Client    logPb.PaymentLogServiceClient
	peerClients map[string]coordPb.CoordinatorClusterClient // NEW: The connection pool
}

// ---------------------------------------------------------
// 1. COORDINATOR CLUSTER SERVICE (Bully Algorithm)
// ---------------------------------------------------------

func parseNodeID(id string) int {
	parts := strings.Split(id, "-")
	if len(parts) == 2 {
		num, err := strconv.Atoi(parts[1])
		if err == nil {
			return num
		}
	}
	return 0 // Fallback if parsing fails
}

func (c *CoordinatorNode) Election(ctx context.Context, req *coordPb.ElectionMessage) (*coordPb.ElectionResponse, error) {
	c.mu.Lock()
	currentEpoch := c.Epoch
	c.mu.Unlock()

	// CRITICAL FIX 1: Ignore stale election requests from older epochs
	if req.Epoch < currentEpoch {
		log.Printf("[Node %s] Rejecting stale ELECTION from %s (Epoch %d < %d)", c.ID, req.CandidateId, req.Epoch, currentEpoch)
		return &coordPb.ElectionResponse{
			Epoch: currentEpoch,
			Ok:    false, // Do not agree to the election!
		}, nil
	}

	log.Printf("[Node %s] Received ELECTION from %s with epoch %d", c.ID, req.CandidateId, req.Epoch)

	// CRITICAL FIX 2: Mathematical ID comparison instead of string comparison
	myID := parseNodeID(c.ID)
	candidateID := parseNodeID(req.CandidateId)

	if candidateID < myID {
		go c.triggerElection() // Step down and start our own election to crush them
	}

	return &coordPb.ElectionResponse{
		Epoch: currentEpoch,
		Ok:    true,
	}, nil
}

// AnnounceCoordinator handles the COORDINATOR victory message from a higher node
func (c *CoordinatorNode) AnnounceCoordinator(ctx context.Context, req *coordPb.CoordinatorMessage) (*coordPb.AckResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if req.Epoch < c.Epoch {
		log.Printf("[Node %s] Ignoring stale COORDINATOR announcement from %s (Epoch %d < %d)", c.ID, req.LeaderId, req.Epoch, c.Epoch)
		return &coordPb.AckResponse{
			Acknowledged: false,
		}, nil
	}

	if c.State != "FOLLOWER" || req.Epoch > c.Epoch {
		log.Printf(("[Node %s] Acknowledging Leader %s for Epoch %d"), c.ID, req.LeaderId, req.Epoch)
	}

	myId := parseNodeID(c.ID)
	leaderId := parseNodeID(req.LeaderId)

	if leaderId < myId {
		log.Printf("[Node %s] Rejecting leader %s (lower ID). Initiating election...", c.ID, req.LeaderId)
		// We launch this in a goroutine so we don't block the current request
		go c.triggerElection()

		return &coordPb.AckResponse{
			Acknowledged: false,
		}, nil
	}

	// Step down and accept the new leader
	c.State = "FOLLOWER"

	// Sync our epoch with the leader's epoch
	if req.Epoch > c.Epoch {
		c.Epoch = req.Epoch
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
		return nil, status.Errorf(codes.FailedPrecondition, "node %s is not the leader. please route to the current leader", c.ID)
	}

	log.Printf("[LEADER %s] Received task for amount: %f", c.ID, req.Amount)

	if len(c.TaskQueue) >= cap(c.TaskQueue) {
		log.Printf("[LEADER %s] TaskQueue at capacity. Rejecting request.", c.ID)
		return nil, status.Errorf(codes.ResourceExhausted, "system is under heavy load, please try again")
	}

	// 2. Generate a dynamic Transaction ID (CRITICAL FIX: UUID prevents collisions)
	txnID := "txn-" + uuid.New().String()

	payloadMap := map[string]interface{}{
		"amount":          req.Amount,
		"currency":        req.Currency,
		"merchant_id":     req.MerchantId,
		"idempotency_key": req.IdempotencyKey,
	}
	payloadBytes, err := json.Marshal(payloadMap) // Automatically escapes bad characters
	if err != nil {
		log.Printf("[LEADER %s] JSON Marshal failed: %v", c.ID, err)
		return nil, status.Errorf(codes.Internal, "failed to encode task payload")
	}
	safePayload := string(payloadBytes)

	// 3. --- FORWARD TO C4 (Payment Log Service) ---
	if c.c4Client == nil {
		return nil, status.Errorf(codes.Internal, "internal server error: C4 client not initialized")
	}

	logRes, err := c.c4Client.AppendEntry(ctx, &logPb.LogEntry{
		Epoch:   c.Epoch,
		TxnId:   txnID,
		State:   "QUEUED",
		Payload: safePayload,
	})

	if err != nil {
		// It is safe to return an error here because nothing was written to the WAL.
		return nil, status.Errorf(codes.Unavailable, "failed to persist task to WAL: %v", err)
	}

	log.Printf("[LEADER %s] Successfully wrote to C4 log! Index: %d", c.ID, logRes.LogIndex)

	// 4. --- PUSH TO WORKER QUEUE (Bounded) ---
	newTask := &workerPb.TaskAssignment{
		Epoch:          c.Epoch,
		TaskId:         txnID,
		IdempotencyKey: req.IdempotencyKey,
		Amount:         req.Amount,
		// Currency:       req.Currency,   // Point 6 fix
		// MerchantId:     req.MerchantId, // Point 6 fix
	}

	// POINT 3 FIX: Use a select statement with a strict timeout instead of an unbounded goroutine.
	select {
	case c.TaskQueue <- newTask:
		// Success! The queue had space.
		log.Printf("[LEADER %s] Task %s enqueued. Queue size: %d", c.ID, txnID, len(c.TaskQueue))
		return &paymentPb.SubmitTaskResponse{
			TxnId: txnID,
			Result: &paymentPb.SubmitTaskResponse_Success{
				Success: true,
			},
		}, nil
	case <-time.After(1 * time.Second):
		// The queue filled up during the split-second we were writing to the database.
		// We drop the memory operation to prevent a goroutine leak. 
		// The task is safe in the C4 WAL and will be recovered on the next leader election.
		log.Printf("[LEADER %s] ⚠️ CRITICAL: Queue blocked. Task %s stranded in WAL.", c.ID, txnID)
		return nil, status.Errorf(codes.ResourceExhausted, "system is under heavy load, task %s recorded but delayed", txnID)
	}
}
// ---------------------------------------------------------
// 3. WORKER MANAGEMENT SERVICE (From C3 Workers)
// ---------------------------------------------------------

func (c *CoordinatorNode) RegisterWorker(ctx context.Context, req *workerPb.RegisterRequest) (*workerPb.RegisterResponse, error) {
	c.mu.Lock()
	isLeader := c.State == "LEADER"
	c.mu.Unlock()

	if !isLeader {
		return nil, status.Errorf(codes.FailedPrecondition, "node %s is not the leader. cannot register workers", c.ID)
	}

	c.mu.Lock()
	c.Workers[req.WorkerId] = time.Now()
	c.mu.Unlock()

	log.Printf("[LEADER %s] Registered worker: %s", c.ID, req.WorkerId)
	return &workerPb.RegisterResponse{Success: true}, nil
}

// Heartbeat receives periodic health checks from workers
func (c *CoordinatorNode) Heartbeat(ctx context.Context, req *workerPb.HeartbeatRequest) (*workerPb.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.State == "LEADER" {
		_, exists := c.Workers[req.WorkerId]
		if !exists {
			log.Printf("[LEADER %s] New worker joined the pool: %s", c.ID, req.WorkerId)
		}
		c.Workers[req.WorkerId] = time.Now() // Update last seen timestamp
	}

	return &workerPb.HeartbeatResponse{
		Acknowledged: true,
	}, nil
}

// startLeaderHeartbeat continuously suppresses follower elections
func (c *CoordinatorNode) startLeaderHeartbeat() {
	log.Printf("[LEADER %s] Starting dedicated per-peer heartbeat tickers...", c.ID)

	for peerID, client := range c.peerClients {
		// Spawn exactly ONE long-lived goroutine per peer
		go func(pID string, cli coordPb.CoordinatorClusterClient) {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()

			for range ticker.C {
				c.mu.Lock()
				state := c.State
				epoch := c.Epoch
				c.mu.Unlock()

				// If we step down, this specific peer's goroutine quietly exits
				if state != "LEADER" {
					return 
				}

				// Strict 1-second timeout prevents overlapping network hangs
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				_, err := cli.AnnounceCoordinator(ctx, &coordPb.CoordinatorMessage{
					Epoch:    epoch,
					LeaderId: c.ID,
				})
				cancel() // Always cancel context after the call
				
				if err != nil {
					// Peer is down, but we don't leak goroutines waiting for them
				}
			}
		}(peerID, client)
	}
}

// ReportResult handles the outcome of a payment from a worker
func (c *CoordinatorNode) ReportResult(ctx context.Context, req *workerPb.TaskResult) (*workerPb.ResultAck, error) {
	c.mu.Lock()
	isLeader := c.State == "LEADER"
	c.mu.Unlock()

	if !isLeader {
		return nil, status.Errorf(codes.FailedPrecondition, "node %s is not the leader", c.ID)
	}

	log.Printf("[LEADER %s] Processing task result for %s from worker %s", c.ID, req.TaskId, req.WorkerId)

	// 1. Determine if the worker succeeded or failed
	isSuccess := req.GetSuccess()
	if !isSuccess {
		log.Printf("[LEADER %s] Worker %s failed task %s. Error: %s", c.ID, req.WorkerId, req.TaskId, req.GetErrorMessage())
	} else {
		log.Printf("[LEADER %s] Worker %s successfully completed task %s", c.ID, req.WorkerId, req.TaskId)
	}

	// 2. --- FORWARD RESULT TO C4 (Payment Log Service) ---
	// FIX: Removed the Dial/NewClient block to prevent connection exhaustion
	if c.c4Client != nil {
		_, err := c.c4Client.WriteResult(ctx, &logPb.WriteResultRequest{
			TxnId:   req.TaskId,
			Success: isSuccess,
			// IdempotencyKey: req.GetIdempotencyKey(), // FIX: Uncomment this once you've added the field to worker.proto
		})

		if err != nil {
			log.Printf("[LEADER %s] C4 WriteResult failed for task %s: %v", c.ID, req.TaskId, err)
		} else {
			log.Printf("[LEADER %s] Permanently recorded task %s outcome to C4.", c.ID, req.TaskId)
		}
	} else {
		log.Printf("[LEADER %s] Skip C4 update: Persistent client is nil", c.ID)
	}

	c.mu.Lock()
	delete(c.InFlight, req.WorkerId)
	c.mu.Unlock()

	// 3. Acknowledge the worker
	return &workerPb.ResultAck{
		Acknowledged: true,
	}, nil
}

// PollTasks is a SERVER STREAMING endpoint where the coordinator pushes tasks to the worker
func (c *CoordinatorNode) PollTasks(req *workerPb.PollRequest, stream workerPb.WorkerManagement_PollTasksServer) error {
	c.mu.Lock()
	isLeader := c.State == "LEADER"
	c.mu.Unlock()

	if !isLeader {
		return status.Errorf(codes.FailedPrecondition, "node %s is not the leader. stream rejected", c.ID)
	}

	log.Printf("[LEADER %s] Worker %s started polling for tasks", c.ID, req.WorkerId)

	// Listen for either a new task or the client disconnecting
	for {
		select {
		case <-stream.Context().Done():
			// The worker closed the connection or timed out.
			// Exit the goroutine cleanly!
			log.Printf("[LEADER %s] Worker %s disconnected. Closing stream.", c.ID, req.WorkerId)
			return nil

		case task := <-c.TaskQueue:
			log.Printf("[LEADER %s] Dispatching task %s to worker %s", c.ID, task.TaskId, req.WorkerId)

			err := stream.Send(task)
			if err != nil {
				log.Printf("[LEADER %s] Failed to send task %s to worker %s: %v", c.ID, task.TaskId, req.WorkerId, err)

				// Re-enqueue the task safely
				select {
				case c.TaskQueue <- task:
					log.Printf("[LEADER %s] Successfully re-enqueued task %s", c.ID, task.TaskId)
				default:
					log.Printf("[LEADER %s] ⚠️ CRITICAL: Queue full! Dropped re-enqueued task %s", c.ID, task.TaskId)
				}
				return err
			}
			c.mu.Lock()
			c.InFlight[req.WorkerId] = task
			c.mu.Unlock()
		}
	}
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

	// Transition to CANDIDATE and increment our Epoch
	c.State = "CANDIDATE"
	c.Epoch++

	log.Printf("[Node %s] Leader timeout! Transitioned to CANDIDATE for Epoch %d", c.ID, c.Epoch)

	go c.broadcastElection()
}

// broadcastElection sends an ELECTION message to all nodes with a higher ID
func (c *CoordinatorNode) broadcastElection() {
	// 1. Data Race Fix: Capture the epoch safely before spawning any network calls
	c.mu.Lock()
	currentEpoch := c.Epoch
	c.mu.Unlock()

	higherClients := make(map[string]coordPb.CoordinatorClusterClient)
	myNumericID := parseNodeID(c.ID)

	// 2. Connection Pool Fix: Filter our existing, persistent connections
	// instead of using raw IP strings
	for id, client := range c.peerClients {
		peerNumericID := parseNodeID(id)
		if peerNumericID > myNumericID {
			higherClients[id] = client
		}
	}

	// 3. Base Case: If there are no higher nodes, we win instantly!
	if len(higherClients) == 0 {
		log.Printf("[Node %s] possessing highest ID, election won!", c.ID)
		c.becomeLeader()
		return
	}

	log.Printf("[Node %s] Broadcasting ELECTION to %d higher nodes...", c.ID, len(higherClients))

	// 4. Concurrent RPC calls using a Go channel to collect responses
	responses := make(chan bool, len(higherClients))

	for id, client := range higherClients {
		// Pass the pre-warmed client into the goroutine!
		go func(peerID string, cli coordPb.CoordinatorClusterClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// No more grpc.NewClient() here! We just use the 'cli' passed in.
			res, err := cli.Election(ctx, &coordPb.ElectionMessage{
				Epoch:       currentEpoch, // Using the safely captured epoch
				CandidateId: c.ID,
			})

			// If the RPC succeeded and the node said OK
			if err == nil && res.Ok {
				log.Printf("[Node %s] Received OK from higher node: %s", c.ID, peerID)
				responses <- true
			} else {
				responses <- false
			}
		}(id, client)
	}

	// 5. Wait for all responses
	gotOk := false
	for i := 0; i < len(higherClients); i++ {
		if <-responses {
			gotOk = true // Someone higher is alive!
		}
	}

	// 6. Evaluate the Election Results
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
	// Safely grab the epoch to prevent data races during the broadcast
	c.mu.Lock()
	currentEpoch := c.Epoch
	c.mu.Unlock()

	for peerID, client := range c.peerClients {
		// Fire off a concurrent gRPC call to each peer
		go func(pID string, cli coordPb.CoordinatorClusterClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err := cli.AnnounceCoordinator(ctx, &coordPb.CoordinatorMessage{
				Epoch:    currentEpoch,
				LeaderId: c.ID,
			})
			if err != nil {
				return // Peer is likely down, which is fine
			}
		}(peerID, client)
	}
}

// startWorkerMonitor sweeps the worker pool to detect crashed containers
func (c *CoordinatorNode) startWorkerMonitor() {
	log.Printf("[LEADER %s] Starting worker heartbeat monitor...", c.ID)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()

		if c.State != "LEADER" {
			log.Printf("[Node %s] Stepped down. Stopping worker monitor.", c.ID)
			return
		}

		now := time.Now()
		for workerID, lastSeen := range c.Workers {
			// If it has been more than 6 seconds since their last ping...
			if now.Sub(lastSeen) > 6*time.Second {
				log.Printf("[LEADER %s] Worker %s timeout (missed 3 heartbeats). Marking as DEAD.", c.ID, workerID)

				delete(c.Workers, workerID)

				if strandedTask, exists := c.InFlight[workerID]; exists {
					log.Printf("[LEADER %s] Reassigning incomplete task %s from dead worker %s", c.ID, strandedTask.TaskId, workerID)
					delete(c.InFlight, workerID)

					// Push back to the queue in a goroutine to avoid blocking the monitor
					go func(t *workerPb.TaskAssignment) {
						c.TaskQueue <- t
					}(strandedTask)
				}
			}
		}
		c.mu.Unlock()
	}
}

// rebuildQueueFromC4 queries the persistent log for stranded tasks and re-enqueues them.
func (c *CoordinatorNode) rebuildQueueFromC4() {
	if c.c4Client == nil {
		log.Printf("[LEADER %s] ⚠️ Cannot rebuild queue: C4 persistent client is nil", c.ID)
		return
	}

	log.Printf("[LEADER %s] Initiating WAL recovery from C4...", c.ID)

	// Give the database 10 seconds to respond with the pending records
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := c.c4Client.GetAllPending(ctx, &logPb.PendingRequest{
		Epoch: c.Epoch,
	})
	if err != nil {
		log.Printf("[LEADER %s] CRITICAL: Failed to fetch pending tasks from C4: %v", c.ID, err)
		return
	}

	recoveredCount := 0

	// 2. Process the stream of LogEntries one by one
	for {
		entry, err := stream.Recv()

		if err == io.EOF {
			// The database has finished sending all pending tasks
			break
		}
		if err != nil {
			log.Printf("[LEADER %s] ⚠️ Error reading from C4 recovery stream: %v", c.ID, err)
			break
		}

		// 3. Unmarshal the JSON payload
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(entry.Payload), &payload); err != nil {
			log.Printf("[LEADER %s] Failed to parse JSON payload for recovered task %s: %v", c.ID, entry.TxnId, err)
			continue
		}

		// 4. Safely type-assert the fields extracted from the JSON map
		amount, _ := payload["amount"].(float64)
		// currency, _ := payload["currency"].(string)
		// merchantID, _ := payload["merchant_id"].(string)
		idempotencyKey, _ := payload["idempotency_key"].(string)

		// 5. Reconstruct the TaskAssignment
		recoveredTask := &workerPb.TaskAssignment{
			Epoch:  c.Epoch, // Stamp it with the NEW leader's epoch
			TaskId: entry.TxnId,
			Amount: amount,
			// Currency:       currency,
			// MerchantId:     merchantID,
			IdempotencyKey: idempotencyKey,
		}

		// 6. Non-blocking enqueue
		select {
		case c.TaskQueue <- recoveredTask:
			recoveredCount++
			log.Printf("[LEADER %s] Successfully re-enqueued stranded task %s", c.ID, recoveredTask.TaskId)
		default:
			log.Printf("[LEADER %s] ⚠️ Queue full during startup recovery! Dropped task %s", c.ID, recoveredTask.TaskId)
		}
	}

	log.Printf("[LEADER %s] Recovery complete. Restored %d pending tasks from C4 WAL", c.ID, recoveredCount)
}

// becomeLeader locks in the victory and changes the state
func (c *CoordinatorNode) becomeLeader() {
	c.mu.Lock()
	c.State = "LEADER"
	c.mu.Unlock()

	c.broadcastCoordinator()

	log.Printf("[LEADER %s] Successfully elected as LEADER. Operating in Epoch %d", c.ID, c.Epoch)
	log.Printf("[LEADER %s] TODO: Call C4.GetAllPending() to rebuild the task queue", c.ID)

	c.rebuildQueueFromC4() // Recovery Function

	go c.startLeaderHeartbeat()
	go c.startWorkerMonitor()
}

func main() {
	idFlag := flag.String("id", "coordinator-1", "The ID of the coordinator node")
	portFlag := flag.String("port", ":50051", "The port to run the gRPC server on")
	flag.Parse() // Parse the flags from the terminal

	log.Printf("Booting up node %s... Connecting to C4 Database...", *idFlag)
	var c4Client logPb.PaymentLogServiceClient // Declare variable
	c4Conn, err := grpc.NewClient("localhost:50054", grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err == nil {
		defer c4Conn.Close()
		c4Client = logPb.NewPaymentLogServiceClient(c4Conn)
		log.Printf("Successfully connected to C4 Database!")
	}

	// Set up the persistent peer connection pool
	peerClients := make(map[string]coordPb.CoordinatorClusterClient)
	for id, addr := range clusterPeers {
		if id == *idFlag {
			continue // Don't dial ourselves
		}

		// Note: grpc.NewClient doesn't block waiting for connection by default,
		// so it's safe to do this even if peers aren't online yet.
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			defer conn.Close() // Ensure connections are cleaned up on shutdown
			peerClients[id] = coordPb.NewCoordinatorClusterClient(conn)
		}
	}

	node := &CoordinatorNode{
		ID:            *idFlag,
		State:         "FOLLOWER",
		Epoch:         int64(1),
		Workers:       make(map[string]time.Time),
		leaderTimeout: 5 * time.Second,
		resetTimer:    make(chan bool, 1),
		TaskQueue:     make(chan *workerPb.TaskAssignment, 100),
		InFlight:      make(map[string]*workerPb.TaskAssignment),
		c4Client:      c4Client,
		peerClients:   peerClients,
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
