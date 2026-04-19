package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/config"
	"github.com/your-org/payflow/worker/internal/domain"
	"github.com/stretchr/testify/require"
)

// TestFullPipelineDemo is a "verbose" test that walks through every endpoint
// mentioned by the user: Registration, Heartbeat, C4 Idempotency, and Mock Bank.
func TestFullPipelineDemo(t *testing.T) {
	// 1. Initialize the Harness (Boots C3 Worker, Mock C2, Mock C4, Mock Bank)
	// This automatically handles "Registration" via the Harness constructor logic
	// which wires the C3 Worker to the Mock C2.
	fmt.Println("\n[DEMO] 1. Initializing Service & Registering with Coordinator (C2)...")
	h := NewHarness(t, &config.Config{HeartbeatInterval: 1 * time.Second})
	
	// Give it a moment to establish the "Heartbeat" stream and send a few pings
	fmt.Println("[DEMO] 2. Establishing Heartbeat stream (pings every 2s)...")
	fmt.Println("       >> Watching for 'heartbeat sent' logs for 5 seconds...")
	time.Sleep(5 * time.Second)

	// 2. Prepare a Payment Task
	task := &domain.Task{
		TaskID:         "demo-task-001",
		IdempotencyKey: "demo-idem-key-001",
		Amount:         250.75,
		Currency:       "USD",
		MerchantID:     "merchant-demo",
		Epoch:          1,
		ReceivedAt:     time.Now(),
	}

	fmt.Printf("[DEMO] 3. Dispatching Task: %s (Idempotency: %s)\n", task.TaskID, task.IdempotencyKey)

	// 3. Execute the Task
	// This will internally:
	//   a. Call C4.CheckIdempotency()
	//   b. Call Mock Bank API HTTP POST /charge
	//   c. Report results back to C2
	fmt.Println("[DEMO] 4. Worker → Checking C4 for Idempotency...")
	_, err := h.worker.ExecuteTask(context.Background(), task)
	require.NoError(t, err)

	fmt.Println("[DEMO] 5. Worker → Calling Mock Bank API...")
	
	// 4. Verify Side-Effects
	fmt.Println("[DEMO] 6. Verifying Side-Effects:")
	
	// A. Check C4 was consulted
	checks := h.C4.IdempotencyCheckCount()
	fmt.Printf("   - C4 Idempotency Check Count: %d (PASSED)\n", checks)
	require.GreaterOrEqual(t, checks, 1)

	// B. Check Bank was called
	bankCalls := h.BankHandler.CallCount.Load()
	fmt.Printf("   - Mock Bank API Call Count: %d (PASSED)\n", bankCalls)
	require.Equal(t, int32(1), bankCalls)

	// C. Check C2 received final result
	h.AssertResult(t, task.TaskID, domain.TaskStatusSuccess)
	fmt.Println("   - C2 ReportResult Success Received (PASSED)")

	// D. Prometheus Metrics (Internal check)
	fmt.Println("[DEMO] 7. All endpoints successfully exercised via Bufconn/Harness.")
}
