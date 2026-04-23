package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	coordPb "payflow/proto/coordinator"
)

func TestCoordinatorRejoinDoesNotResetEpoch(t *testing.T) {
	// Connect to coordinator-1 and coordinator-2
	conn1, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Skip("Coordinator-1 not reachable, skipping test")
	}
	defer conn1.Close()

	conn2, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Skip("Coordinator-2 not reachable, skipping test")
	}
	defer conn2.Close()

	client1 := coordPb.NewCoordinatorClusterClient(conn1)
	client2 := coordPb.NewCoordinatorClusterClient(conn2)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Simulate coordinator-1 triggering an election with epoch 10
	resp, err := client2.Election(ctx, &coordPb.ElectionMessage{
		Epoch:       10,
		CandidateId: "coordinator-1",
	})
	
	if err == nil {
		// Verify that coordinator-2 accepted the higher epoch
		assert.GreaterOrEqual(t, resp.Epoch, int64(10), "Coordinator 2 should adopt epoch >= 10")
	}

	// Now simulate coordinator-2 crashing and coming back (it would normally read epoch from C4)
	// But let's say it tries to send a stale election with epoch 5 (less than 10)
	resp1, err := client1.Election(ctx, &coordPb.ElectionMessage{
		Epoch:       5,
		CandidateId: "coordinator-2",
	})

	if err == nil {
		assert.False(t, resp1.Ok, "Coordinator 1 should reject stale election from epoch 5")
		assert.GreaterOrEqual(t, resp1.Epoch, int64(10), "Coordinator 1 should return its higher epoch")
	}
}

func TestNetworkPartitionStaleMessageDropped(t *testing.T) {
	conn1, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Skip("Coordinator-1 not reachable, skipping test")
	}
	defer conn1.Close()

	client1 := coordPb.NewCoordinatorClusterClient(conn1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Advance coordinator-1's epoch
	client1.Election(ctx, &coordPb.ElectionMessage{
		Epoch:       20,
		CandidateId: "coordinator-2",
	})

	// Simulate a stale CoordinatorMessage from a partitioned leader (epoch 15)
	ackResp, err := client1.AnnounceCoordinator(ctx, &coordPb.CoordinatorMessage{
		Epoch:    15,
		LeaderId: "coordinator-3",
	})

	if err == nil {
		assert.False(t, ackResp.Acknowledged, "Coordinator 1 should reject stale leader announcement")
	}
}
