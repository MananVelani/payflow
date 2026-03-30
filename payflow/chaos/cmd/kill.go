// Package cmd implements the PayFlow chaos CLI.
// kill command stops coordinator or worker containers.
package cmd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
)

var killCmd = &cobra.Command{
	Use:   "kill",
	Short: "Kill PayFlow containers to test fault tolerance",
	Long:  "Kill coordinator, worker, or payment-log containers to simulate crashes and test the system's recovery behavior.",
}

var killCoordinatorCmd = &cobra.Command{
	Use:   "coordinator <id>",
	Short: "Kill a coordinator container (triggers Bully election)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		id, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("invalid coordinator ID %q: must be 1, 2, or 3: %w", args[0], err)
		}
		if id < 1 || id > 3 {
			return fmt.Errorf("coordinator ID must be 1, 2, or 3 (got %d)", id)
		}

		containerName := fmt.Sprintf("coordinator-%d", id)
		logChaos("killing container %s", containerName)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := getDockerClient().KillContainer(ctx, containerName); err != nil {
			return fmt.Errorf("killing coordinator-%d: %w", id, err)
		}

		fmt.Printf("✅ Container %s killed\n", containerName)
		fmt.Println("Expected: Bully election < 5s, new leader resumes from C4")
		fmt.Println("Monitor dashboard should show leader change within 10s")
		return nil
	},
}

var killWorkerCmd = &cobra.Command{
	Use:   "worker <id|all>",
	Short: "Kill a worker container (triggers task reassignment)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if args[0] == "all" {
			logChaos("killing ALL worker containers (worker-1 through worker-5)")
			for i := 1; i <= 5; i++ {
				containerName := fmt.Sprintf("worker-%d", i)
				if err := getDockerClient().KillContainer(ctx, containerName); err != nil {
					return fmt.Errorf("killing %s: %w", containerName, err)
				}
				fmt.Printf("✅ Container %s killed\n", containerName)
			}
			fmt.Println("Expected: All tasks remain QUEUED in C4; on worker restart + re-register, coordinator dispatches full queue")
			return nil
		}

		id, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("invalid worker ID %q: must be 1-5 or 'all': %w", args[0], err)
		}
		if id < 1 || id > 5 {
			return fmt.Errorf("worker ID must be 1-5 (got %d)", id)
		}

		containerName := fmt.Sprintf("worker-%d", id)
		logChaos("killing container %s", containerName)

		if err := getDockerClient().KillContainer(ctx, containerName); err != nil {
			return fmt.Errorf("killing worker-%d: %w", id, err)
		}

		fmt.Printf("✅ Container %s killed\n", containerName)
		fmt.Println("Expected: Heartbeat timeout 6s, task reassigned by coordinator")
		fmt.Println("C4 idempotency check prevents double charge on reassignment")
		return nil
	},
}

var killPaymentLogCmd = &cobra.Command{
	Use:   "payment-log",
	Short: "Kill the payment-log container (tests write buffering)",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		containerName := "payment-log"
		logChaos("killing container %s", containerName)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := getDockerClient().KillContainer(ctx, containerName); err != nil {
			return fmt.Errorf("killing payment-log: %w", err)
		}

		fmt.Printf("✅ Container %s killed\n", containerName)
		fmt.Println("Expected: Coordinator buffers writes in-memory; on C4 restart, replays buffered entries in order")
		fmt.Println("Gateway returns 503 with retry-after if buffer exceeds 60s capacity")
		return nil
	},
}

func init() {
	killCmd.AddCommand(killCoordinatorCmd)
	killCmd.AddCommand(killWorkerCmd)
	killCmd.AddCommand(killPaymentLogCmd)
	rootCmd.AddCommand(killCmd)
}
