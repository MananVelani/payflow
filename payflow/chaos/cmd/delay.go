// Package cmd implements the PayFlow chaos CLI.
// delay command injects network latency into containers.
package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

// validContainers lists all container names that accept delay injection.
var validContainers = map[string]bool{
	"coordinator-1": true, "coordinator-2": true, "coordinator-3": true,
	"worker-1": true, "worker-2": true, "worker-3": true, "worker-4": true, "worker-5": true,
	"api-gateway": true, "payment-log": true, "monitor": true,
}

var delayMs int

var delayCmd = &cobra.Command{
	Use:   "delay-network <container-name>",
	Short: "Inject network latency into a container",
	Long: `Inject artificial network latency using tc netem rules inside the target container.
This simulates slow network conditions to test timeout handling and election behavior.

Set --ms 0 to remove previously injected latency.

Examples:
  chaos delay-network coordinator-1 --ms 200
  chaos delay-network worker-3 --ms 500
  chaos delay-network coordinator-1 --ms 0    # remove delay`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		containerName := args[0]

		if !validContainers[containerName] {
			return fmt.Errorf("unknown container %q; valid names: coordinator-1/2/3, worker-1..5, api-gateway, payment-log, monitor", containerName)
		}

		if delayMs < 0 || delayMs > 5000 {
			return fmt.Errorf("--ms must be between 0 and 5000 (got %d)", delayMs)
		}

		logChaos("adding %dms network delay to container %s", delayMs, containerName)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := getDockerClient().AddNetworkDelay(ctx, containerName, delayMs); err != nil {
			return fmt.Errorf("adding delay to %s: %w", containerName, err)
		}

		if delayMs == 0 {
			fmt.Printf("✅ Removed network delay from %s\n", containerName)
		} else {
			fmt.Printf("✅ Added %dms latency to %s\n", delayMs, containerName)
			fmt.Printf("Remove with: chaos delay-network %s --ms 0\n", containerName)
		}
		return nil
	},
}

func init() {
	delayCmd.Flags().IntVar(&delayMs, "ms", 100, "Network delay in milliseconds (0-5000, 0 removes delay)")
	rootCmd.AddCommand(delayCmd)
}
