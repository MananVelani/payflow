// Package cmd implements the PayFlow chaos CLI.
// partition command simulates network partitions between containers.
package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var partitionCmd = &cobra.Command{
	Use:   "partition-nodes <id1,id2,...>",
	Short: "Simulate network partition between specified containers",
	Long: `Simulate a network partition by blocking traffic between the specified containers.
This tests the system's behavior when certain nodes cannot communicate with each other,
which is critical for validating Bully election and split-brain prevention.

WEEK3: Actual iptables-based partition will be implemented. Currently, this command
only prints what the partition would look like.

Examples:
  chaos partition-nodes coordinator-1,coordinator-2
  chaos partition-nodes coordinator-1,coordinator-2,coordinator-3`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeList := args[0]
		nodes := strings.Split(nodeList, ",")

		if len(nodes) < 2 {
			return fmt.Errorf("partition requires at least 2 node names, got %d", len(nodes))
		}

		// Validate all node names
		for _, node := range nodes {
			trimmed := strings.TrimSpace(node)
			if !validContainers[trimmed] {
				return fmt.Errorf("unknown container %q; valid names: coordinator-1/2/3, worker-1..5, api-gateway, payment-log, monitor", trimmed)
			}
		}

		logChaos("creating network partition between: %s", nodeList)

		// Print all isolation pairs
		fmt.Println("Partition plan:")
		pairCount := 0
		for i := 0; i < len(nodes); i++ {
			for j := i + 1; j < len(nodes); j++ {
				a := strings.TrimSpace(nodes[i])
				b := strings.TrimSpace(nodes[j])
				fmt.Printf("  🔒 %s ←✕→ %s (traffic blocked both directions)\n", a, b)
				pairCount++
			}
		}
		fmt.Printf("\nTotal pairs isolated: %d\n", pairCount)

		// WEEK3: implement actual iptables-based partition via docker exec
		fmt.Println("\n⚠  WEEK3: Actual iptables rules not yet implemented")
		fmt.Println("   This is a dry-run preview of the partition topology")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(partitionCmd)
}
