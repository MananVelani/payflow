// Package cmd implements the PayFlow chaos CLI.
// partition command simulates network partitions between containers.
package cmd

import (
	"context"
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

Examples:
  chaos partition-nodes coordinator-1,coordinator-2
  chaos partition-nodes coordinator-1,coordinator-2,coordinator-3`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		nodeList := args[0]
		rawNodes := strings.Split(nodeList, ",")
		nodes := make([]string, 0, len(rawNodes))
		for _, node := range rawNodes {
			trimmed := strings.TrimSpace(node)
			if trimmed == "" {
				continue
			}
			nodes = append(nodes, trimmed)
		}

		if len(nodes) < 2 {
			return fmt.Errorf("partition requires at least 2 node names, got %d", len(nodes))
		}

		// Validate all node names
		for _, node := range nodes {
			if !validContainers[node] {
				return fmt.Errorf("unknown container %q; valid names: coordinator-1/2/3, worker-1..5, api-gateway, payment-log, monitor", node)
			}
			trimmedNodes = append(trimmedNodes, trimmed)
		}

		logChaos("creating network partition between: %s", nodeList)

		// Print all isolation pairs
		fmt.Println("Partition plan:")
		pairCount := 0
		for i := 0; i < len(nodes); i++ {
			for j := i + 1; j < len(nodes); j++ {
				a := nodes[i]
				b := nodes[j]
				fmt.Printf("  🔒 %s ←✕→ %s (traffic blocked both directions)\n", a, b)
				pairCount++
			}
		}
		fmt.Printf("\nTotal pairs isolated: %d\n", pairCount)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := getDockerClient().PartitionNodes(ctx, nodes); err != nil {
			return fmt.Errorf("applying partition rules: %w", err)
		}

		if dryRun {
			fmt.Println("\n[DRY-RUN] Partition plan validated (no live rules applied)")
		} else {
			fmt.Println("\n✅ Partition rules applied successfully")
			fmt.Println("Use container restart or manual iptables cleanup to heal partition")
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(partitionCmd)
}
