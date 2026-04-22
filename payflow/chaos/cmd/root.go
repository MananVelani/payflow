// Package cmd implements the PayFlow chaos CLI.
// The chaos CLI is a fault injection tool for testing PayFlow's distributed
// fault tolerance capabilities. It can kill containers, inject network latency,
// and simulate network partitions. By default, all commands run in --dry-run
// mode to prevent accidental damage.
package cmd

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/payflow/chaos/docker"
	"github.com/spf13/cobra"
)

var (
	composeFile  string
	dryRun       bool
	timeout      time.Duration
	dockerClient *docker.Client
	clientOnce   sync.Once
	clientErr    error
)

var rootCmd = &cobra.Command{
	Use:   "chaos",
	Short: "PayFlow fault injection CLI",
	Long: `PayFlow Chaos — Fault Injection CLI for Distributed Payment System Testing

This tool allows you to inject faults into a running PayFlow cluster to test
the system's fault tolerance capabilities. Available operations include:

  • kill coordinator <id>    — Stop a coordinator container (triggers Bully election)
  • kill worker <id|all>     — Stop a worker container (triggers task reassignment)
  • kill payment-log         — Stop the payment log service (tests write buffering)
  • delay-network <name>     — Inject network latency into a container
  • partition-nodes <ids>    — Simulate network partition between nodes

⚠  WARNING: This tool manipulates Docker containers. The --dry-run flag (enabled
   by default) prevents actual container operations. Disable it explicitly with
   --dry-run=false only when you understand the consequences.

Examples:
  chaos kill coordinator 1 --dry-run=false
  chaos kill worker all
  chaos delay-network coordinator-1 --ms 200
  chaos partition-nodes coordinator-1,coordinator-2`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		clientOnce.Do(func() {
			dockerClient, clientErr = docker.New(composeFile, dryRun)
		})
		if clientErr != nil {
			return fmt.Errorf("initializing docker client: %w", clientErr)
		}
		return nil
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&composeFile, "compose-file", "docker-compose.yml",
		"Path to the docker-compose.yml file")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", true,
		"When true (default), only log what would happen without performing actual operations")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 30*time.Second,
		"Timeout for Docker operations")
}

// Execute runs the root command. Called from main.go.
func Execute() error {
	return rootCmd.Execute()
}

// getDockerClient returns the initialized Docker client.
func getDockerClient() *docker.Client {
	return dockerClient
}

// logChaos prints a timestamped chaos operation message.
func logChaos(format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	if dryRun {
		log.Printf("[%s] [DRY-RUN] CHAOS: %s", timestamp, msg)
	} else {
		log.Printf("[%s] CHAOS: %s", timestamp, msg)
		fmt.Fprintf(os.Stderr, "⚠ WARNING: This is a LIVE operation (--dry-run=false)\n")
	}
}
