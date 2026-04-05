// Package docker wraps the Docker SDK for container lifecycle operations.
// It provides methods to kill, pause, and inject network delays into
// PayFlow containers for fault injection testing.
package docker

import (
	"context"
	"fmt"
	"log"

	dockerclient "github.com/docker/docker/client"
)

// knownContainers is the list of expected PayFlow container names.
var knownContainers = []string{
	"coordinator-1", "coordinator-2", "coordinator-3",
	"worker-1", "worker-2", "worker-3", "worker-4", "worker-5",
	"api-gateway", "payment-log", "monitor", "jaeger",
}

// Client wraps the Docker SDK client for PayFlow container operations.
type Client struct {
	cli         *dockerclient.Client
	composeFile string
	dryRun      bool
}

// New creates a Docker client and validates connectivity to the Docker daemon.
// Returns an error if the Docker daemon is not reachable.
func New(composeFile string, dryRun bool) (*Client, error) {
	cli, err := dockerclient.NewClientWithOpts(
		dockerclient.FromEnv,
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating docker client: %w", err)
	}

	ctx := context.Background()
	ping, err := cli.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("docker daemon unreachable (is Docker running?): %w", err)
	}

	log.Printf("Docker daemon connected, API version: %s", ping.APIVersion)

	return &Client{
		cli:         cli,
		composeFile: composeFile,
		dryRun:      dryRun,
	}, nil
}

// KillContainer stops a container by name, simulating a crash.
// WEEK3: implement actual docker kill via c.cli.ContainerKill
func (c *Client) KillContainer(ctx context.Context, name string) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would kill container: %s", name)
		return nil
	}
	return fmt.Errorf("WEEK3: KillContainer not yet implemented for container %s", name)
}

// PauseContainer freezes a container, simulating a process hang or GC pause.
// WEEK3: implement via c.cli.ContainerPause
func (c *Client) PauseContainer(ctx context.Context, name string) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would pause container: %s", name)
		return nil
	}
	return fmt.Errorf("WEEK3: PauseContainer not yet implemented for container %s", name)
}

// AddNetworkDelay injects artificial latency into a container's network stack.
// WEEK3: implement via docker exec tc netem
func (c *Client) AddNetworkDelay(ctx context.Context, containerName string, delayMs int) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would add %dms delay to: %s", delayMs, containerName)
		return nil
	}
	return fmt.Errorf("WEEK3: AddNetworkDelay not yet implemented for container %s", containerName)
}

// ListContainers returns the names of all known PayFlow containers.
// WEEK3: implement via c.cli.ContainerList with label filtering
func (c *Client) ListContainers(ctx context.Context) ([]string, error) {
	if c.dryRun {
		log.Println("[DRY-RUN] Listing expected PayFlow containers")
		return knownContainers, nil
	}
	// Return hardcoded list for dry-run output in Week 1
	return knownContainers, nil
}

// Close releases the Docker client resources.
func (c *Client) Close() error {
	if c.cli != nil {
		return c.cli.Close()
	}
	return nil
}
