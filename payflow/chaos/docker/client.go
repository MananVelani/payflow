// Package docker wraps the Docker SDK for container lifecycle operations.
// It provides methods to kill, pause, and inject network delays into
// PayFlow containers for fault injection testing.
package docker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
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
// Uses Docker's SIGKILL to immediately terminate the container process.
func (c *Client) KillContainer(ctx context.Context, name string) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would kill container: %s", name)
		return nil
	}

	// Find container by name (compose project may prefix the name)
	containers, err := c.cli.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("name", name),
			filters.Arg("status", "running"),
		),
	})
	if err != nil {
		return fmt.Errorf("listing containers: %w", err)
	}

	if len(containers) == 0 {
		return fmt.Errorf("no running container found matching name %q", name)
	}

	// Kill the first matching container
	containerID := containers[0].ID
	log.Printf("Killing container %s (ID: %.12s)", name, containerID)

	if err := c.cli.ContainerKill(ctx, containerID, "SIGKILL"); err != nil {
		return fmt.Errorf("killing container %s: %w", name, err)
	}

	return nil
}

// PauseContainer freezes a container, simulating a process hang or GC pause.
func (c *Client) PauseContainer(ctx context.Context, name string) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would pause container: %s", name)
		return nil
	}

	containers, err := c.cli.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("name", name),
			filters.Arg("status", "running"),
		),
	})
	if err != nil {
		return fmt.Errorf("listing containers: %w", err)
	}

	if len(containers) == 0 {
		return fmt.Errorf("no running container found matching name %q", name)
	}

	containerID := containers[0].ID
	log.Printf("Pausing container %s (ID: %.12s)", name, containerID)

	if err := c.cli.ContainerPause(ctx, containerID); err != nil {
		return fmt.Errorf("pausing container %s: %w", name, err)
	}

	return nil
}

// UnpauseContainer resumes a paused container.
func (c *Client) UnpauseContainer(ctx context.Context, name string) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would unpause container: %s", name)
		return nil
	}

	containers, err := c.cli.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("name", name),
			filters.Arg("status", "paused"),
		),
	})
	if err != nil {
		return fmt.Errorf("listing containers: %w", err)
	}

	if len(containers) == 0 {
		return fmt.Errorf("no paused container found matching name %q", name)
	}

	containerID := containers[0].ID
	log.Printf("Unpausing container %s (ID: %.12s)", name, containerID)

	if err := c.cli.ContainerUnpause(ctx, containerID); err != nil {
		return fmt.Errorf("unpausing container %s: %w", name, err)
	}

	return nil
}

// AddNetworkDelay injects artificial latency into a container's network stack.
// Uses tc qdisc netem to add delay. Set delayMs=0 to remove delay.
func (c *Client) AddNetworkDelay(ctx context.Context, containerName string, delayMs int) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would add %dms delay to: %s", delayMs, containerName)
		return nil
	}

	containers, err := c.cli.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", containerName)),
	})
	if err != nil {
		return fmt.Errorf("listing containers: %w", err)
	}

	if len(containers) == 0 {
		return fmt.Errorf("no container found matching name %q", containerName)
	}

	containerID := containers[0].ID

	// Build tc command
	var cmd []string
	if delayMs == 0 {
		// Remove existing qdisc
		cmd = []string{"tc", "qdisc", "del", "dev", "eth0", "root"}
	} else {
		// Add delay using netem
		cmd = []string{"tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", fmt.Sprintf("%dms", delayMs)}
	}

	// Execute command inside container
	execConfig := types.ExecConfig{
		Cmd:          cmd,
		AttachStderr: true,
		AttachStdout: true,
	}

	execID, err := c.cli.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("creating exec: %w", err)
	}

	if err := c.cli.ContainerExecStart(ctx, execID.ID, types.ExecStartCheck{}); err != nil {
		// Ignore "qdisc not found" errors when removing (no delay was set)
		if delayMs == 0 {
			log.Printf("Note: no existing delay to remove from %s", containerName)
			return nil
		}
		return fmt.Errorf("executing tc command: %w", err)
	}

	return nil
}

// PartitionNodes creates a symmetric network partition between the provided nodes.
// It adds INPUT/OUTPUT iptables DROP rules for each pair of container IPs.
func (c *Client) PartitionNodes(ctx context.Context, nodeNames []string) error {
	if len(nodeNames) < 2 {
		return fmt.Errorf("need at least 2 nodes to create a partition")
	}

	if c.dryRun {
		log.Printf("[DRY-RUN] Would partition nodes: %s", strings.Join(nodeNames, ","))
		return nil
	}

	type nodeMeta struct {
		name string
		id   string
		ip   string
	}

	metas := make([]nodeMeta, 0, len(nodeNames))
	for _, name := range nodeNames {
		containerID, err := c.findContainerID(ctx, name, true)
		if err != nil {
			return err
		}

		ip, err := c.containerIP(ctx, containerID)
		if err != nil {
			return fmt.Errorf("resolving IP for %s: %w", name, err)
		}

		metas = append(metas, nodeMeta{name: name, id: containerID, ip: ip})
	}

	var errs []error
	for i := 0; i < len(metas); i++ {
		for j := i + 1; j < len(metas); j++ {
			a := metas[i]
			b := metas[j]

			if err := c.blockOneWay(ctx, a.id, b.ip); err != nil {
				errs = append(errs, fmt.Errorf("blocking %s -> %s: %w", a.name, b.name, err))
			}
			if err := c.blockOneWay(ctx, b.id, a.ip); err != nil {
				errs = append(errs, fmt.Errorf("blocking %s -> %s: %w", b.name, a.name, err))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (c *Client) blockOneWay(ctx context.Context, containerID, peerIP string) error {
	inputRule := fmt.Sprintf("iptables -C INPUT -s %s -j DROP || iptables -A INPUT -s %s -j DROP", peerIP, peerIP)
	outputRule := fmt.Sprintf("iptables -C OUTPUT -d %s -j DROP || iptables -A OUTPUT -d %s -j DROP", peerIP, peerIP)

	if err := c.execInContainer(ctx, containerID, []string{"/bin/sh", "-c", inputRule}); err != nil {
		return err
	}
	if err := c.execInContainer(ctx, containerID, []string{"/bin/sh", "-c", outputRule}); err != nil {
		return err
	}

	return nil
}

func (c *Client) findContainerID(ctx context.Context, name string, runningOnly bool) (string, error) {
	filterArgs := filters.NewArgs(filters.Arg("name", name))
	if runningOnly {
		filterArgs.Add("status", "running")
	}

	containers, err := c.cli.ContainerList(ctx, container.ListOptions{Filters: filterArgs})
	if err != nil {
		return "", fmt.Errorf("listing containers for %q: %w", name, err)
	}

	if len(containers) == 0 {
		return "", fmt.Errorf("no running container found matching name %q", name)
	}

	return containers[0].ID, nil
}

func (c *Client) containerIP(ctx context.Context, containerID string) (string, error) {
	inspect, err := c.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("inspecting container %.12s: %w", containerID, err)
	}

	ip := ipFromNetworks(inspect.NetworkSettings.Networks)
	if ip == "" {
		return "", fmt.Errorf("container %.12s has no network IP", containerID)
	}

	return ip, nil
}

func ipFromNetworks(networks map[string]*network.EndpointSettings) string {
	if len(networks) == 0 {
		return ""
	}

	if ep, ok := networks["payflow-net"]; ok && ep != nil && ep.IPAddress != "" {
		return ep.IPAddress
	}

	for _, ep := range networks {
		if ep != nil && ep.IPAddress != "" {
			return ep.IPAddress
		}
	}

	return ""
}

func (c *Client) execInContainer(ctx context.Context, containerID string, cmd []string) error {
	execConfig := types.ExecConfig{
		Cmd:          cmd,
		AttachStderr: false,
		AttachStdout: false,
	}

	execID, err := c.cli.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("creating exec %q: %w", strings.Join(cmd, " "), err)
	}

	if err := c.cli.ContainerExecStart(ctx, execID.ID, types.ExecStartCheck{}); err != nil {
		return fmt.Errorf("starting exec %q: %w", strings.Join(cmd, " "), err)
	}

	for {
		execInspect, err := c.cli.ContainerExecInspect(ctx, execID.ID)
		if err != nil {
			return fmt.Errorf("inspecting exec %q: %w", strings.Join(cmd, " "), err)
		}

		if !execInspect.Running {
			if execInspect.ExitCode != 0 {
				return fmt.Errorf("command %q failed with exit code %d", strings.Join(cmd, " "), execInspect.ExitCode)
			}
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for exec %q: %w", strings.Join(cmd, " "), ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// ListContainers returns the names of all running PayFlow containers.
func (c *Client) ListContainers(ctx context.Context) ([]string, error) {
	if c.dryRun {
		log.Println("[DRY-RUN] Listing expected PayFlow containers")
		return knownContainers, nil
	}

	containers, err := c.cli.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("status", "running"),
		),
	})
	if err != nil {
		return nil, fmt.Errorf("listing containers: %w", err)
	}

	// Filter for PayFlow containers by matching known names
	var names []string
	knownSet := make(map[string]bool, len(knownContainers))
	for _, name := range knownContainers {
		knownSet[name] = true
	}

	for _, c := range containers {
		for _, name := range c.Names {
			// Docker prefixes names with "/"
			cleanName := name
			if len(name) > 0 && name[0] == '/' {
				cleanName = name[1:]
			}
			// Check if it matches any known container or contains the name
			for known := range knownSet {
				if cleanName == known || containsName(cleanName, known) {
					names = append(names, cleanName)
					break
				}
			}
		}
	}

	return names, nil
}

// containsName checks if fullName contains the expectedName (handles compose prefixes).
func containsName(fullName, expectedName string) bool {
	// Docker Compose may prefix with project name like "payflow-coordinator-1"
	return len(fullName) >= len(expectedName) &&
		fullName[len(fullName)-len(expectedName):] == expectedName
}

// Close releases the Docker client resources.
func (c *Client) Close() error {
	if c.cli != nil {
		return c.cli.Close()
	}
	return nil
}
