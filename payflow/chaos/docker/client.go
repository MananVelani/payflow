// Package docker wraps the Docker SDK for container lifecycle operations.
// It provides methods to kill, pause, and inject network delays into
// PayFlow containers for fault injection testing.
package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/docker/docker/api/types"
	containerapi "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	networkapi "github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/stdcopy"
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

	containerID, resolvedName, err := c.resolveContainer(ctx, name)
	if err != nil {
		return err
	}

	if err := c.cli.ContainerKill(ctx, containerID, "SIGKILL"); err != nil {
		return fmt.Errorf("docker kill %s: %w", resolvedName, err)
	}

	log.Printf("Killed container %s (%s)", resolvedName, containerID[:12])
	return nil
}

// PauseContainer freezes a container, simulating a process hang or GC pause.
func (c *Client) PauseContainer(ctx context.Context, name string) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would pause container: %s", name)
		return nil
	}

	containerID, resolvedName, err := c.resolveContainer(ctx, name)
	if err != nil {
		return err
	}

	if err := c.cli.ContainerPause(ctx, containerID); err != nil {
		return fmt.Errorf("docker pause %s: %w", resolvedName, err)
	}

	log.Printf("Paused container %s (%s)", resolvedName, containerID[:12])
	return nil
}

// AddNetworkDelay injects artificial latency into a container's network stack.
func (c *Client) AddNetworkDelay(ctx context.Context, containerName string, delayMs int) error {
	if c.dryRun {
		log.Printf("[DRY-RUN] Would add %dms delay to: %s", delayMs, containerName)
		return nil
	}

	containerID, resolvedName, err := c.resolveContainer(ctx, containerName)
	if err != nil {
		return err
	}

	var script string
	if delayMs == 0 {
		script = "tc qdisc del dev eth0 root >/dev/null 2>&1 || true"
	} else {
		script = fmt.Sprintf("tc qdisc del dev eth0 root >/dev/null 2>&1 || true; tc qdisc add dev eth0 root netem delay %dms", delayMs)
	}

	if err := c.runInContainer(ctx, containerID, []string{"sh", "-c", script}); err != nil {
		return fmt.Errorf("setting network delay on %s: %w (hint: container needs 'tc' binary and NET_ADMIN capability)", resolvedName, err)
	}

	if delayMs == 0 {
		log.Printf("Removed network delay from %s", resolvedName)
	} else {
		log.Printf("Applied %dms network delay to %s", delayMs, resolvedName)
	}
	return nil
}

// ListContainers returns the names of all known PayFlow containers.
func (c *Client) ListContainers(ctx context.Context) ([]string, error) {
	if c.dryRun {
		log.Println("[DRY-RUN] Listing expected PayFlow containers")
		return knownContainers, nil
	}

	containers, err := c.cli.ContainerList(ctx, containerapi.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("listing containers: %w", err)
	}

	results := make([]string, 0, len(containers))
	for _, ctr := range containers {
		for _, n := range ctr.Names {
			name := strings.TrimPrefix(n, "/")
			if isPayflowContainerName(name) {
				results = append(results, name)
				break
			}
		}
	}

	if len(results) == 0 {
		return knownContainers, nil
	}

	return results, nil
}

// PartitionNodes isolates the provided containers into a separate bridge network,
// simulating a network partition from the rest of the cluster.
func (c *Client) PartitionNodes(ctx context.Context, nodeNames []string) error {
	if len(nodeNames) < 2 {
		return fmt.Errorf("partition requires at least 2 nodes")
	}

	if c.dryRun {
		log.Printf("[DRY-RUN] Would partition nodes: %s", strings.Join(nodeNames, ","))
		return nil
	}

	type resolvedNode struct {
		name string
		id   string
	}

	resolved := make([]resolvedNode, 0, len(nodeNames))
	for _, n := range nodeNames {
		id, name, err := c.resolveContainer(ctx, n)
		if err != nil {
			return err
		}
		resolved = append(resolved, resolvedNode{name: name, id: id})
	}

	primaryNetwork, err := c.inferPrimaryNetwork(ctx, resolved[0].id)
	if err != nil {
		return err
	}

	partitionNetwork := primaryNetwork + "-partition"
	if err := c.ensureNetwork(ctx, partitionNetwork); err != nil {
		return err
	}

	for _, node := range resolved {
		info, err := c.cli.ContainerInspect(ctx, node.id)
		if err != nil {
			return fmt.Errorf("inspecting %s: %w", node.name, err)
		}

		if info.NetworkSettings == nil || info.NetworkSettings.Networks == nil {
			return fmt.Errorf("container %s has no network settings", node.name)
		}

		if _, ok := info.NetworkSettings.Networks[partitionNetwork]; !ok {
			if err := c.cli.NetworkConnect(ctx, partitionNetwork, node.id, &networkapi.EndpointSettings{}); err != nil {
				return fmt.Errorf("connecting %s to %s: %w", node.name, partitionNetwork, err)
			}
		}

		if _, ok := info.NetworkSettings.Networks[primaryNetwork]; ok {
			if err := c.cli.NetworkDisconnect(ctx, primaryNetwork, node.id, true); err != nil {
				return fmt.Errorf("disconnecting %s from %s: %w", node.name, primaryNetwork, err)
			}
		}
	}

	log.Printf("Partitioned %d node(s): %s", len(resolved), strings.Join(nodeNames, ","))
	return nil
}

// Close releases the Docker client resources.
func (c *Client) Close() error {
	if c.cli != nil {
		return c.cli.Close()
	}
	return nil
}

func (c *Client) resolveContainer(ctx context.Context, serviceName string) (string, string, error) {
	byService, err := c.cli.ContainerList(ctx, containerapi.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", "com.docker.compose.service="+serviceName),
		),
	})
	if err != nil {
		return "", "", fmt.Errorf("resolving service %s: %w", serviceName, err)
	}

	if len(byService) > 0 {
		picked := pickContainer(byService)
		return picked.ID, firstContainerName(picked), nil
	}

	byName, err := c.cli.ContainerList(ctx, containerapi.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("name", serviceName),
		),
	})
	if err != nil {
		return "", "", fmt.Errorf("resolving container name %s: %w", serviceName, err)
	}

	if len(byName) == 0 {
		return "", "", fmt.Errorf("container %s not found", serviceName)
	}

	picked := pickContainer(byName)
	return picked.ID, firstContainerName(picked), nil
}

func pickContainer(list []types.Container) types.Container {
	for _, ctr := range list {
		if ctr.State == "running" {
			return ctr
		}
	}
	return list[0]
}

func firstContainerName(ctr types.Container) string {
	if len(ctr.Names) == 0 {
		if len(ctr.ID) > 12 {
			return ctr.ID[:12]
		}
		return ctr.ID
	}
	return strings.TrimPrefix(ctr.Names[0], "/")
}

func (c *Client) runInContainer(ctx context.Context, containerID string, cmd []string) error {
	execResp, err := c.cli.ContainerExecCreate(ctx, containerID, types.ExecConfig{
		User:         "0",
		Privileged:   true,
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          cmd,
	})
	if err != nil {
		return fmt.Errorf("creating exec in container: %w", err)
	}

	attach, err := c.cli.ContainerExecAttach(ctx, execResp.ID, types.ExecStartCheck{})
	if err != nil {
		return fmt.Errorf("attaching exec: %w", err)
	}
	defer attach.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, attach.Reader); err != nil && err != io.EOF {
		return fmt.Errorf("reading exec output: %w", err)
	}

	inspect, err := c.cli.ContainerExecInspect(ctx, execResp.ID)
	if err != nil {
		return fmt.Errorf("inspecting exec result: %w", err)
	}

	if inspect.ExitCode != 0 {
		out := strings.TrimSpace(stdout.String())
		errOut := strings.TrimSpace(stderr.String())
		if errOut != "" {
			out = errOut
		}
		if out == "" {
			out = "command failed without output"
		}
		return fmt.Errorf("exec exit code %d: %s", inspect.ExitCode, out)
	}

	return nil
}

func (c *Client) inferPrimaryNetwork(ctx context.Context, containerID string) (string, error) {
	info, err := c.cli.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("inspecting container networks: %w", err)
	}

	if info.NetworkSettings == nil || info.NetworkSettings.Networks == nil {
		return "", fmt.Errorf("container has no network settings")
	}

	for netName := range info.NetworkSettings.Networks {
		if strings.Contains(netName, "payflow-net") {
			return netName, nil
		}
	}

	for netName := range info.NetworkSettings.Networks {
		return netName, nil
	}

	return "", fmt.Errorf("no networks found for container")
}

func (c *Client) ensureNetwork(ctx context.Context, networkName string) error {
	_, err := c.cli.NetworkInspect(ctx, networkName, types.NetworkInspectOptions{})
	if err == nil {
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return fmt.Errorf("inspecting network %s: %w", networkName, err)
	}

	_, err = c.cli.NetworkCreate(ctx, networkName, types.NetworkCreate{
		CheckDuplicate: true,
		Driver:         "bridge",
		Attachable:     true,
		Labels: map[string]string{
			"com.payflow.chaos": "true",
		},
	})
	if err != nil {
		return fmt.Errorf("creating network %s: %w", networkName, err)
	}

	return nil
}

func isPayflowContainerName(name string) bool {
	if strings.Contains(name, "coordinator-") {
		return true
	}
	if strings.Contains(name, "worker-") {
		return true
	}
	if strings.Contains(name, "api-gateway") {
		return true
	}
	if strings.Contains(name, "payment-log") {
		return true
	}
	if strings.Contains(name, "monitor") {
		return true
	}
	if strings.Contains(name, "jaeger") {
		return true
	}
	return false
}
