// Package scaling implements dynamic worker autoscaling for the C5 monitor.
// It observes queue depth snapshots and starts new worker containers via Docker API.
package scaling

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	dockerclient "github.com/docker/docker/client"
	"github.com/payflow/monitor/scraper"
)

var workerIDPattern = regexp.MustCompile(`worker-(\d+)`)

// DockerAPI abstracts Docker operations needed by autoscaling.
type DockerAPI interface {
	RunningWorkerCount(ctx context.Context) (int, error)
	NextWorkerID(ctx context.Context) (int, error)
	LaunchWorker(ctx context.Context, workerID int) (string, error)
	Close() error
}

// DockerClient is a Docker SDK backed implementation used by the monitor service.
type DockerClient struct {
	cli            *dockerclient.Client
	templateWorker string
}

// NewDockerClient creates and validates a Docker SDK client.
func NewDockerClient(templateWorker string) (*DockerClient, error) {
	cli, err := dockerclient.NewClientWithOpts(
		dockerclient.FromEnv,
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating docker client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := cli.Ping(ctx); err != nil {
		return nil, fmt.Errorf("docker daemon unreachable: %w", err)
	}

	if strings.TrimSpace(templateWorker) == "" {
		templateWorker = "worker-1"
	}

	return &DockerClient{
		cli:            cli,
		templateWorker: templateWorker,
	}, nil
}

// Close releases the underlying Docker client.
func (d *DockerClient) Close() error {
	if d.cli != nil {
		return d.cli.Close()
	}
	return nil
}

// RunningWorkerCount returns the number of currently running worker containers.
func (d *DockerClient) RunningWorkerCount(ctx context.Context) (int, error) {
	ids, err := d.workerIDs(ctx, true)
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// NextWorkerID returns the next worker numeric ID that does not currently exist.
func (d *DockerClient) NextWorkerID(ctx context.Context) (int, error) {
	ids, err := d.workerIDs(ctx, false)
	if err != nil {
		return 0, err
	}

	maxID := 0
	for id := range ids {
		if id > maxID {
			maxID = id
		}
	}
	if maxID == 0 {
		return 1, nil
	}
	return maxID + 1, nil
}

// LaunchWorker starts a new worker container cloned from the template worker.
func (d *DockerClient) LaunchWorker(ctx context.Context, workerID int) (string, error) {
	if workerID <= 0 {
		return "", fmt.Errorf("worker ID must be positive")
	}

	templateContainer, err := d.findTemplateWorker(ctx)
	if err != nil {
		return "", err
	}

	templateInspect, err := d.cli.ContainerInspect(ctx, templateContainer.ID)
	if err != nil {
		return "", fmt.Errorf("inspecting template worker %s: %w", d.templateWorker, err)
	}

	newName := fmt.Sprintf("worker-%d", workerID)
	cfg := cloneWorkerConfig(templateInspect.Config, workerID)
	hostCfg := &container.HostConfig{}
	if templateInspect.HostConfig != nil {
		hostCfg.RestartPolicy = templateInspect.HostConfig.RestartPolicy
	}

	if networkName := preferredNetworkName(templateInspect); networkName != "" {
		hostCfg.NetworkMode = container.NetworkMode(networkName)
	}

	resp, err := d.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, newName)
	if err != nil {
		return "", fmt.Errorf("creating %s: %w", newName, err)
	}

	if err := d.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("starting %s: %w", newName, err)
	}

	log.Printf("[scaler] started container %s (id %.12s)", newName, resp.ID)
	return newName, nil
}

func (d *DockerClient) workerIDs(ctx context.Context, runningOnly bool) (map[int]struct{}, error) {
	filterArgs := filters.NewArgs()
	if runningOnly {
		filterArgs.Add("status", "running")
	}

	containers, err := d.cli.ContainerList(ctx, container.ListOptions{All: !runningOnly, Filters: filterArgs})
	if err != nil {
		return nil, fmt.Errorf("listing containers: %w", err)
	}

	ids := make(map[int]struct{})
	for _, c := range containers {
		for _, rawName := range c.Names {
			clean := strings.TrimPrefix(rawName, "/")
			if id, ok := extractWorkerID(clean); ok {
				ids[id] = struct{}{}
			}
		}
	}

	return ids, nil
}

func (d *DockerClient) findTemplateWorker(ctx context.Context) (types.Container, error) {
	containers, err := d.cli.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("status", "running"),
			filters.Arg("name", d.templateWorker),
		),
	})
	if err != nil {
		return types.Container{}, fmt.Errorf("listing template worker %s: %w", d.templateWorker, err)
	}
	if len(containers) == 0 {
		return types.Container{}, fmt.Errorf("template worker %q not running", d.templateWorker)
	}

	return containers[0], nil
}

func cloneWorkerConfig(base *container.Config, workerID int) *container.Config {
	if base == nil {
		return &container.Config{}
	}

	env := make([]string, len(base.Env))
	copy(env, base.Env)
	env = setEnvValue(env, "WORKER_ID", strconv.Itoa(workerID))
	env = setEnvValue(env, "OTEL_SERVICE_NAME", fmt.Sprintf("worker-%d", workerID))

	labels := make(map[string]string, len(base.Labels)+1)
	for k, v := range base.Labels {
		labels[k] = v
	}
	labels["com.payflow.autoscaled"] = "true"

	return &container.Config{
		Hostname:        base.Hostname,
		Domainname:      base.Domainname,
		User:            base.User,
		AttachStdin:     base.AttachStdin,
		AttachStdout:    base.AttachStdout,
		AttachStderr:    base.AttachStderr,
		ExposedPorts:    base.ExposedPorts,
		Tty:             base.Tty,
		OpenStdin:       base.OpenStdin,
		StdinOnce:       base.StdinOnce,
		Env:             env,
		Cmd:             base.Cmd,
		Healthcheck:     base.Healthcheck,
		ArgsEscaped:     base.ArgsEscaped,
		Image:           base.Image,
		Volumes:         base.Volumes,
		WorkingDir:      base.WorkingDir,
		Entrypoint:      base.Entrypoint,
		NetworkDisabled: base.NetworkDisabled,
		Labels:          labels,
		StopSignal:      base.StopSignal,
		StopTimeout:     base.StopTimeout,
		Shell:           base.Shell,
	}
}

func setEnvValue(env []string, key, value string) []string {
	needle := key + "="
	for i, kv := range env {
		if strings.HasPrefix(kv, needle) {
			env[i] = needle + value
			return env
		}
	}
	return append(env, needle+value)
}

func preferredNetworkName(inspect types.ContainerJSON) string {
	if inspect.NetworkSettings == nil || len(inspect.NetworkSettings.Networks) == 0 {
		return ""
	}

	if _, ok := inspect.NetworkSettings.Networks["payflow-net"]; ok {
		return "payflow-net"
	}

	for name := range inspect.NetworkSettings.Networks {
		return name
	}

	return ""
}

func extractWorkerID(containerName string) (int, bool) {
	match := workerIDPattern.FindStringSubmatch(containerName)
	if len(match) != 2 {
		return 0, false
	}
	id, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return id, true
}

// Scaler observes queue depth snapshots and scales workers when needed.
type Scaler struct {
	docker         DockerAPI
	queueThreshold int64
	maxWorkers     int
	cooldown       time.Duration
	snapshots      chan scraper.ClusterSnapshot

	mu          sync.Mutex
	lastScaleAt time.Time
}

// New creates a new Scaler instance.
func New(docker DockerAPI, queueThreshold int64, maxWorkers int, cooldown time.Duration) *Scaler {
	if queueThreshold <= 0 {
		queueThreshold = 50
	}
	if maxWorkers <= 0 {
		maxWorkers = 10
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}

	return &Scaler{
		docker:         docker,
		queueThreshold: queueThreshold,
		maxWorkers:     maxWorkers,
		cooldown:       cooldown,
		snapshots:      make(chan scraper.ClusterSnapshot, 1),
	}
}

// OnSnapshot receives cluster snapshots from the monitor scraper.
func (s *Scaler) OnSnapshot(snap scraper.ClusterSnapshot) {
	select {
	case s.snapshots <- snap:
	default:
		// Keep only the latest snapshot if processing lags.
		select {
		case <-s.snapshots:
		default:
		}
		select {
		case s.snapshots <- snap:
		default:
		}
	}
}

// Start runs the autoscaling event loop until context cancellation.
func (s *Scaler) Start(ctx context.Context) {
	log.Printf("[scaler] enabled (queue>%d, max_workers=%d, cooldown=%s)", s.queueThreshold, s.maxWorkers, s.cooldown)
	for {
		select {
		case <-ctx.Done():
			log.Println("[scaler] stopped")
			return
		case snap := <-s.snapshots:
			s.processSnapshot(ctx, snap)
		}
	}
}

func (s *Scaler) processSnapshot(ctx context.Context, snap scraper.ClusterSnapshot) {
	queueDepth := snap.TotalQueueDepth()
	if queueDepth <= s.queueThreshold {
		return
	}

	s.mu.Lock()
	if !s.lastScaleAt.IsZero() && time.Since(s.lastScaleAt) < s.cooldown {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	runningWorkers, err := s.docker.RunningWorkerCount(ctx)
	if err != nil {
		log.Printf("[scaler] worker count failed: %v", err)
		return
	}
	if runningWorkers >= s.maxWorkers {
		return
	}

	nextWorkerID, err := s.docker.NextWorkerID(ctx)
	if err != nil {
		log.Printf("[scaler] next worker id failed: %v", err)
		return
	}

	name, err := s.docker.LaunchWorker(ctx, nextWorkerID)
	if err != nil {
		log.Printf("[scaler] launch worker-%d failed: %v", nextWorkerID, err)
		return
	}

	s.mu.Lock()
	s.lastScaleAt = time.Now()
	s.mu.Unlock()

	log.Printf("[scaler] queue_depth=%d triggered scale-up: launched %s", queueDepth, name)
}
