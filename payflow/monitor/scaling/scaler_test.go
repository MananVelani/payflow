package scaling

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/payflow/monitor/scraper"
)

type fakeDocker struct {
	running   int
	nextID    int
	launched  []int
	launchErr error
}

func (f *fakeDocker) RunningWorkerCount(context.Context) (int, error) {
	return f.running, nil
}

func (f *fakeDocker) NextWorkerID(context.Context) (int, error) {
	if f.nextID > 0 {
		return f.nextID, nil
	}
	return f.running + 1, nil
}

func (f *fakeDocker) LaunchWorker(_ context.Context, workerID int) (string, error) {
	if f.launchErr != nil {
		return "", f.launchErr
	}
	f.launched = append(f.launched, workerID)
	f.running++
	f.nextID = workerID + 1
	return fmt.Sprintf("worker-%d", workerID), nil
}

func (f *fakeDocker) Close() error {
	return nil
}

func snapshotWithQueueDepth(depth int64) scraper.ClusterSnapshot {
	return scraper.ClusterSnapshot{
		Coordinators: []scraper.CoordinatorState{
			{
				NodeID:     "coordinator-1",
				IsLeader:   true,
				QueueDepth: depth,
			},
		},
	}
}

func TestProcessSnapshotBelowThresholdNoScale(t *testing.T) {
	fd := &fakeDocker{running: 5, nextID: 6}
	s := New(fd, 50, 10, 5*time.Second)

	s.processSnapshot(context.Background(), snapshotWithQueueDepth(50))

	if len(fd.launched) != 0 {
		t.Fatalf("expected no launched workers, got %v", fd.launched)
	}
}

func TestProcessSnapshotAboveThresholdScales(t *testing.T) {
	fd := &fakeDocker{running: 5, nextID: 6}
	s := New(fd, 50, 10, 5*time.Second)

	s.processSnapshot(context.Background(), snapshotWithQueueDepth(80))

	if len(fd.launched) != 1 {
		t.Fatalf("expected one launch, got %d", len(fd.launched))
	}
	if fd.launched[0] != 6 {
		t.Fatalf("expected worker-6 launch, got worker-%d", fd.launched[0])
	}
}

func TestProcessSnapshotHonorsMaxWorkers(t *testing.T) {
	fd := &fakeDocker{running: 10, nextID: 11}
	s := New(fd, 50, 10, 5*time.Second)

	s.processSnapshot(context.Background(), snapshotWithQueueDepth(120))

	if len(fd.launched) != 0 {
		t.Fatalf("expected no launch at max workers, got %v", fd.launched)
	}
}

func TestProcessSnapshotHonorsCooldown(t *testing.T) {
	fd := &fakeDocker{running: 5, nextID: 6}
	s := New(fd, 50, 10, 2*time.Minute)

	s.processSnapshot(context.Background(), snapshotWithQueueDepth(80))
	s.processSnapshot(context.Background(), snapshotWithQueueDepth(90))

	if len(fd.launched) != 1 {
		t.Fatalf("expected one launch due to cooldown, got %d", len(fd.launched))
	}
}

func TestProcessSnapshotLaunchFailureDoesNotSetCooldown(t *testing.T) {
	fd := &fakeDocker{running: 5, nextID: 6, launchErr: fmt.Errorf("boom")}
	s := New(fd, 50, 10, 2*time.Minute)

	s.processSnapshot(context.Background(), snapshotWithQueueDepth(80))

	if !s.lastScaleAt.IsZero() {
		t.Fatalf("expected lastScaleAt to remain zero on launch failure")
	}
}
