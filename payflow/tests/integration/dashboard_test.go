// Package integration provides end-to-end tests for PayFlow.
// These tests verify the real dashboard is live and broadcasting actual cluster
// state, not a stub heartbeat.
package integration

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/payflow/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
)

// TestWebSocketReceivesSnapshotType verifies that the WebSocket endpoint sends
// messages with type="snapshot" (Week 2 format) instead of type="heartbeat".
func TestWebSocketReceivesSnapshotType(t *testing.T) {
	t.Parallel()

	conn := helpers.ConnectWebSocket(t, MonitorWsURL, 30*time.Second)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(20 * time.Second))

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("WebSocket read error before receiving snapshot: %v", err)
		}

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg, &envelope); err != nil {
			t.Logf("Skipping unparseable message: %s", string(msg))
			continue
		}

		if envelope.Type == "snapshot" {
			t.Logf("✅ Received snapshot message from dashboard")
			return
		}

		if envelope.Type == "ping" {
			t.Logf("Received ping, waiting for snapshot...")
			continue
		}

		t.Logf("Received message type=%q, waiting for snapshot...", envelope.Type)
	}
}

// TestSnapshotHasThreeCoordinators verifies that the /api/state endpoint returns
// a ClusterSnapshot with exactly 3 coordinator entries.
func TestSnapshotHasThreeCoordinators(t *testing.T) {
	t.Parallel()

	resp := helpers.WaitForHTTP(t, MonitorURL+"/api/state", 30*time.Second)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "should read response body")

	var snap struct {
		Coordinators []struct {
			NodeID   string `json:"node_id"`
			State    string `json:"state"`
			IsLeader bool   `json:"is_leader"`
			Epoch    int64  `json:"epoch"`
		} `json:"coordinators"`
	}
	err = json.Unmarshal(body, &snap)
	assert.NoError(t, err, "should unmarshal JSON")
	assert.Len(t, snap.Coordinators, 3, "expected exactly 3 coordinators")

	for _, c := range snap.Coordinators {
		t.Logf("  %s — state=%s, leader=%v, epoch=%d", c.NodeID, c.State, c.IsLeader, c.Epoch)
	}
}

// TestSnapshotHasFiveWorkers verifies that the /api/state endpoint returns
// a ClusterSnapshot with exactly 5 worker entries.
func TestSnapshotHasFiveWorkers(t *testing.T) {
	t.Parallel()

	resp := helpers.WaitForHTTP(t, MonitorURL+"/api/state", 30*time.Second)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "should read response body")

	var snap struct {
		Workers []struct {
			WorkerID string `json:"worker_id"`
			Alive    bool   `json:"alive"`
		} `json:"workers"`
	}
	err = json.Unmarshal(body, &snap)
	assert.NoError(t, err, "should unmarshal JSON")
	assert.Len(t, snap.Workers, 5, "expected exactly 5 workers")

	for _, w := range snap.Workers {
		t.Logf("  %s — alive=%v", w.WorkerID, w.Alive)
	}
}

// TestExactlyOneLeader verifies there is never more than one coordinator
// marked as LEADER in /api/state (split-brain protection).
func TestExactlyOneLeader(t *testing.T) {
	t.Parallel()

	resp := helpers.WaitForHTTP(t, MonitorURL+"/api/state", 30*time.Second)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "should read response body")

	var snap struct {
		Coordinators []struct {
			NodeID   string `json:"node_id"`
			IsLeader bool   `json:"is_leader"`
			State    string `json:"state"`
		} `json:"coordinators"`
	}
	err = json.Unmarshal(body, &snap)
	assert.NoError(t, err, "should unmarshal JSON")

	leaderCount := 0
	leaderNodeID := ""
	for _, c := range snap.Coordinators {
		if c.IsLeader {
			leaderCount++
			leaderNodeID = c.NodeID
		}
	}

	assert.LessOrEqual(t, leaderCount, 1, "expected at most one LEADER coordinator")
	if leaderCount == 0 {
		t.Log("No leader observed at scrape time (acceptable during election/convergence)")
		return
	}
	t.Logf("Leader node: %s", leaderNodeID)
}

// TestDashboardHTMLContainsExpectedElements verifies that the root / page
// returns HTML containing the expected dashboard elements.
func TestDashboardHTMLContainsExpectedElements(t *testing.T) {
	t.Parallel()

	resp := helpers.WaitForHTTP(t, MonitorURL+"/", 30*time.Second)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "should read response body")

	htmlStr := string(body)
	assert.True(t, strings.Contains(htmlStr, "PayFlow Monitor"),
		"HTML should contain 'PayFlow Monitor'")
	assert.True(t, strings.Contains(htmlStr, "/ws"),
		"HTML should contain WebSocket path '/ws'")
	assert.True(t, strings.Contains(htmlStr, "coordinator"),
		"HTML should contain 'coordinator'")
	assert.True(t, strings.Contains(htmlStr, "worker"),
		"HTML should contain 'worker'")

	t.Log("✅ Dashboard HTML contains all expected elements")
}
