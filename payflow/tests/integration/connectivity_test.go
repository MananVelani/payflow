// Package integration provides end-to-end tests for PayFlow.
package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/payflow/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
)

// TestAllServicesHealthy verifies that the API Gateway and Monitor services
// both respond to /health with HTTP 200 and a JSON body containing "status":"ok".
func TestAllServicesHealthy(t *testing.T) {
	t.Parallel()

	t.Run("api-gateway", func(t *testing.T) {
		t.Parallel()
		helpers.AssertHealthy(t, GatewayURL+"/health")
		t.Log("C1 API Gateway is healthy")
	})

	t.Run("monitor", func(t *testing.T) {
		t.Parallel()
		helpers.AssertHealthy(t, MonitorURL+"/health")
		t.Log("C5 Monitor is healthy")
	})
}

// TestMonitorWebSocketConnects verifies that the Monitor WebSocket endpoint
// accepts connections and sends dashboard messages (snapshot or ping).
func TestMonitorWebSocketConnects(t *testing.T) {
	conn := helpers.ConnectWebSocket(t, MonitorWsURL, 30*time.Second)
	defer conn.Close()

	// Dashboard emits either "snapshot" (on scrape) or "ping" (every 5s).
	conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	for {
		_, msg, err := conn.ReadMessage()
		assert.NoError(t, err, "should receive a WebSocket message")

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg, &envelope); err != nil {
			t.Logf("Skipping unparseable WebSocket frame: %s", string(msg))
			continue
		}

		if envelope.Type == "snapshot" || envelope.Type == "ping" {
			t.Logf("Received WebSocket message type=%s", envelope.Type)
			return
		}
	}
}

// TestPrometheusMetricsExposed verifies that the C5 Prometheus /metrics endpoint
// returns HTTP 200 and contains the expected PayFlow monitor metrics.
func TestPrometheusMetricsExposed(t *testing.T) {
	resp := helpers.WaitForHTTP(t, PrometheusURL+"/metrics", 30*time.Second)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Prometheus /metrics should return 200")

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "should be able to read response body")

	bodyStr := string(body)
	assert.Contains(t, bodyStr, "payflow_monitor_scrape_duration_seconds",
		"metrics should contain scrape_duration_seconds histogram")
	assert.Contains(t, bodyStr, "payflow_monitor_target_up",
		"metrics should contain target_up gauge")

	t.Log("✅ Prometheus metrics endpoint contains expected PayFlow metrics")
}

// TestPayflowNetworkConnectivity checks that all critical PayFlow service ports
// are reachable via TCP. Uses t.Errorf (not t.Fatalf) so ALL results are reported
// even if some ports are unreachable.
func TestPayflowNetworkConnectivity(t *testing.T) {
	type endpoint struct {
		host string
		port string
		desc string
	}

	endpoints := []endpoint{
		{"localhost", "50051", "coordinator-1 gRPC"},
		{"localhost", "50052", "coordinator-2 gRPC"},
		{"localhost", "50053", "coordinator-3 gRPC"},
		{"localhost", "50054", "payment-log gRPC"},
		{"localhost", "8080", "api-gateway HTTP"},
		{"localhost", "3000", "monitor HTTP"},
		{"localhost", "9999", "mock-bank HTTP"},
	}

	failCount := 0
	for _, ep := range endpoints {
		reachable := helpers.TCPReachable(ep.host, ep.port, 5*time.Second)
		result := helpers.FormatResult(ep.host, ep.port, reachable)

		if !reachable {
			failCount++
			t.Errorf("%s — %s is NOT reachable", result, ep.desc)
		} else {
			t.Logf("%s — %s", result, ep.desc)
		}
	}

	if failCount > 0 {
		t.Logf("❌ %d/%d endpoints unreachable", failCount, len(endpoints))
		// Run: docker compose logs <service> to debug
		t.Log("Debug: run 'docker compose ps' and 'docker compose logs <service>' to investigate")
	} else {
		t.Logf("✅ All %d endpoints reachable", len(endpoints))
	}

	// Check internal endpoints (these use Docker hostnames, not localhost)
	internalNote := `
Note: coordinator-1:50051, coordinator-2:50052, coordinator-3:50053, 
payment-log:50054 are tested via localhost port mappings above.
Internal Docker network connectivity between containers is verified 
by the services' own health checks and the monitor's scraper.`
	t.Log(strings.TrimSpace(internalNote))
}
