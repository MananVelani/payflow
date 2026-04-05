// Package helpers provides HTTP and WebSocket test client utilities.
// These helpers simplify common integration test patterns like waiting for
// services to become healthy, asserting HTTP responses, and establishing
// WebSocket connections with retry logic.
package helpers

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// WaitForHTTP polls the given URL every 2 seconds until it receives a successful
// HTTP response or the timeout is exceeded. Returns the final successful response.
// Calls t.Fatalf if the timeout is exceeded without a successful response.
func WaitForHTTP(t *testing.T, url string, timeout time.Duration) *http.Response {
	t.Helper()

	deadline := time.Now().Add(timeout)
	attempt := 0
	client := &http.Client{Timeout: 5 * time.Second}

	for time.Now().Before(deadline) {
		attempt++
		t.Logf("Waiting for %s (attempt %d)...", url, attempt)

		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			t.Logf("✅ %s responded with HTTP %d on attempt %d", url, resp.StatusCode, attempt)
			return resp
		}

		if resp != nil {
			resp.Body.Close()
		}

		time.Sleep(2 * time.Second)
	}

	t.Fatalf("❌ %s did not respond successfully within %s (%d attempts)", url, timeout, attempt)
	return nil // unreachable, but satisfies compiler
}

// AssertHealthy calls WaitForHTTP with a 60-second timeout and verifies
// that the response body contains "status":"ok".
func AssertHealthy(t *testing.T, url string) {
	t.Helper()

	resp := WaitForHTTP(t, url, 60*time.Second)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("❌ %s returned HTTP %d, expected 200", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("❌ %s: failed to read response body: %v", url, err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, `"status":"ok"`) {
		t.Fatalf("❌ %s: response body does not contain '\"status\":\"ok\"', got: %s", url, bodyStr)
	}

	t.Logf("✅ %s is healthy", url)
}

// TCPReachable attempts to establish a TCP connection to the given host:port
// within the specified timeout. Returns true if the connection succeeds.
func TCPReachable(host string, port string, timeout time.Duration) bool {
	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// ConnectWebSocket establishes a WebSocket connection to the given URL,
// retrying every 2 seconds until the timeout is exceeded.
// Calls t.Fatalf if the connection cannot be established within the timeout.
func ConnectWebSocket(t *testing.T, url string, timeout time.Duration) *websocket.Conn {
	t.Helper()

	deadline := time.Now().Add(timeout)
	attempt := 0
	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}

	for time.Now().Before(deadline) {
		attempt++
		t.Logf("Connecting to WebSocket %s (attempt %d)...", url, attempt)

		conn, resp, err := dialer.Dial(url, nil)
		if err == nil {
			t.Logf("✅ WebSocket connected to %s on attempt %d", url, attempt)
			if resp != nil {
				resp.Body.Close()
			}
			return conn
		}

		if resp != nil {
			resp.Body.Close()
		}

		t.Logf("WebSocket dial attempt %d failed: %v", attempt, err)
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("❌ Could not connect WebSocket to %s within %s (%d attempts)", url, timeout, attempt)
	return nil // unreachable
}

// FormatResult returns a formatted pass/fail string for a connectivity check.
func FormatResult(host string, port string, reachable bool) string {
	if reachable {
		return fmt.Sprintf("✅ %s:%s reachable", host, port)
	}
	return fmt.Sprintf("❌ %s:%s unreachable", host, port)
}

// AssertJSONField unmarshals body into a JSON object and asserts that the given
// field exists and matches the expected value. Supports string, bool, and numeric
// (float64) comparisons. Uses fmt.Sprintf for numeric comparison to avoid float
// precision issues.
func AssertJSONField(t *testing.T, body []byte, field string, expected interface{}) {
	t.Helper()

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("AssertJSONField: invalid JSON: %v", err)
		return
	}

	actual, ok := data[field]
	if !ok {
		t.Errorf("field %q not found in JSON", field)
		return
	}

	match := false
	switch exp := expected.(type) {
	case string:
		if actualStr, ok := actual.(string); ok {
			match = actualStr == exp
		}
	case bool:
		if actualBool, ok := actual.(bool); ok {
			match = actualBool == exp
		}
	case int:
		// JSON numbers are float64
		if actualNum, ok := actual.(float64); ok {
			match = fmt.Sprintf("%.0f", actualNum) == fmt.Sprintf("%d", exp)
		}
	case float64:
		if actualNum, ok := actual.(float64); ok {
			match = fmt.Sprintf("%g", actualNum) == fmt.Sprintf("%g", exp)
		}
	default:
		match = fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
	}

	if !match {
		t.Errorf("field %q: got %v, want %v", field, actual, expected)
		return
	}

	t.Logf("✅ JSON field %q = %v", field, actual)
}
