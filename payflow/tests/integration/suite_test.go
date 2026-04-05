// Package integration provides end-to-end tests for PayFlow.
// These tests assume the Docker Compose stack is ALREADY running.
// They do NOT start or stop containers — this keeps tests fast and
// avoids Testcontainers complexity in Week 1.
//
// Run with: go test ./integration/... -v -timeout 5m
package integration

import (
	"log"
	"net"
	"os"
	"testing"
	"time"
)

const (
	// GatewayURL is the base URL for the C1 API Gateway.
	GatewayURL = "http://localhost:8080"

	// MonitorURL is the base URL for the C5 Monitor HTTP server.
	MonitorURL = "http://localhost:3000"

	// MonitorWsURL is the WebSocket endpoint for the C5 Monitor dashboard.
	MonitorWsURL = "ws://localhost:3000/ws"

	// PrometheusURL is the C5 Prometheus metrics endpoint.
	PrometheusURL = "http://localhost:9091"
)

// TestMain is the test suite entry point. It verifies Docker is accessible
// before running any integration tests.
func TestMain(m *testing.M) {
	// Check if Docker is available by attempting to connect to common Docker ports
	// or checking if the compose stack services are reachable
	dockerAvailable := isDockerAvailable()

	if !dockerAvailable {
		log.Println("⚠ Skipping integration tests: Docker not available.")
		log.Println("  Set DOCKER_HOST or start Docker, then run: docker compose up --build -d")
		os.Exit(0)
	}

	log.Println("Integration test suite starting — ensure docker compose stack is running")
	log.Println("  Gateway:    " + GatewayURL)
	log.Println("  Monitor:    " + MonitorURL)
	log.Println("  WebSocket:  " + MonitorWsURL)
	log.Println("  Prometheus: " + PrometheusURL)

	os.Exit(m.Run())
}

// isDockerAvailable checks if Docker and the compose stack are accessible.
// It tries to connect to the gateway port as a proxy for "stack is running".
func isDockerAvailable() bool {
	// First check: can we reach the gateway? (quick check that compose is up)
	conn, err := net.DialTimeout("tcp", "localhost:8080", 3*time.Second)
	if err == nil {
		conn.Close()
		return true
	}

	// Second check: try the monitor port
	conn, err = net.DialTimeout("tcp", "localhost:3000", 3*time.Second)
	if err == nil {
		conn.Close()
		return true
	}

	// Check DOCKER_HOST env var as a fallback signal
	if os.Getenv("DOCKER_HOST") != "" {
		return true
	}

	return false
}
