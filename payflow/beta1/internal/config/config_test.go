package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear relevant env vars to test defaults
	keys := []string{
		"COORDINATOR_ADDR", "LOG_SERVICE_ADDR", "BANK_API_ADDR",
		"MAX_CONCURRENT_TASKS", "SHUTDOWN_TIMEOUT",
	}
	for _, k := range keys {
		os.Unsetenv(k)
	}

	// Set required ones
	t.Setenv("COORDINATOR_ADDR", "localhost:8080")
	t.Setenv("LOG_SERVICE_ADDR", "localhost:8081")
	t.Setenv("BANK_API_ADDR", "localhost:8082")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.MaxConcurrentTasks != 10 {
		t.Errorf("expected default 10, got %d", cfg.MaxConcurrentTasks)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("expected default 10s, got %v", cfg.ShutdownTimeout)
	}
	if cfg.HealthPort != "8090" {
		t.Errorf("expected default 8090, got %s", cfg.HealthPort)
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("COORDINATOR_ADDR", "coord:123")
	t.Setenv("LOG_SERVICE_ADDR", "log:456")
	t.Setenv("BANK_API_ADDR", "bank:789")
	t.Setenv("MAX_CONCURRENT_TASKS", "50")
	t.Setenv("SHUTDOWN_TIMEOUT", "5s")
	t.Setenv("RETRY_MAX_ATTEMPTS", "10")
	t.Setenv("CB_FAILURE_THRESHOLD", "0.8")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.CoordinatorAddr != "coord:123" {
		t.Errorf("expected 'coord:123', got '%s'", cfg.CoordinatorAddr)
	}
	if cfg.MaxConcurrentTasks != 50 {
		t.Errorf("expected 50, got %d", cfg.MaxConcurrentTasks)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("expected 5s, got %v", cfg.ShutdownTimeout)
	}
	if cfg.RetryMaxAttempts != 10 {
		t.Errorf("expected 10, got %d", cfg.RetryMaxAttempts)
	}
	if cfg.CBFailureThreshold != 0.8 {
		t.Errorf("expected 0.8, got %f", cfg.CBFailureThreshold)
	}
}

func TestLoad_Errors(t *testing.T) {
	t.Setenv("COORDINATOR_ADDR", "localhost:8080")
	t.Setenv("LOG_SERVICE_ADDR", "localhost:8081")
	t.Setenv("BANK_API_ADDR", "localhost:8082")

	// Test invalid integer
	t.Setenv("MAX_CONCURRENT_TASKS", "not-an-int")
	_, err := Load()
	if err == nil {
		t.Error("expected error for invalid MAX_CONCURRENT_TASKS, got nil")
	}

	// Test invalid duration
	t.Setenv("MAX_CONCURRENT_TASKS", "10")
	t.Setenv("SHUTDOWN_TIMEOUT", "invalid-duration")
	_, err = Load()
	if err == nil {
		t.Error("expected error for invalid SHUTDOWN_TIMEOUT, got nil")
	}
}
