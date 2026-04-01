package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config centralizes all tunable parameters and service addresses.
type Config struct {
	// Service Addresses & IDs
	CoordinatorAddr string
	LogServiceAddr  string
	BankAPIAddr     string
	WorkerID        string
	WorkerToken     string
	LogLevel        string

	// Ports
	GRPCPort          int
	MetricsPort       int
	HealthPort        string
	HeartbeatInterval time.Duration

	// Bank Mock (Production config usually comes from secret manager, but keeping for parity)
	BankFailRate     float64
	BankLatencyMinMS int
	BankLatencyMaxMS int

	// Concurrency
	MaxConcurrentTasks int
	MaxTaskDuration    time.Duration
	ShutdownTimeout    time.Duration

	// Resilience
	RetryMaxAttempts   int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	CBMaxRequests      uint32
	CBFailureThreshold float64
	CBTimeout          time.Duration

	// Stream
	KeepaliveTime     time.Duration
	KeepaliveTimeout  time.Duration
	ConnectRetryDelay time.Duration

	// Outbox
	OutboxFlushInterval time.Duration
	OutboxMaxSize       int
	OutboxDBPath        string
	ReservationRedisURL string
}



// Load reads simplified environment variables into the Config struct.
// It uses only the standard library and returns a descriptive error on failure.
func Load() (*Config, error) {
	cfg := &Config{}

	// Addresses & IDs
	cfg.CoordinatorAddr = getEnv("COORDINATOR_ADDR", "")
	cfg.LogServiceAddr = getEnv("LOG_SERVICE_ADDR", "")
	cfg.BankAPIAddr = getEnv("BANK_API_ADDR", "")
	cfg.WorkerID = getEnv("WORKER_ID", "")
	cfg.WorkerToken = getEnv("WORKER_TOKEN", "")
	cfg.LogLevel = getEnv("LOG_LEVEL", "info")

	// Required addresses
	if cfg.CoordinatorAddr == "" {
		return nil, fmt.Errorf("config error: COORDINATOR_ADDR is required")
	}
	if cfg.LogServiceAddr == "" {
		return nil, fmt.Errorf("config error: LOG_SERVICE_ADDR is required")
	}
	if cfg.BankAPIAddr == "" {
		return nil, fmt.Errorf("config error: BANK_API_ADDR is required")
	}

	// Ports
	var err error
	if cfg.GRPCPort, err = getIntEnv("GRPC_PORT", 0); err != nil {
		return nil, fmt.Errorf("config error: GRPC_PORT: %w", err)
	}
	if cfg.MetricsPort, err = getIntEnv("METRICS_PORT", 9092); err != nil {
		return nil, fmt.Errorf("config error: METRICS_PORT: %w", err)
	}
	cfg.HealthPort = getEnv("HEALTH_PORT", "8090")
	if cfg.HeartbeatInterval, err = getDurationEnv("HEARTBEAT_INTERVAL", 2*time.Second); err != nil {
		return nil, fmt.Errorf("config error: HEARTBEAT_INTERVAL: %w", err)
	}

	// Bank Mock
	if cfg.BankFailRate, err = getFloatEnv("BANK_FAIL_RATE", 0.10); err != nil {
		return nil, fmt.Errorf("config error: BANK_FAIL_RATE: %w", err)
	}
	if cfg.BankLatencyMinMS, err = getIntEnv("BANK_LATENCY_MIN_MS", 50); err != nil {
		return nil, fmt.Errorf("config error: BANK_LATENCY_MIN_MS: %w", err)
	}
	if cfg.BankLatencyMaxMS, err = getIntEnv("BANK_LATENCY_MAX_MS", 500); err != nil {
		return nil, fmt.Errorf("config error: BANK_LATENCY_MAX_MS: %w", err)
	}

	// Concurrency
	if cfg.MaxConcurrentTasks, err = getIntEnv("MAX_CONCURRENT_TASKS", 10); err != nil {
		return nil, fmt.Errorf("config error: MAX_CONCURRENT_TASKS: %w", err)
	}
	if cfg.MaxTaskDuration, err = getDurationEnv("MAX_TASK_DURATION", 60*time.Second); err != nil {
		return nil, fmt.Errorf("config error: MAX_TASK_DURATION: %w", err)
	}
	if cfg.ShutdownTimeout, err = getDurationEnv("SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return nil, fmt.Errorf("config error: SHUTDOWN_TIMEOUT: %w", err)
	}

	// Resilience
	if cfg.RetryMaxAttempts, err = getIntEnv("RETRY_MAX_ATTEMPTS", 5); err != nil {
		return nil, fmt.Errorf("config error: RETRY_MAX_ATTEMPTS: %w", err)
	}
	if cfg.RetryBaseDelay, err = getDurationEnv("RETRY_BASE_DELAY", 100*time.Millisecond); err != nil {
		return nil, fmt.Errorf("config error: RETRY_BASE_DELAY: %w", err)
	}
	if cfg.RetryMaxDelay, err = getDurationEnv("RETRY_MAX_DELAY", 30*time.Second); err != nil {
		return nil, fmt.Errorf("config error: RETRY_MAX_DELAY: %w", err)
	}
	if val, err := getIntEnv("CB_MAX_REQUESTS", 5); err != nil {
		return nil, fmt.Errorf("config error: CB_MAX_REQUESTS: %w", err)
	} else {
		cfg.CBMaxRequests = uint32(val)
	}
	if cfg.CBFailureThreshold, err = getFloatEnv("CB_FAILURE_THRESHOLD", 0.5); err != nil {
		return nil, fmt.Errorf("config error: CB_FAILURE_THRESHOLD: %w", err)
	}
	if cfg.CBTimeout, err = getDurationEnv("CB_TIMEOUT", 30*time.Second); err != nil {
		return nil, fmt.Errorf("config error: CB_TIMEOUT: %w", err)
	}

	// Stream
	if cfg.KeepaliveTime, err = getDurationEnv("KEEPALIVE_TIME", 10*time.Second); err != nil {
		return nil, fmt.Errorf("config error: KEEPALIVE_TIME: %w", err)
	}
	if cfg.KeepaliveTimeout, err = getDurationEnv("KEEPALIVE_TIMEOUT", 5*time.Second); err != nil {
		return nil, fmt.Errorf("config error: KEEPALIVE_TIMEOUT: %w", err)
	}
	if cfg.ConnectRetryDelay, err = getDurationEnv("CONNECT_RETRY_DELAY", 2*time.Second); err != nil {
		return nil, fmt.Errorf("config error: CONNECT_RETRY_DELAY: %w", err)
	}

	// Outbox
	if cfg.OutboxFlushInterval, err = getDurationEnv("OUTBOX_FLUSH_INTERVAL", 5*time.Second); err != nil {
		return nil, fmt.Errorf("config error: OUTBOX_FLUSH_INTERVAL: %w", err)
	}
	if cfg.OutboxMaxSize, err = getIntEnv("OUTBOX_MAX_SIZE", 1000); err != nil {
		return nil, fmt.Errorf("config error: OUTBOX_MAX_SIZE: %w", err)
	}
	cfg.OutboxDBPath = getEnv("OUTBOX_DB_PATH", "/var/data/c3/outbox")
	cfg.ReservationRedisURL = getEnv("RESERVATION_REDIS_URL", "")



	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getIntEnv(key string, fallback int) (int, error) {
	valStr, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, fmt.Errorf("invalid integer: %s", valStr)
	}
	return val, nil
}

func getFloatEnv(key string, fallback float64) (float64, error) {
	valStr, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid float: %s", valStr)
	}
	return val, nil
}

func getDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	valStr, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	val, err := time.ParseDuration(valStr)
	if err != nil {
		return 0, fmt.Errorf("invalid duration: %s (expected e.g. 10s, 100ms)", valStr)
	}
	return val, nil
}
