package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

// Config centralizes all tunable parameters and service addresses.
type Config struct {
	// Service Addresses & IDs
	CoordinatorAddr string        `mapstructure:"COORDINATOR_ADDR"`
	LogServiceAddr  string        `mapstructure:"LOG_SERVICE_ADDR"`
	BankAPIAddr     string        `mapstructure:"BANK_API_ADDR"`
	WorkerID        string        `mapstructure:"WORKER_ID"`
	WorkerToken     string        `mapstructure:"WORKER_TOKEN"`
	LogLevel        string        `mapstructure:"LOG_LEVEL"`

	// Ports
	GRPCPort          int           `mapstructure:"GRPC_PORT"`
	MetricsPort       int           `mapstructure:"METRICS_PORT"`
	HealthPort        string        `mapstructure:"HEALTH_PORT"`
	HeartbeatInterval time.Duration `mapstructure:"HEARTBEAT_INTERVAL"`

	// Bank Mock
	BankFailRate     float64 `mapstructure:"BANK_FAIL_RATE"`
	BankLatencyMinMS int     `mapstructure:"BANK_LATENCY_MIN_MS"`
	BankLatencyMaxMS int     `mapstructure:"BANK_LATENCY_MAX_MS"`

	// Concurrency
	MaxConcurrentTasks int           `mapstructure:"MAX_CONCURRENT_TASKS"`
	MaxTaskDuration    time.Duration `mapstructure:"MAX_TASK_DURATION"`
	ShutdownTimeout    time.Duration `mapstructure:"SHUTDOWN_TIMEOUT"`

	// Resilience
	RetryMaxAttempts   int           `mapstructure:"RETRY_MAX_ATTEMPTS"`
	RetryBaseDelay     time.Duration `mapstructure:"RETRY_BASE_DELAY"`
	RetryMaxDelay      time.Duration `mapstructure:"RETRY_MAX_DELAY"`
	CBMaxRequests      uint32        `mapstructure:"CB_MAX_REQUESTS"`
	CBFailureThreshold float64       `mapstructure:"CB_FAILURE_THRESHOLD"`
	CBTimeout          time.Duration `mapstructure:"CB_TIMEOUT"`

	// Stream
	KeepaliveTime     time.Duration `mapstructure:"KEEPALIVE_TIME"`
	KeepaliveTimeout  time.Duration `mapstructure:"KEEPALIVE_TIMEOUT"`
	ConnectRetryDelay time.Duration `mapstructure:"CONNECT_RETRY_DELAY"`

	// Outbox
	OutboxFlushInterval time.Duration `mapstructure:"OUTBOX_FLUSH_INTERVAL"`
	OutboxMaxSize       int           `mapstructure:"OUTBOX_MAX_SIZE"`
	OutboxDBPath        string        `mapstructure:"OUTBOX_DB_PATH"`
	ReservationRedisURL string        `mapstructure:"RESERVATION_REDIS_URL"`
	RequireRedis        bool          `mapstructure:"REQUIRE_REDIS"`

	// Week 3: Retry Engine
	RetryTaskMaxAttempts int `mapstructure:"RETRY_TASK_MAX_ATTEMPTS"`
}

// Load reads configuration using viper.
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Environment overrides
	v.AutomaticEnv()
	// Explicitly bind to ensure Unmarshal picks them up even without defaults
	v.BindEnv("COORDINATOR_ADDR")
	v.BindEnv("LOG_SERVICE_ADDR")
	v.BindEnv("BANK_API_ADDR")
	v.BindEnv("WORKER_ID")
	v.BindEnv("WORKER_TOKEN")

	// Defaults
	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("METRICS_PORT", 9092)
	v.SetDefault("HEALTH_PORT", "8090")
	v.SetDefault("HEARTBEAT_INTERVAL", 2*time.Second)
	v.SetDefault("BANK_FAIL_RATE", 0.10)
	v.SetDefault("BANK_LATENCY_MIN_MS", 50)
	v.SetDefault("BANK_LATENCY_MAX_MS", 500)
	v.SetDefault("MAX_CONCURRENT_TASKS", 10)
	v.SetDefault("MAX_TASK_DURATION", 60*time.Second)
	v.SetDefault("SHUTDOWN_TIMEOUT", 10*time.Second)
	v.SetDefault("RETRY_MAX_ATTEMPTS", 5)
	v.SetDefault("RETRY_BASE_DELAY", 100*time.Millisecond)
	v.SetDefault("RETRY_MAX_DELAY", 30*time.Second)
	v.SetDefault("CB_MAX_REQUESTS", 5)
	v.SetDefault("CB_FAILURE_THRESHOLD", 0.5)
	v.SetDefault("CB_TIMEOUT", 30*time.Second)
	v.SetDefault("KEEPALIVE_TIME", 10*time.Second)
	v.SetDefault("KEEPALIVE_TIMEOUT", 5*time.Second)
	v.SetDefault("CONNECT_RETRY_DELAY", 2*time.Second)
	v.SetDefault("OUTBOX_FLUSH_INTERVAL", 5*time.Second)
	v.SetDefault("OUTBOX_MAX_SIZE", 1000)
	v.SetDefault("OUTBOX_DB_PATH", "/var/data/c3/outbox")
	v.SetDefault("REQUIRE_REDIS", false)
	v.SetDefault("RETRY_TASK_MAX_ATTEMPTS", 3)

	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config file: %w", err)
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

func validate(cfg *Config) error {
	var errs []error

	if cfg.CoordinatorAddr == "" {
		errs = append(errs, errors.New("COORDINATOR_ADDR is required"))
	}
	if cfg.LogServiceAddr == "" {
		errs = append(errs, errors.New("LOG_SERVICE_ADDR is required"))
	}
	if cfg.BankAPIAddr == "" {
		errs = append(errs, errors.New("BANK_API_ADDR is required"))
	}
	if cfg.RetryMaxAttempts < 1 {
		errs = append(errs, errors.New("RETRY_MAX_ATTEMPTS must be >= 1"))
	}
	if cfg.CBFailureThreshold <= 0.0 || cfg.CBFailureThreshold >= 1.0 {
		errs = append(errs, errors.New("CB_FAILURE_THRESHOLD must be between 0.0 and 1.0 exclusive"))
	}
	if cfg.MaxConcurrentTasks < 1 {
		errs = append(errs, errors.New("MAX_CONCURRENT_TASKS must be >= 1"))
	}
	if cfg.ShutdownTimeout < 1*time.Second {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be >= 1s"))
	}
	if cfg.RetryTaskMaxAttempts < 1 {
		errs = append(errs, errors.New("RETRY_TASK_MAX_ATTEMPTS must be >= 1"))
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation failed: %w", errors.Join(errs...))
	}

	return nil
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
