package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
)

type Config struct {
	CoordinatorAddr    string        `mapstructure:"COORDINATOR_ADDR"`
	LogServiceAddr     string        `mapstructure:"LOG_SERVICE_ADDR"`
	BankAPIAddr        string        `mapstructure:"BANK_API_ADDR"`
	WorkerID           string        `mapstructure:"WORKER_ID"`
	GRPCPort           int           `mapstructure:"GRPC_PORT"`
	MetricsPort        int           `mapstructure:"METRICS_PORT"`
	MaxConcurrentTasks int           `mapstructure:"MAX_CONCURRENT_TASKS"`
	HeartbeatInterval  time.Duration `mapstructure:"HEARTBEAT_INTERVAL"`
	BankFailRate       float64       `mapstructure:"BANK_FAIL_RATE"`
	BankLatencyMinMS   int           `mapstructure:"BANK_LATENCY_MIN_MS"`
	BankLatencyMaxMS   int           `mapstructure:"BANK_LATENCY_MAX_MS"`
	RetryMaxAttempts   int           `mapstructure:"RETRY_MAX_ATTEMPTS"`
	RetryBaseDelayMS   int           `mapstructure:"RETRY_BASE_DELAY_MS"`
	LogLevel           string        `mapstructure:"LOG_LEVEL"`
	Hostname           string        // Populated from system
}

var envKeys = []string{
	"COORDINATOR_ADDR",
	"LOG_SERVICE_ADDR",
	"BANK_API_ADDR",
	"WORKER_ID",
	"GRPC_PORT",
	"METRICS_PORT",
	"MAX_CONCURRENT_TASKS",
	"HEARTBEAT_INTERVAL",
	"BANK_FAIL_RATE",
	"BANK_LATENCY_MIN_MS",
	"BANK_LATENCY_MAX_MS",
	"RETRY_MAX_ATTEMPTS",
	"RETRY_BASE_DELAY_MS",
	"LOG_LEVEL",
}

func Load() (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("GRPC_PORT", 0)
	v.SetDefault("METRICS_PORT", 9092)
	v.SetDefault("MAX_CONCURRENT_TASKS", 5)
	v.SetDefault("HEARTBEAT_INTERVAL", "2s")
	v.SetDefault("BANK_FAIL_RATE", 0.10)
	v.SetDefault("BANK_LATENCY_MIN_MS", 50)
	v.SetDefault("BANK_LATENCY_MAX_MS", 500)
	v.SetDefault("RETRY_MAX_ATTEMPTS", 3)
	v.SetDefault("RETRY_BASE_DELAY_MS", 1000)
	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("WORKER_ID", "")

	// AutomaticEnv + replacer handles keys with dots
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Explicit bindings so Viper reads the exact uppercase env var names
	// regardless of its internal case-folding behaviour
	for _, key := range envKeys {
		_ = v.BindEnv(key)
	}

	// Optional .env file — ignored if absent
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	_ = v.ReadInConfig()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// WORKER_ID fallback — generate a short stable-ish UUID suffix
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-" + uuid.New().String()[:8]
	}

	// Populate Hostname from system
	host, _ := os.Hostname()
	if host == "" {
		host = "localhost"
	}
	cfg.Hostname = host

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("configuration: %w", err)
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	required := []struct {
		name string
		val  string
	}{
		{"COORDINATOR_ADDR", c.CoordinatorAddr},
		{"LOG_SERVICE_ADDR", c.LogServiceAddr},
		{"BANK_API_ADDR", c.BankAPIAddr},
	}
	for _, r := range required {
		if r.val == "" {
			return fmt.Errorf("required env var %s is not set", r.name)
		}
	}
	return nil
}
