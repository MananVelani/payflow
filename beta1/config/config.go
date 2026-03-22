package config

import (
	"fmt"
	"strings"
	"time"

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
}

func Load() (*Config, error) {
	v := viper.New()
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

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	_ = v.ReadInConfig()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	required := map[string]string{
		"COORDINATOR_ADDR": c.CoordinatorAddr,
		"LOG_SERVICE_ADDR": c.LogServiceAddr,
		"BANK_API_ADDR":    c.BankAPIAddr,
	}
	for name, val := range required {
		if val == "" {
			return fmt.Errorf("required env var %s is not set", name)
		}
	}
	return nil
}
