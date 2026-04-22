// Package config loads C5 monitor configuration from environment variables.
package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration values for the C5 monitoring service.
// All values are loaded from environment variables with sensible defaults.
type Config struct {
	// HTTPPort is the port for the main HTTP server (dashboard + health).
	HTTPPort string

	// PrometheusPort is the port for the Prometheus metrics endpoint.
	PrometheusPort string

	// ScrapeInterval is the duration between metric scrape cycles.
	ScrapeInterval time.Duration

	// ScrapeTargets is the list of Prometheus /metrics endpoint URLs to scrape.
	ScrapeTargets []string

	// ServiceName identifies this service in logs and metrics.
	ServiceName string

	// Version is the semantic version of this service build.
	Version string

	// ScalingEnabled toggles dynamic worker autoscaling.
	ScalingEnabled bool

	// ScaleQueueThreshold triggers scale-up when queue depth exceeds this value.
	ScaleQueueThreshold int64

	// ScaleMaxWorkers caps total running workers including base workers.
	ScaleMaxWorkers int

	// ScaleCooldown is the minimum time between consecutive scale-up events.
	ScaleCooldown time.Duration

	// ScaleTemplateWorker is the base worker container name used for cloning.
	ScaleTemplateWorker string
}

// Load reads configuration from environment variables and returns a validated Config.
// Returns an error if any configuration value is invalid.
func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:            getEnv("HTTP_PORT", "3000"),
		PrometheusPort:      getEnv("PROMETHEUS_PORT", "9091"),
		ServiceName:         getEnv("SERVICE_NAME", "monitor"),
		Version:             getEnv("VERSION", "0.2.0"),
		ScaleTemplateWorker: getEnv("SCALING_TEMPLATE_WORKER", "worker-1"),
	}

	// Parse scrape interval
	intervalStr := getEnv("SCRAPE_INTERVAL", "15s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("invalid SCRAPE_INTERVAL %q: %w", intervalStr, err)
	}
	if interval < 1*time.Second {
		return nil, fmt.Errorf("SCRAPE_INTERVAL must be >= 1s, got %s (protection against hammering targets)", interval)
	}
	cfg.ScrapeInterval = interval

	scalingEnabled, err := parseBoolEnv("SCALING_ENABLED", true)
	if err != nil {
		return nil, err
	}
	cfg.ScalingEnabled = scalingEnabled

	queueThreshold, err := parseInt64Env("SCALING_QUEUE_THRESHOLD", 50)
	if err != nil {
		return nil, err
	}
	if queueThreshold < 1 {
		return nil, fmt.Errorf("SCALING_QUEUE_THRESHOLD must be >= 1, got %d", queueThreshold)
	}
	cfg.ScaleQueueThreshold = queueThreshold

	maxWorkers, err := parseIntEnv("SCALING_MAX_WORKERS", 10)
	if err != nil {
		return nil, err
	}
	if maxWorkers < 1 {
		return nil, fmt.Errorf("SCALING_MAX_WORKERS must be >= 1, got %d", maxWorkers)
	}
	cfg.ScaleMaxWorkers = maxWorkers

	cooldown, err := time.ParseDuration(getEnv("SCALING_COOLDOWN", "30s"))
	if err != nil {
		return nil, fmt.Errorf("invalid SCALING_COOLDOWN: %w", err)
	}
	if cooldown < 1*time.Second {
		return nil, fmt.Errorf("SCALING_COOLDOWN must be >= 1s, got %s", cooldown)
	}
	cfg.ScaleCooldown = cooldown

	// Parse scrape targets from comma-separated list
	targetsStr := getEnv("SCRAPE_TARGETS", "")
	if targetsStr != "" {
		parts := strings.Split(targetsStr, ",")
		cfg.ScrapeTargets = make([]string, 0, len(parts))
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				cfg.ScrapeTargets = append(cfg.ScrapeTargets, trimmed)
			}
		}
	}

	// Log loaded configuration in compact format
	log.Printf("[config] %s v%s — HTTP:%s, Prom:%s, interval:%s, targets:%d, scaling:%t(threshold=%d,max=%d,cooldown=%s)",
		cfg.ServiceName,
		cfg.Version,
		cfg.HTTPPort,
		cfg.PrometheusPort,
		cfg.ScrapeInterval,
		len(cfg.ScrapeTargets),
		cfg.ScalingEnabled,
		cfg.ScaleQueueThreshold,
		cfg.ScaleMaxWorkers,
		cfg.ScaleCooldown,
	)

	return cfg, nil
}

// getEnv reads an environment variable with a fallback default value.
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func parseBoolEnv(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(getEnv(key, strconv.FormatBool(fallback)))
	val, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	return val, nil
}

func parseIntEnv(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(getEnv(key, strconv.Itoa(fallback)))
	val, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	return val, nil
}

func parseInt64Env(key string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(getEnv(key, strconv.FormatInt(fallback, 10)))
	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	return val, nil
}
