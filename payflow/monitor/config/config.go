// Package config loads C5 monitor configuration from environment variables.
package config

import (
	"fmt"
	"log"
	"os"
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
}

// Load reads configuration from environment variables and returns a validated Config.
// Returns an error if any configuration value is invalid.
func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:       getEnv("HTTP_PORT", "3000"),
		PrometheusPort: getEnv("PROMETHEUS_PORT", "9091"),
		ServiceName:    getEnv("SERVICE_NAME", "monitor"),
		Version:        getEnv("VERSION", "0.1.0"),
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

	// Parse scrape targets from comma-separated list
	targetsStr := getEnv("SCRAPE_TARGETS", "")
	if targetsStr != "" {
		parts := strings.Split(targetsStr, ",")
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				cfg.ScrapeTargets = append(cfg.ScrapeTargets, trimmed)
			}
		}
	}

	// Log loaded configuration
	log.Printf("[config] HTTPPort:       %s", cfg.HTTPPort)
	log.Printf("[config] PrometheusPort: %s", cfg.PrometheusPort)
	log.Printf("[config] ScrapeInterval: %s", cfg.ScrapeInterval)
	log.Printf("[config] ScrapeTargets:  %v (%d targets)", cfg.ScrapeTargets, len(cfg.ScrapeTargets))
	log.Printf("[config] ServiceName:    %s", cfg.ServiceName)
	log.Printf("[config] Version:        %s", cfg.Version)

	return cfg, nil
}

// getEnv reads an environment variable with a fallback default value.
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
