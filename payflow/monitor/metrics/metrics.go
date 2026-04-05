// Package metrics defines Prometheus metrics exported by C5.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds all Prometheus metric instruments for the C5 monitoring service.
type Metrics struct {
	// ScrapeDuration measures the duration of each metric scrape per target.
	ScrapeDuration *prometheus.HistogramVec

	// ScrapeErrors counts the total number of scrape errors per target and reason.
	ScrapeErrors *prometheus.CounterVec

	// WebSocketConnections tracks the number of active WebSocket connections.
	WebSocketConnections prometheus.Gauge

	// ScrapeTargetsUp indicates whether each scrape target is reachable (1) or not (0).
	ScrapeTargetsUp *prometheus.GaugeVec
}

// NewRegistry creates a new non-default Prometheus registry with Go and process
// collectors registered. Using a non-default registry avoids polluting the global
// default registry with unrelated metrics.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// MustRegister creates all C5 metrics and registers them with the provided registry.
// Panics with a clear message if any metric registration fails.
func MustRegister(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		ScrapeDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "payflow",
				Subsystem: "monitor",
				Name:      "scrape_duration_seconds",
				Help:      "Duration of metric scrape per target in seconds.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"target"},
		),

		ScrapeErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "payflow",
				Subsystem: "monitor",
				Name:      "scrape_errors_total",
				Help:      "Total number of scrape errors per target.",
			},
			[]string{"target", "reason"},
		),

		WebSocketConnections: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "payflow",
				Subsystem: "monitor",
				Name:      "websocket_connections_active",
				Help:      "Number of active WebSocket connections to the dashboard.",
			},
		),

		ScrapeTargetsUp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "payflow",
				Subsystem: "monitor",
				Name:      "target_up",
				Help:      "1 if the scrape target is reachable, 0 otherwise.",
			},
			[]string{"target"},
		),
	}

	// Register all metrics — panic on failure so startup is fail-fast
	for name, collector := range map[string]prometheus.Collector{
		"scrape_duration_seconds":       m.ScrapeDuration,
		"scrape_errors_total":           m.ScrapeErrors,
		"websocket_connections_active":  m.WebSocketConnections,
		"target_up":                     m.ScrapeTargetsUp,
	} {
		if err := reg.Register(collector); err != nil {
			panic("failed to register metric " + name + ": " + err.Error())
		}
	}

	return m
}
