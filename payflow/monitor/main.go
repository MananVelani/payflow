// Command monitor is the C5 PayFlow monitoring service.
// It scrapes Prometheus metrics from all PayFlow components (C1-C4) and serves
// a real-time WebSocket dashboard showing cluster health, scrape results, and
// system throughput metrics.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/payflow/monitor/config"
	"github.com/payflow/monitor/dashboard"
	"github.com/payflow/monitor/health"
	"github.com/payflow/monitor/metrics"
	"github.com/payflow/monitor/scraper"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("[monitor] C5 PayFlow Monitoring Service starting...")

	// 1. Load configuration from environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[monitor] configuration error: %v", err)
	}
	log.Printf("PayFlow Monitor v0.2.0 starting — scraping %d targets every %s",
		len(cfg.ScrapeTargets), cfg.ScrapeInterval)

	// 2. Initialize Prometheus metrics registry
	reg := metrics.NewRegistry()
	m := metrics.MustRegister(reg)
	log.Println("[monitor] Prometheus metrics registered")

	// 3. Create scraper
	scr := scraper.New(cfg, m)

	// 4. Create dashboard WebSocket server
	dash := dashboard.NewServer(scr, m)

	// 5. Wire scraper subscription → dashboard push
	// This must happen BEFORE scraper.Start so no snapshots are missed.
	scr.Subscribe(dash.OnSnapshot)

	// 6. Create health handler
	healthHandler := health.NewHandler(cfg.Version)

	// 7. Wire up HTTP muxes
	// Main mux: dashboard + health + WebSocket + API state (port 3000)
	mainMux := http.NewServeMux()
	mainMux.HandleFunc("/health", healthHandler)
	dash.RegisterRoutes(mainMux)

	// Prometheus mux: /metrics endpoint (port 9091)
	promMux := http.NewServeMux()
	promMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	// 8. Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// 9. Start goroutines
	go scr.Start(ctx)
	go dash.Start(ctx)

	go func() {
		mainAddr := ":" + cfg.HTTPPort
		log.Printf("[monitor] main HTTP server listening on %s", mainAddr)
		if err := http.ListenAndServe(mainAddr, mainMux); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[monitor] main HTTP server error: %v", err)
		}
	}()

	go func() {
		promAddr := ":" + cfg.PrometheusPort
		log.Printf("[monitor] Prometheus metrics server listening on %s", promAddr)
		if err := http.ListenAndServe(promAddr, promMux); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[monitor] Prometheus server error: %v", err)
		}
	}()

	// 10. Block on shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	log.Printf("[monitor] received signal %s, shutting down...", sig)

	// 11. Cancel context and allow goroutines to drain
	cancel()
	time.Sleep(2 * time.Second)
	log.Println("[monitor] C5 Monitoring Service stopped")
	os.Exit(0)
}
