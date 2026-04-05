// Package main implements a canonical placeholder HTTP server for PayFlow services.
// Members 1–4 copy this into their service directories and replace with real code in Weeks 2–3.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Update this module path to match your go.mod
	paymentPb "payflow/proto/payment" 
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	port := getEnv("PORT", "8080")
	metricsPort := getEnv("METRICS_PORT", "2112")

	// Server A: main service endpoints
	mainMux := http.NewServeMux()
	mainMux.HandleFunc("/health", handleHealth)
	mainMux.HandleFunc("/metrics", handleMetricsStub)
	mainMux.HandleFunc("/v1/payments", handlePaymentStub)

	mainServer := &http.Server{
		Addr:         ":" + port,
		Handler:      mainMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	grpcClient := paymentPb.NewPaymentGatewayClient(conn)

	metricsServer := &http.Server{
		Addr:         ":" + metricsPort,
		Handler:      metricsMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start both servers concurrently
	go func() {
		log.Printf("placeholder service listening on :%s", port)
		if err := mainServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("main server error: %v", err)
		}
	}()

		// 3. Forward request to Coordinator via gRPC [cite: 59]
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := grpcClient.SubmitTask(ctx, &paymentPb.SubmitTaskRequest{
			Epoch:          1, // Dummy epoch for Week 1
			Amount:         req.Amount,
			Currency:       req.Currency,
			MerchantId:     req.MerchantID,
			IdempotencyKey: req.IdempotencyKey,
		})

		if err != nil {
			http.Error(w, fmt.Sprintf("Coordinator error: %v", err), http.StatusServiceUnavailable)
			return
		}
	}()

	// Graceful shutdown on SIGTERM/SIGINT
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	sig := <-quit
	log.Printf("received signal %s, shutting down gracefully...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mainServer.Shutdown(ctx); err != nil {
		log.Printf("main server shutdown error: %v", err)
	}
	if err := metricsServer.Shutdown(ctx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}

	log.Println("placeholder service stopped")
}

// handleHealth returns a JSON health check response.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := map[string]string{
		"status":  "ok",
		"service": "placeholder",
	}
	json.NewEncoder(w).Encode(resp)
}

// handleMetricsStub returns a minimal metrics stub for the main port.
func handleMetricsStub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "# placeholder metrics\n")
}

// handlePaymentStub returns a stub payment response.
func handlePaymentStub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	resp := map[string]string{
		"txn_id": "stub-001",
		"status": "queued",
	}
	json.NewEncoder(w).Encode(resp)
}

// handlePrometheusMetrics returns valid Prometheus text format metrics.
func handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `# HELP placeholder_up Service is up
# TYPE placeholder_up gauge
placeholder_up 1
`)
}

// getEnv reads an environment variable with a fallback default value.
func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
