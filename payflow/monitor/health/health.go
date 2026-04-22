// Package health provides the /health HTTP endpoint for C5.
package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// HealthResponse represents the JSON structure returned by the /health endpoint.
type HealthResponse struct {
	Status    string `json:"status"`
	Component string `json:"component"`
	Version   string `json:"version"`
	Uptime    string `json:"uptime"`
}

// NewHandler creates an http.HandlerFunc that returns health status information.
// The uptime field is computed from the moment this handler is created.
func NewHandler(version string) http.HandlerFunc {
	startTime := time.Now()

	return func(w http.ResponseWriter, r *http.Request) {
		uptime := time.Since(startTime)
		hours := int(uptime.Hours())
		minutes := int(uptime.Minutes()) % 60
		seconds := int(uptime.Seconds()) % 60

		resp := HealthResponse{
			Status:    "ok",
			Component: "monitor",
			Version:   version,
			Uptime:    fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, fmt.Sprintf("encoding health response: %v", err), http.StatusInternalServerError)
			return
		}
	}
}
