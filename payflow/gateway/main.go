package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	// Update this module path to match your go.mod
	paymentPb "payflow/proto/payment" 
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PaymentRequest represents the incoming JSON payload
type PaymentRequest struct {
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	MerchantID     string  `json:"merchant_id"`
	IdempotencyKey string  `json:"idempotency_key"`
}

func main() {
	// 1. Set up gRPC connection to the Coordinator (C2)
	// Using coordinator-1:50051 as defined in the Docker service map [cite: 78]
	coordinatorAddr := "localhost:50051" // Use "coordinator-1:50051" inside Docker
	
	conn, err := grpc.Dial(coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to coordinator: %v", err)
	}
	defer conn.Close()

	grpcClient := paymentPb.NewPaymentGatewayClient(conn)

	// 2. Set up the REST HTTP/2 server
	http.HandleFunc("/v1/payments", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req PaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

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

		// Return dummy txn_id to client 
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"txn_id": resp.GetTxnId(),
			"status": "QUEUED",
		})
	})

	log.Println("C1 API Gateway starting on port 8080...") // Port 8080 per service map [cite: 78]
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start gateway: %v", err)
	}
}