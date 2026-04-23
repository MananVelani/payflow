package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

func post(amount float64, key string) string {
	payload := []byte(fmt.Sprintf(`{"amount": %f, "currency": "USD", "merchant_id": "merch_123", "idempotency_key": "%s"}`, amount, key))
	resp, err := http.Post("http://localhost:8080/v1/payments", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func main() {
	// Submit 3 regular payments (< $10k)
	fmt.Println("=== Submitting 3 small payments ===")
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("idemp-small-%d", time.Now().UnixNano())
		fmt.Printf("  Response: %s\n", post(100.50, key))
		time.Sleep(500 * time.Millisecond)
	}

	// Submit 3 high-value payments (> $10k)
	fmt.Println("\n=== Submitting 3 high-value payments (triggers 2PC) ===")
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("idemp-large-%d", time.Now().UnixNano())
		fmt.Printf("  Response: %s\n", post(15000.00, key))
		time.Sleep(500 * time.Millisecond)
	}

	// Submit same key twice to test idempotency hit
	fmt.Println("\n=== Submitting duplicate key (should produce 1 Idemp. Hit) ===")
	key := "idemp-duplicate-key-xyz789"
	fmt.Printf("  First:  %s\n", post(500.00, key))
	fmt.Println("  Waiting 3s for first payment to be processed and written to log...")
	time.Sleep(3 * time.Second)
	fmt.Printf("  Second: %s\n", post(500.00, key))

	fmt.Println("\n=== Waiting 5s for workers to process ===")
	time.Sleep(5 * time.Second)
}
