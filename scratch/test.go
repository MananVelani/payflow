package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
)

func main() {
	payload := []byte(`{"amount": 15000.50, "currency": "USD", "merchant_id": "merch_123", "idempotency_key": "idemp-test-999"}`)
	resp, err := http.Post("http://localhost:8080/v1/payments", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))
}
