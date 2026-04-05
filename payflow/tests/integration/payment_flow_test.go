// Package integration provides end-to-end tests for PayFlow.
// These tests verify that the full 5-service payment pipeline is end-to-end
// live — the Wednesday integration checkpoint formalized as repeatable
// automated tests.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/payflow/tests/integration/helpers"
	"github.com/stretchr/testify/assert"
)

const (
	paymentEndpoint = GatewayURL + "/v1/payments"
)

// PaymentRequest is the JSON payload for submitting a payment.
type PaymentRequest struct {
	Amount         int    `json:"amount"`
	Currency       string `json:"currency"`
	MerchantID     string `json:"merchant_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

// PaymentResponse is the JSON payload returned from a payment submission.
type PaymentResponse struct {
	TxnID  string `json:"txn_id"`
	Status string `json:"status"`
}

// submitPayment sends a payment request to the API gateway and returns the response.
func submitPayment(t *testing.T, req PaymentRequest) PaymentResponse {
	t.Helper()

	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal payment request: %v", err)
	}

	resp, err := http.Post(paymentEndpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("failed to POST payment: %v", err)
	}
	defer resp.Body.Close()

	// Accept 200 OK or 202 Accepted
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("payment submission returned HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var pr PaymentResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		t.Fatalf("failed to decode payment response: %v", err)
	}

	if pr.TxnID == "" {
		t.Fatalf("payment response has empty txn_id")
	}

	return pr
}

// waitForStatus polls the payment status endpoint until a terminal state is
// reached or the timeout expires. Calls t.Fatalf on timeout.
func waitForStatus(t *testing.T, txnID string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 5 * time.Second}

	for time.Now().Before(deadline) {
		resp, err := client.Get(GatewayURL + "/v1/payments/" + txnID)
		if err != nil {
			t.Logf("status poll failed for %s: %v", txnID, err)
			time.Sleep(2 * time.Second)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Logf("status poll for %s returned HTTP %d", txnID, resp.StatusCode)
			time.Sleep(2 * time.Second)
			continue
		}

		var status struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(body, &status); err != nil {
			t.Logf("failed to parse status response: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if status.Status == "SUCCESS" || status.Status == "FAILED" {
			return status.Status
		}

		t.Logf("txn %s status=%s, waiting...", txnID, status.Status)
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("txn %s did not reach terminal status within %s", txnID, timeout)
	return "" // unreachable
}

// TestSinglePaymentEndToEnd submits one payment and verifies it reaches a
// terminal state (SUCCESS or FAILED).
func TestSinglePaymentEndToEnd(t *testing.T) {
	idemKey := fmt.Sprintf("test-e2e-%d", time.Now().UnixNano())

	resp := submitPayment(t, PaymentRequest{
		Amount:         500,
		Currency:       "INR",
		MerchantID:     "test-merchant-01",
		IdempotencyKey: idemKey,
	})

	assert.NotEmpty(t, resp.TxnID, "txn_id should not be empty")
	t.Logf("Submitted payment, txn_id: %s", resp.TxnID)

	finalStatus := waitForStatus(t, resp.TxnID, 30*time.Second)
	assert.Contains(t, []string{"SUCCESS", "FAILED"}, finalStatus,
		"payment must reach terminal state, got: "+finalStatus)

	t.Logf("✅ Payment %s reached status: %s", resp.TxnID, finalStatus)
}

// TestIdempotencyKeyDeduplication verifies that submitting the same payment
// twice with the same idempotency key returns the same txn_id.
func TestIdempotencyKeyDeduplication(t *testing.T) {
	idemKey := fmt.Sprintf("test-dedup-%d", time.Now().UnixNano())
	req := PaymentRequest{
		Amount:         1000,
		Currency:       "INR",
		MerchantID:     "test-merchant-dedup",
		IdempotencyKey: idemKey,
	}

	resp1 := submitPayment(t, req)
	resp2 := submitPayment(t, req)

	assert.Equal(t, resp1.TxnID, resp2.TxnID,
		"both submissions with same idempotency key should return the same txn_id")

	t.Logf("✅ Dedup confirmed: both submissions returned txn_id: %s", resp1.TxnID)
}

// TestMonitorReflectsPaymentActivity verifies that the monitor's /api/state
// endpoint reflects payment processing activity (tasks_processed increases).
func TestMonitorReflectsPaymentActivity(t *testing.T) {
	// Get baseline
	beforeResp := helpers.WaitForHTTP(t, MonitorURL+"/api/state", 30*time.Second)
	beforeBody, err := io.ReadAll(beforeResp.Body)
	beforeResp.Body.Close()
	assert.NoError(t, err)

	var beforeSnap struct {
		Workers []struct {
			TasksProcessed int64 `json:"tasks_processed"`
		} `json:"workers"`
	}
	json.Unmarshal(beforeBody, &beforeSnap)

	var beforeTotal int64
	for _, w := range beforeSnap.Workers {
		beforeTotal += w.TasksProcessed
	}
	t.Logf("Baseline tasks_processed: %d", beforeTotal)

	// Submit 3 payments
	for i := 0; i < 3; i++ {
		idemKey := fmt.Sprintf("test-monitor-%d-%d", time.Now().UnixNano(), i)
		submitPayment(t, PaymentRequest{
			Amount:         200 + i*100,
			Currency:       "INR",
			MerchantID:     "test-monitor-merchant",
			IdempotencyKey: idemKey,
		})
	}

	// Wait for processing
	time.Sleep(20 * time.Second)

	// Get updated state
	afterResp := helpers.WaitForHTTP(t, MonitorURL+"/api/state", 30*time.Second)
	afterBody, err := io.ReadAll(afterResp.Body)
	afterResp.Body.Close()
	assert.NoError(t, err)

	var afterSnap struct {
		Workers []struct {
			TasksProcessed int64 `json:"tasks_processed"`
		} `json:"workers"`
	}
	json.Unmarshal(afterBody, &afterSnap)

	var afterTotal int64
	for _, w := range afterSnap.Workers {
		afterTotal += w.TasksProcessed
	}

	delta := afterTotal - beforeTotal
	t.Logf("Tasks processed delta: %d (before=%d, after=%d)", delta, beforeTotal, afterTotal)

	assert.GreaterOrEqual(t, delta, int64(1),
		"total tasks_processed should increase by at least 1 after submitting payments")
}
