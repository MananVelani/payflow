package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// PaymentRequest payload
type PaymentRequest struct {
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	MerchantID     string  `json:"merchant_id"`
	IdempotencyKey string  `json:"idempotency_key"`
}

// PaymentResponse payload for a single submission
type PaymentResponse struct {
	TxnID   string `json:"txn_id"`
	Status  string `json:"status"`
	TraceID string `json:"trace_id"`
}

// StatusResponse payload for get status check
type StatusResponse struct {
	TxnID   string `json:"txn_id"`
	Status  string `json:"status"`
	TraceID string `json:"trace_id"`
}

// BatchResponse payload for multiple submissions
type BatchResponse struct {
	TxnID   string `json:"txn_id"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

type BatchSubmissionResponse struct {
	BatchResults []BatchResponse `json:"batch_results"`
	TraceID      string          `json:"trace_id"`
}

type Client struct {
	baseURL    string
	httpClient *http.Client
	maxRetries int
}

// NewClient initializes a new SDK Client
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		maxRetries: 3, // default retries
	}
}

// SetMaxRetries configure internal retry counter
func (c *Client) SetMaxRetries(retries int) {
	c.maxRetries = retries
}

func (c *Client) doWithRetry(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
			time.Sleep(backoff)
		}

		resp, err = c.httpClient.Do(req)
		if err != nil {
			continue
		}

		if resp.StatusCode >= 500 {
			resp.Body.Close()
			err = fmt.Errorf("server error: %d", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("request failed after %d retries: %v", c.maxRetries, err)
}

func (c *Client) injectTracing(ctx context.Context, req *http.Request) {
	if ctx != nil {
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	}
}

// SubmitPayment sends a single payment to Gateway
func (c *Client) SubmitPayment(ctx context.Context, reqData PaymentRequest) (*PaymentResponse, error) {
	url := fmt.Sprintf("%s/v1/payments", c.baseURL)
	jsonData, _ := json.Marshal(reqData)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	
	// A05 trace context injection mapping
	c.injectTracing(ctx, req)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s", body)
	}

	var result PaymentResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// GetStatus checks the status of an existing txn
func (c *Client) GetStatus(ctx context.Context, txnID string) (*StatusResponse, error) {
	url := fmt.Sprintf("%s/v1/payments/%s", c.baseURL, txnID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	c.injectTracing(ctx, req)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s", body)
	}

	var result StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// SubmitBatch sends an array of payments at once
func (c *Client) SubmitBatch(ctx context.Context, reqs []PaymentRequest) (*BatchSubmissionResponse, error) {
	url := fmt.Sprintf("%s/v1/batch", c.baseURL)
	jsonData, _ := json.Marshal(reqs)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	c.injectTracing(ctx, req)

	resp, err := c.doWithRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error: %s", body)
	}

	var result BatchSubmissionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}
