package domain

import "time"

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "PENDING"
	TaskStatusInProgress TaskStatus = "IN_PROGRESS"
	TaskStatusSuccess    TaskStatus = "SUCCESS"
	TaskStatusFailure    TaskStatus = "FAILURE"
	TaskStatusRevoked    TaskStatus = "REVOKED"
)

// Task mirrors proto/worker.proto Task message exactly.
type Task struct {
	TaskID         string     `json:"task_id"`
	IdempotencyKey string     `json:"idempotency_key"`
	Amount         float64    `json:"amount"`
	Currency       string     `json:"currency"`
	MerchantID     string     `json:"merchant_id"`
	Epoch          int64      `json:"epoch"`
	ReceivedAt     time.Time  `json:"received_at"`
	DeadlineUnixMs int64      `json:"deadline_unix_ms"` // Unix milliseconds; 0 means no deadline
}

// PaymentResult mirrors proto/worker.proto PaymentResult message exactly.
type PaymentResult struct {
	TaskID         string     `json:"task_id"`
	WorkerID       string     `json:"worker_id"`
	Status         TaskStatus `json:"status"`
	BankTxnRef     string     `json:"bank_txn_ref"`
	IdempotencyKey string     `json:"idempotency_key"`
	Epoch          int64      `json:"epoch"`
	CompletedAt    time.Time  `json:"completed_at"`
}

// WorkerStats is reported to C2 on every heartbeat ping.
type WorkerStats struct {
	WorkerID            string
	Load                float32
	TasksProcessedCount int64
	AvgTaskDurationMS   int64
	Epoch               int64
}
