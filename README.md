# Payflow System Documentation

Payflow is a microservices-based payment processing system consisting of a RESTful API Gateway and several internal services communicating over gRPC.

## Architecture Overview

The system is composed of the following core components:
1. **Gateway**: Exposes an external HTTP REST API to ingest payment requests and forwards them to the coordinator.
2. **Coordinator**: Manages task distribution, handles worker nodes, and ensures high availability via leader election (Bully Algorithm).
3. **Worker**: Polls tasks from the coordinator and processes the payments.
4. **Payment Log**: A centralized log store (backed by BoltDB) for persistent state, idempotency deduplication, and 2-Phase Commit coordination.

---

## 1. External REST API (Gateway)

The **Gateway** service runs an HTTP server (default port `8080`) and exposes the following public REST endpoints.

### `POST /v1/payments`
Submit a new payment task to the system.

**Request Payload (JSON)**
```json
{
  "amount": 100.50,
  "currency": "USD",
  "merchant_id": "merch123",
  "idempotency_key": "unique-uuid-or-key"
}
```

**Response (JSON)**
Returns a transaction ID and the initial state of the payment.
```json
{
  "txn_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "QUEUED"
}
```

**Status Codes**:
- `200 OK`: Request successfully queued.
- `400 Bad Request`: Invalid JSON payload.
- `405 Method Not Allowed`: Used an HTTP method other than `POST`.
- `503 Service Unavailable`: Coordinator is unreachable or returned an error.

---

## 2. Internal gRPC APIs

The internal services communicate using gRPC. Below are the gRPC services defined across the system.

### Coordinator Services (`coordinator.proto`, `payment.proto`)

#### `payflow.payment.v1.PaymentGateway`
Used by the Gateway to submit tasks to the Coordinator.
- `rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse)`

#### `payflow.coordinator.v1.CoordinatorCluster`
Used between Coordinator nodes for leader election and cluster management.
- `rpc Election(ElectionMessage) returns (ElectionResponse)`
- `rpc AnnounceCoordinator(CoordinatorMessage) returns (AckResponse)`

### Worker Services (`worker.proto`)

#### `payflow.worker.v1.WorkerManagement`
Used by Worker nodes to communicate with the Coordinator.
- `rpc RegisterWorker(RegisterRequest) returns (RegisterResponse)`
- `rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse)`
- `rpc PollTasks(PollRequest) returns (stream TaskAssignment)`
- `rpc ReportResult(TaskResult) returns (ResultAck)`

### Payment Log Services (`log.proto`)

#### `payflow.log.v1.PaymentLogService`
Dedicated internal data store service mimicking append-only behavior.
- **Append & Retrieve**
  - `rpc AppendEntry(LogEntry) returns (AppendResponse)`
  - `rpc GetEntry(GetEntryRequest) returns (LogEntry)`
  - `rpc GetLogRange(GetRangeRequest) returns (stream LogEntry)`
  - `rpc GetAllPending(PendingRequest) returns (stream LogEntry)`

- **Idempotency Deduplication**
  - `rpc CheckIdempotency(IdempotencyRequest) returns (IdempotencyResponse)`
  - `rpc WriteResult(WriteResultRequest) returns (WriteResultAck)`

- **2-Phase Commit for High-Value Payments**
  - `rpc HandlePrepare(TxnRequest) returns (TxnResponse)`
  - `rpc HandleCommit(TxnRequest) returns (TxnResponse)`
  - `rpc HandleRollback(TxnRequest) returns (TxnResponse)`

---

## Running the System

To deploy the entire system using Docker:
```bash
docker-compose up --build
```
