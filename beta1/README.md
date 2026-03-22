# `worker-service` — C3: Payment Execution Engine

> **Owner:** Member 2 · **Language:** Go 1.22 · **Protocol:** gRPC · **Project:** PayFlow

---

## Quick Reference: Who Needs What

| If you are… | Jump to |
|---|---|
| **M1 (C2 Coordinator)** | §3 — gRPC contract, heartbeat fields, REVOKE RPC |
| **M3 (Proto / API Gateway)** | §4 — required proto fields, `worker.proto` spec |
| **M4 (C4 Payment Log)** | §5 — the two C4 RPCs C3 calls, call semantics |
| **M5 (Infra / Monitoring)** | §6 — Prometheus port, Docker image spec, health check |

---

## 1. Service Overview

`worker-service` (C3) is a **stateless, horizontally scalable** payment execution engine.

**What C3 does:**
- Registers with C2 (Coordinator) on startup, enters the live worker pool
- Receives payment tasks from C2 over a persistent gRPC stream
- Checks idempotency with C4 **before** calling the bank — always, no exceptions
- Calls Mock Bank API with retry + circuit breaker
- Reports final result to C2 (C2 then writes state to C4)

**What C3 does NOT do:**
- Write to any database — C4 owns all state
- Accept requests from external clients — C2 is the only task source
- Make scheduling decisions — C2 owns scheduling

---

## 2. Exactly-Once Execution Pipeline

**Skipping Step 2 causes double charges.** This sequence is mandatory and ordered.

```
STEP 1  Receive Task from C2 (AssignTask gRPC stream)
         ↓
STEP 2  Call C4.CheckIdempotency(idempotency_key)
         → exists=true:  return cached result to C2. STOP. Do not call bank.
         → exists=false: proceed to step 3.
         ↓
STEP 3  Call Mock Bank API (SAME idempotency_key on every retry)
         → Retry 3× with 1s / 2s / 4s exponential backoff
         → After 3 failures: report FAILURE to C2
         ↓
STEP 4  ReportResult(task_id, status, bank_txn_ref) to C2
         C2 then calls C4.WriteResult on our behalf
```

---

## 3. Integration: M1 (C2 Coordinator)

### Registration — called on startup AND on every coordinator reconnect
```
RegisterWorker(WorkerInfo) → WorkerAck
```

| Field | Type | Value |
|---|---|---|
| `worker_id` | string | Stable UUID — set via `WORKER_ID` env var |
| `grpc_addr` | string | `"worker-N:PORT"` — C2 uses this to send REVOKE |
| `max_capacity` | int32 | Value of `MAX_CONCURRENT_TASKS` env var |

> C3 re-registers on every coordinator reconnect — never assumes C2 remembers it across a leader election.

### Heartbeat — every 2 seconds on the `WorkerHeartbeat` bidirectional stream

| Field | Source |
|---|---|
| `worker_id` | config |
| `load` | `active_tasks / max_capacity` (0.0–1.0) |
| `tasks_processed_count` | cumulative counter since startup |
| `avg_task_duration_ms` | rolling average — used by C2 weighted scheduler |
| `epoch` | last epoch received from C2 |

> **If C2 misses 3 heartbeats (6s), it marks this worker DEAD.** Heartbeat interval must stay below 6s.

### REVOKE Handler — C3 exposes `RevokeTask(RevokeRequest) → Ack`

When C2 reassigns a task:
1. C2 calls `RevokeTask(task_id)` on C3
2. C3 sets `revokedTasks[task_id] = true`
3. If the bank call finishes after revoke: C3 **silently discards the result** — does NOT call ReportResult

```go
// Pseudocode
if revokedTasks.Contains(task_id) {
    log.Warn("task revoked mid-execution, discarding result", task_id)
    return // do NOT call ReportResult
}
```

---

## 4. Integration: M3 (Proto / API Gateway)

C3 depends on `proto/worker.proto`. **Flag any schema change in `#proto-changes` before committing.**

### gRPC Methods C3 Exposes (C2 calls these)

| Method | Direction | Description |
|---|---|---|
| `RegisterWorker(WorkerInfo) → WorkerAck` | C3 serves | On startup |
| `RevokeTask(RevokeRequest) → Ack` | C3 serves | Abandon in-flight task |
| `WorkerHeartbeat(stream HeartbeatPing) → stream HeartbeatAck` | C3 initiates | 2s ping |
| `ReportResult(PaymentResult) → Ack` | C3 calls C2 | Final execution outcome |

### Required Fields on `Task` Message

```protobuf
message Task {
  string task_id         = 1;  // REQUIRED — unique per task
  string idempotency_key = 2;  // REQUIRED — passed to bank on every retry
  double amount          = 3;
  string currency        = 4;
  string merchant_id     = 5;
  int64  epoch           = 6;  // REQUIRED — C3 rejects if stale
}
```

> **Breaking change rule:** Adding optional fields is safe. Renaming or removing existing fields blocks M2. Use `reserved` for removed field numbers.

---

## 5. Integration: M4 (C4 Payment Log Service)

C3 makes exactly **two calls** to C4 per task:

| C4 RPC | When Called | C3 Action |
|---|---|---|
| `CheckIdempotency(idempotency_key)` | **Before** bank call | If `exists==true`, return cached result. Skip bank. |
| *(C2 calls WriteResult on C3's behalf)* | After C3 reports to C2 | C3 does NOT call WriteResult directly |

**C4 address:** `LOG_SERVICE_ADDR` env var (e.g. `payment-log:50054`)

**C3 handles C4 unavailability gracefully:**
- Buffers task result in memory during C4 downtime
- Retries `CheckIdempotency` with backoff before calling bank
- Does not fail payments immediately on C4 downtime

---

## 6. Integration: M5 (Infra & Monitoring)

### Prometheus Metrics — port `:9092/metrics`

Add to your `prometheus.yml`:
```yaml
- job_name: 'worker'
  static_configs:
    - targets: ['worker-1:9092', 'worker-2:9093']
```

| Metric | Type | Labels |
|---|---|---|
| `worker_tasks_total` | Counter | `status=success\|failure\|revoked` |
| `worker_active_tasks` | Gauge | — |
| `worker_bank_request_duration_ms` | Histogram | buckets: 50/100/250/500/1000/2000ms |
| `worker_bank_retries_total` | Counter | — |
| `worker_heartbeat_sent_total` | Counter | — |
| `worker_revoked_tasks_total` | Counter | — |

### Docker Image Spec

| Property | Value |
|---|---|
| Image name | `payflow/worker:latest` |
| Base image | `gcr.io/distroless/static:nonroot` |
| Metrics port | `9092` |
| gRPC port | OS-assigned, reported to C2 at `RegisterWorker` |
| Restart policy | `on-failure` (NOT `always`) |
| Health check | `wget -qO- http://localhost:9092/metrics` |
| Run as | `nonroot:nonroot` (UID 65532) |

### Docker Compose Block (add to canonical file)
```yaml
worker-1:
  build: { context: ./worker, dockerfile: Dockerfile }
  image: payflow/worker:latest
  restart: on-failure
  environment:
    COORDINATOR_ADDR: "coordinator-1:50051"
    LOG_SERVICE_ADDR: "payment-log:50054"
    BANK_API_ADDR: "http://mock-bank:8090"
    WORKER_ID: "worker-1"
    METRICS_PORT: "9092"
  ports: ["9092:9092"]
  networks: [payflow-net]
  depends_on: [coordinator-1, payment-log]
  healthcheck:
    test: ["CMD-SHELL", "wget -qO- http://localhost:9092/metrics > /dev/null 2>&1 || exit 1"]
    interval: 10s
    timeout: 5s
    retries: 3
```

---

## 7. Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `COORDINATOR_ADDR` | ✅ | — | C2 gRPC address (e.g. `coordinator-1:50051`) |
| `LOG_SERVICE_ADDR` | ✅ | — | C4 gRPC address (e.g. `payment-log:50054`) |
| `BANK_API_ADDR` | ✅ | — | Mock Bank HTTP (e.g. `http://mock-bank:8090`) |
| `WORKER_ID` | ❌ | auto-UUID | Stable instance identifier |
| `GRPC_PORT` | ❌ | 0 (OS-assigned) | Port for incoming gRPC |
| `METRICS_PORT` | ❌ | 9092 | Prometheus port |
| `MAX_CONCURRENT_TASKS` | ❌ | 5 | Max parallel tasks; reported as capacity to C2 |
| `HEARTBEAT_INTERVAL` | ❌ | 2s | Must be < 6s (C2 dead timeout) |
| `BANK_FAIL_RATE` | ❌ | 0.10 | Simulated failure rate (0.0–1.0) |
| `BANK_LATENCY_MIN_MS` | ❌ | 50 | Min simulated latency |
| `BANK_LATENCY_MAX_MS` | ❌ | 500 | Max simulated latency |
| `RETRY_MAX_ATTEMPTS` | ❌ | 3 | Max bank retries before FAILURE |
| `RETRY_BASE_DELAY_MS` | ❌ | 1000 | Backoff base: 1s → 2s → 4s |
| `LOG_LEVEL` | ❌ | info | `debug\|info\|warn\|error` |

---

## 8. Running Locally

```bash
cd worker
go mod download

# Run (heartbeat loop retries if C2/C4 unreachable)
export COORDINATOR_ADDR=localhost:50051
export LOG_SERVICE_ADDR=localhost:50054
export BANK_API_ADDR=http://localhost:8090
go run ./cmd/worker

# Full stack via Docker Compose
docker compose up coordinator-1 payment-log worker-1
docker compose logs -f worker-1

# Simulate worker death — triggers C2 reassignment
docker kill payflow_worker_1
```

---

## 9. Week 1 Status

| Feature | Status | Available In |
|---|---|---|
| Go module + Clean Architecture | ✅ Live | Now |
| Config + structured JSON logging | ✅ Live | Now |
| gRPC server + interceptors | ✅ Live | Now |
| Heartbeat loop (stub log) | ✅ Live | Now |
| Prometheus metrics endpoint | ✅ Live | Now |
| gRPC health protocol | ✅ Live | Now |
| Distroless Docker image | ✅ Live | Now |
| Mock Bank circuit breaker | ✅ Live | Now |
| Real gRPC calls to C2 | 🔄 Stub | Week 2 (needs M3 proto stubs) |
| Real C4 idempotency check | 🔄 Stub | Week 2 (needs M4 service) |
| Full task execution pipeline | 📅 Planned | Week 2 |
| Retry + reassignment handling | 📅 Planned | Week 2–3 |

---

*Part of the PayFlow distributed payment system. Full architecture in root `README.md`.*
