# PayFlow C3 Worker Service — Operational Runbook

> **Audience:** On-call engineers, platform team, new contributors.
> This document covers local development, production deployment, and failure remediation.

---

## Table of Contents

1. [Local Development](#local-development)
2. [Deployment Checklist](#deployment-checklist)
3. [Failure Playbook](#failure-playbook)

---

## Local Development

### Prerequisites

| Tool | Minimum Version | Install |
|------|-----------------|---------| 
| Go | **1.22+** | https://go.dev/dl |
| Docker Desktop | 4.x | https://www.docker.com/products/docker-desktop |
| Redis (optional) | 7.x | Only required for distributed reservation (`RESERVATION_REDIS_URL`). Without it C3 falls back to a local in-memory store — safe for single-replica local dev. |
| grpcurl | any | Bundled in repo root as `grpcurl.exe` |

### Quick Start

There is no Makefile yet — use the commands below directly.

```bash
# 1. Copy the example environment file
cp .env.example .env        # then edit .env with your coordinator address

# 2. Run all tests
go test ./...

# 3. Run a single service locally (connects to whatever is in .env)
go run ./cmd/worker

# 4. Run with Docker Compose (requires a running coordinator + log-service network)
docker compose up --build
```

#### What `docker compose up` does

| Step | Detail |
|------|--------|
| Builds the distroless image | Uses the two-stage `Dockerfile` |
| Injects env vars | Loaded from the `environment:` block in `docker-compose.yml` |
| Exposes gRPC port | `50053` (or whatever `GRPC_PORT` is set to) |
| Exposes metrics | `9092` → `http://localhost:9092/metrics` |
| Self-healthcheck | Docker polls `http://localhost:9092/metrics` every 30 s |

> **Note:** The compose file does **not** start a coordinator, log-service, or mock bank. You must connect those separately or point `COORDINATOR_ADDR` / `LOG_SERVICE_ADDR` / `BANK_API_ADDR` at running instances.

---

### Environment Variable Reference

All variables are read at startup by `internal/config/config.go`. A missing **REQUIRED** variable causes the service to exit immediately with a descriptive error.

#### Service Identity & Connectivity

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `COORDINATOR_ADDR` | **REQUIRED** | — | gRPC address of the C2 Coordinator (e.g. `coordinator:50051`) |
| `LOG_SERVICE_ADDR` | **REQUIRED** | — | gRPC address of the C4 Payment Log Service (e.g. `payment-log:50054`) |
| `BANK_API_ADDR` | **REQUIRED** | — | Base URL of the Mock Bank API (e.g. `http://mock-bank:8090`) |
| `WORKER_ID` | optional | `""` | Stable identity string used in heartbeats and logs. Should be unique per pod. |
| `WORKER_TOKEN` | optional | `""` | Auth token attached as `x-worker-id` gRPC metadata on outbound calls. |

#### Ports & Observability

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GRPC_PORT` | optional | `0` (OS-assigned) | Port the C3 gRPC server listens on. Set to a fixed port in production. |
| `METRICS_PORT` | optional | `9092` | Port for Prometheus `/metrics` scrape endpoint. |
| `HEALTH_PORT` | optional | `8090` | Port for `/healthz` and `/readyz` HTTP endpoints. |
| `LOG_LEVEL` | optional | `info` | Logging verbosity: `debug` \| `info` \| `warn` \| `error` |

#### Concurrency & Timing

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MAX_CONCURRENT_TASKS` | optional | `10` | Maximum number of tasks executing concurrently (semaphore size). |
| `MAX_TASK_DURATION` | optional | `60s` | Absolute per-task deadline; also the outbox stale-drop threshold. |
| `SHUTDOWN_TIMEOUT` | optional | `10s` | Grace period for in-flight tasks before forced shutdown. |
| `HEARTBEAT_INTERVAL` | optional | `2s` | How frequently C3 sends heartbeat pings to C2. |

#### Bank Simulation

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BANK_FAIL_RATE` | optional | `0.10` | Fraction of bank calls that should artificially fail (0.0–1.0). |
| `BANK_LATENCY_MIN_MS` | optional | `50` | Minimum simulated bank latency in milliseconds. |
| `BANK_LATENCY_MAX_MS` | optional | `500` | Maximum simulated bank latency in milliseconds. |

#### Resilience

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RETRY_MAX_ATTEMPTS` | optional | `5` | Maximum bank retry attempts (per task). |
| `RETRY_BASE_DELAY` | optional | `100ms` | Base delay for exponential-with-full-jitter backoff (e.g. `100ms`, `1s`). |
| `RETRY_MAX_DELAY` | optional | `30s` | Cap on backoff delay. |
| `CB_MAX_REQUESTS` | optional | `5` | Max requests allowed in the circuit breaker half-open state. |
| `CB_FAILURE_THRESHOLD` | optional | `0.5` | Failure ratio that trips the circuit breaker open. |
| `CB_TIMEOUT` | optional | `30s` | How long the circuit breaker stays open before moving to half-open. |

#### Keepalive & Stream

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KEEPALIVE_TIME` | optional | `10s` | Time between TCP keepalive probes on the C2 connection. |
| `KEEPALIVE_TIMEOUT` | optional | `5s` | Timeout waiting for keepalive ACK before killing the connection. |
| `CONNECT_RETRY_DELAY` | optional | `2s` | Delay between C2 reconnection attempts. |

#### Outbox & Reservation

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OUTBOX_FLUSH_INTERVAL` | optional | `5s` | How often the outbox relay goroutine flushes pending results to C2. |
| `OUTBOX_MAX_SIZE` | optional | `1000` | Maximum number of pending outbox entries (informational). |
| `OUTBOX_DB_PATH` | optional | `/var/data/c3/outbox` | Filesystem path for BadgerDB persistent outbox. If empty, falls back to a non-durable in-memory store — **do not use empty in production**. |
| `RESERVATION_REDIS_URL` | optional | `""` | Redis URL for distributed idempotency reservation (e.g. `redis://redis:6379/0`). If empty, falls back to pod-local in-memory store — **not safe for multi-replica deployments**. |

---

## Deployment Checklist

### Required Environment Variables (Production)

The following must be set or the pod will exit with a fatal error at startup:

- [ ] `COORDINATOR_ADDR`
- [ ] `LOG_SERVICE_ADDR`
- [ ] `BANK_API_ADDR`

The following are technically optional but **must** be set for a correct production deployment:

- [ ] `WORKER_ID` — unique per pod (use the pod name via Kubernetes downward API)
- [ ] `GRPC_PORT` — fixed port (e.g. `50053`) so service meshes can address the pod
- [ ] `OUTBOX_DB_PATH` — mount a PVC here; empty means non-durable in-memory fallback
- [ ] `RESERVATION_REDIS_URL` — required for multi-replica exactly-once guarantees

---

### Kubernetes Probe Configuration

C3 exposes two HTTP endpoints on `HEALTH_PORT` (default `8090`):

| Endpoint | Meaning |
|----------|---------|
| `GET /healthz` | Liveness: the process is not deadlocked |
| `GET /readyz` | Readiness: C2 connection is up and gRPC server is serving |

```yaml
# Kubernetes Deployment snippet
livenessProbe:
  httpGet:
    path: /healthz
    port: 8090
  initialDelaySeconds: 10
  periodSeconds: 15
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /readyz
    port: 8090
  initialDelaySeconds: 5
  periodSeconds: 10
  failureThreshold: 3
```

---

### Prometheus Scrape Configuration

C3 exposes Prometheus metrics at `http://<pod>:<METRICS_PORT>/metrics` (default port `9092`).

```yaml
# prometheus.yml scrape_configs
scrape_configs:
  - job_name: payflow-c3-worker
    scrape_interval: 15s
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names: [payflow]
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: c3-worker
      - source_labels: [__meta_kubernetes_pod_ip]
        replacement: "${1}:9092"
        target_label: __address__
```

#### Key Metrics to Alert On

| Metric | Type | Alert Condition |
|--------|------|----------------|
| `worker_tasks_total{status="error"}` | Counter | Rate > 5/min sustained |
| `worker_task_deadline_exceeded_total` | Counter | Any non-zero rate |
| `worker_bank_retries_total` | Counter | Rate > 20/min |
| `worker_revoked_tasks_total` | Counter | Rate > 0/min |
| `worker_saturation` | Gauge | > 0.9 for > 5 min |
| `worker_orphaned_lease_count` | Gauge | > 0 after startup |
| `grpc_server_handled_total{grpc_code!="OK"}` | Counter | Any non-OK codes |

---

## Failure Playbook

### 🔴 "Circuit Breaker Open" — bank calls returning immediately

**Symptom:** Logs show `resilience: circuit breaker open`; `worker_tasks_total{status="error",bank_result="circuit_open"}` counter climbing; tasks failing without hitting the bank.

**Causes:**
- Bank API is down or has high error rate (> `CB_FAILURE_THRESHOLD`)
- Bank API is slow and requests are timing out
- Misconfigured `BANK_API_ADDR`

**Remediation:**

1. **Check bank latency dashboard.** Confirm whether the bank API is genuinely degraded.
2. **Wait for `CB_TIMEOUT`** (default `30s`). The circuit breaker will automatically transition to half-open and probe the bank.
3. **Check `BANK_API_ADDR`** matches the actual bank API in this environment.
4. **Force-close the circuit** — the service does not currently expose an admin force-close endpoint. To reset, perform a rolling restart of the pod:
   ```bash
   kubectl rollout restart deployment/c3-worker -n payflow
   ```
5. If the bank API remains flaky, consider increasing `CB_TIMEOUT` to reduce noisy open/close cycling.

> **No admin endpoint exists yet.** If you need to add one, register an HTTP handler in `cmd/worker/main.go` on a separate admin port (recommend `8091`) that calls `circuitBreaker.Reset()`. See ADR-003 for context.

---

### 🟡 "Outbox Entries Growing" — buffered results not draining

**Symptom:** `WARN outbox: relay attempt failed` repeating in logs; outbox pending count stays non-zero; `WARN outbox: dropping stale outbox entry` appearing (entries older than `MAX_TASK_DURATION`).

**Causes:**
- C2 coordinator is unreachable (network partition, pod restart)
- C2 TLS/auth certificate has expired
- The `x-contract-version` header mismatch (C3 and C2 built from incompatible commits)

**Remediation:**

1. **Check C2 connectivity from the C3 pod:**
   ```bash
   kubectl exec -it <c3-pod> -n payflow -- /bin/sh -c \
     "wget -q -O- http://<coordinator>:50051 || echo unreachable"
   ```
2. **Check if C2 cert has expired:**
   ```bash
   openssl s_client -connect <coordinator>:50051 2>/dev/null | openssl x509 -noout -dates
   ```
3. **Check contract version mismatch:** Look for `ERROR contract version mismatch` in C3 logs. Both C3 and C2 must be built from the same contract version (`c3/v2` as of this writing).
4. **Manual drain (once C2 is restored):** The outbox relay automatically resumes on reconnection — no manual action needed. Entries will drain on the next `OUTBOX_FLUSH_INTERVAL` tick.
5. **Delete stale entries** if `OUTBOX_DB_PATH` is backed by BadgerDB:
   ```bash
   # Only if you are sure entries are irrecoverably stale (> MAX_TASK_DURATION old)
   # Stop the pod first, then delete the DB directory and restart.
   kubectl delete pod <c3-pod> -n payflow
   kubectl exec <new-c3-pod> -- rm -rf /var/data/c3/outbox
   ```
   > ⚠️ This permanently drops undelivered results. Coordinate with the C2 team to resubmit affected tasks.

---

### 🟡 "Epoch Rejected Errors Spiking" — stale tasks arriving

**Symptom:** `WARN fencing: rejected stale task` in logs; `worker_tasks_total{status="rejected_epoch"}` counter climbing rapidly.

**Causes:**
- C2 has been restarted with a higher epoch but old task-dispatch messages are still in flight
- Clock skew between pods causing epoch ordering issues
- C2 deployment is rolling (mixed old/new epoch) — this is transient and expected

**Remediation:**

1. **Check if C2 deployment is in progress:** Epoch rejections during a C2 rolling redeploy are expected and will self-resolve. Monitor rate — should drop to zero within 2 × `HEARTBEAT_INTERVAL`.
2. **Verify clock sync** between C3 and C2 pods:
   ```bash
   kubectl exec -it <c3-pod> -- date
   kubectl exec -it <c2-pod> -- date
   ```
   Drift > 1 s is a problem. Ensure NTP is running on all nodes.
3. **Check C2 deployment version** — if C2 was rolled back, its epoch may be lower than what C3 has already accepted. In this case, restart C3 to reset its epoch validator:
   ```bash
   kubectl rollout restart deployment/c3-worker -n payflow
   ```
4. If rejections persist after C2 stabilises, escalate to the C2 team.

---

### 🟡 "Orphaned Lease Count > 0" — crashed mid-task

**Symptom:** `worker_orphaned_lease_count` gauge is non-zero after pod startup; logs show `INFO outbox: recovered N orphaned leases`.

**Causes:**
- C3 pod crashed (OOM, node eviction, SIGKILL) while a task was between `Acquire` and `Release`
- BadgerDB lease entries were written but the corresponding `DeleteLease` was never called

**Remediation:**

1. **This is handled automatically.** On startup C3 reads all lease records from BadgerDB and recovers them. The `OrphanedLeaseCount` metric reflects how many were found and recovered at boot.
2. **Verify C2 resubmitted those tasks.** Recovered leases mean C3 knows those task IDs were in flight. Check C2 logs to confirm those task results were eventually reported. If not, contact the C2 team to resubmit.
3. **Clear stale leases manually** only if you are certain C2 will not resubmit:
   ```bash
   # Identify the leased task IDs from C3 logs at startup (look for "lease recovery" messages)
   # Then remove the BadgerDB directory and restart (same caution applies as outbox drain above).
   kubectl rollout restart deployment/c3-worker -n payflow
   ```
4. If the count is consistently non-zero across restarts with no crash history, investigate whether `MAX_TASK_DURATION` is shorter than actual bank latency, causing tasks to time out before releasing their lease.

---

### 🔴 "/readyz returning 503 indefinitely"

**Symptom:** Kubernetes readiness probe fails; pod remains out of service rotation; `readyz` returns `503 Service Unavailable`.

**Causes:**
- C3 cannot connect to C2 at `COORDINATOR_ADDR`
- Network policy blocks egress from C3 pods to C2 on the gRPC port
- `COORDINATOR_ADDR` is misconfigured (wrong hostname or port)
- C2 pods are not yet running (ordering issue during cluster startup)

**Remediation:**

1. **Test C2 reachability directly from the C3 pod:**
   ```bash
   kubectl exec -it <c3-pod> -n payflow -- \
     /bin/sh -c "nc -zv <coordinator-host> 50051 && echo reachable || echo BLOCKED"
   ```
2. **Check `COORDINATOR_ADDR`** in the pod's environment:
   ```bash
   kubectl exec <c3-pod> -n payflow -- env | grep COORDINATOR
   ```
3. **Check Kubernetes NetworkPolicy** — ensure there is a policy allowing C3 → C2 egress on TCP port matching `GRPC_PORT`:
   ```yaml
   # NetworkPolicy example (adjust labels/namespaces as needed)
   apiVersion: networking.k8s.io/v1
   kind: NetworkPolicy
   metadata:
     name: allow-c3-to-c2
     namespace: payflow
   spec:
     podSelector:
       matchLabels:
         app: c3-worker
     policyTypes: [Egress]
     egress:
       - to:
           - podSelector:
               matchLabels:
                 app: c2-coordinator
         ports:
           - protocol: TCP
             port: 50051
   ```
4. **Check pod startup ordering.** If C2 starts after C3, readyz will fail until C2 is up. C3 will continuously retry the connection (`CONNECT_RETRY_DELAY`) — readyz should eventually flip to 200.
5. If in a service mesh (Istio/Linkerd), check that the sidecar proxy is not blocking plaintext gRPC.
