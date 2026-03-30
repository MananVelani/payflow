# PayFlow
### Fault-Tolerant Distributed Payment Processing System
> **Distributed Computing Lab — Final Project Proposal**
> Team of 5 · 4-Week Sprint · March 2026

---

| Resume Title | 5 Server-Side Components | Language / RPC |
|---|---|---|
| Distributed Task Orchestration Platform with Leader Election and Fault Recovery | ✅ Fully Compliant with Faculty Mandate | Go + gRPC + Protocol Buffers |

| Domain | Deployment | Data Architecture |
|---|---|---|
| Payment Processing · Fault Tolerance | Docker Compose — 10+ Containers | Decoupled — Each service owns its data |

---

## Table of Contents

1. [Faculty Compliance Checklist](#1-faculty-compliance-checklist)
   - [✅ Requirement 1 — The 5-Component Mandate](#-requirement-1--the-5-component-mandate)
   - [✅ Requirement 2 — Architectural Standards](#-requirement-2--architectural-standards)
   - [Single-Request Flow Across All 5 Components](#single-request-flow-across-all-5-components)
2. [Project Overview](#2-project-overview)
   - [Why Payment Processing?](#why-payment-processing)
   - [Syllabus Concept Mapping](#syllabus-concept-mapping)
3. [System Architecture](#3-system-architecture)
   - [Architecture Diagram](#architecture-diagram)
   - [Docker Compose Service Map](#docker-compose-service-map)
4. [Complete Feature List](#4-complete-feature-list)
   - [Core Features — Must Complete All](#core-features--must-complete-all)
   - [Advanced Features — Differentiators](#advanced-features--differentiators)
5. [Team Roles & Responsibilities](#5-team-roles--responsibilities)
   - [Member 1 · Coordinator Cluster & Leader Election (C2)](#member-1--coordinator-cluster--leader-election-c2)
   - [Member 2 · Worker Service & Payment Execution (C3)](#member-2--worker-service--payment-execution-c3)
   - [Member 3 · API Gateway, gRPC Layer & Client SDK (C1)](#member-3--api-gateway-grpc-layer--client-sdk-c1)
   - [Member 4 · Payment Log Service (C4) — Dedicated Data Component](#member-4--payment-log-service-c4--dedicated-data-component)
   - [Member 5 · Monitoring Service, Testing & Infrastructure (C5)](#member-5--monitoring-service-testing--infrastructure-c5)
6. [Four-Week Sprint Roadmap](#6-four-week-sprint-roadmap)
   - [Week 1: Foundation & Infrastructure](#week-1-foundation--infrastructure)
   - [Week 2: Core Distributed Logic](#week-2-core-distributed-logic)
   - [Week 3: Fault Tolerance & Advanced Features](#week-3-fault-tolerance--advanced-features)
   - [Week 4: Integration, Testing & Hardening](#week-4-integration-testing--hardening)
7. [Milestones & Failure Scenario Test Matrix](#7-milestones--failure-scenario-test-matrix)
   - [Project Milestones](#project-milestones)
   - [Failure Scenario Test Matrix](#failure-scenario-test-matrix-member-5-runs-all-5)
8. [Technology Stack & Repository Layout](#8-technology-stack--repository-layout)
   - [Repository Structure](#repository-structure)
9. [Hard Challenges & Solutions](#9-hard-challenges--solutions)
10. [Resume Bullets & Interview Talking Points](#10-resume-bullets--interview-talking-points)
    - [Resume Bullet Points](#resume-bullet-points)
    - [Viva Questions to Prepare](#viva-questions-to-prepare)
    - [Final Submission Checklist](#final-submission-checklist)

> **Note:** This Table of Contents is generated via field codes. To ensure page number accuracy after editing, please right-click the TOC and select "Update Field."

---

## 1. Faculty Compliance Checklist

> This section maps PayFlow against every criterion in the Common Instructions mandate. **Review this before all other sections.**

---

### ✅ Requirement 1 — The 5-Component Mandate

PayFlow deploys **5 functionally distinct server-side services**, each in its own isolated Docker container with its own codebase, responsibility, and runtime:

| # | Service | Logical Role | Container(s) | Network Protocol |
|---|---|---|---|---|
| **C1** | **API Gateway** | Client-facing entry point. Validates JWT, applies rate limiting, discovers current leader coordinator, and proxies payment requests. Handles leader-redirect transparently. | `api-gateway` | REST HTTP/2 + gRPC |
| **C2** | **Coordinator Service** | Leader election (Bully Algorithm), task scheduling, worker pool management, heartbeat monitoring. 3 instances run same binary but take distinct runtime roles (LEADER / FOLLOWER / CANDIDATE) — qualifies under complex consensus logic. | `coordinator-1/2/3` | gRPC (inter-node) |
| **C3** | **Worker Service** | Executes payment processing tasks by calling the Mock Bank API. Sends periodic heartbeats to the coordinator. Implements retry with idempotency key checks. Scales horizontally. | `worker-1..N` | gRPC |
| **C4** | **Payment Log Service** | Dedicated stateful data service. Owns the append-only replicated payment log, idempotency deduplication store, and transaction audit records. No other service accesses its BoltDB store directly. | `payment-log` | gRPC |
| **C5** | **Monitoring Service** | Scrapes Prometheus metrics from all services. Serves a real-time WebSocket dashboard showing coordinator ring, worker pool health, queue depth, throughput, and latency charts. | `monitor` | HTTP (Prometheus) + WebSocket |

> ⚡ **Note on Coordinator Cluster:** 3 instances run the same binary but implement distinct runtime roles (LEADER vs FOLLOWER vs CANDIDATE) using the Bully election algorithm with epoch tracking and quorum checks — this is analogous to the P2P exception for complex distributed logic. They are counted as **ONE logical component (C2)** with 3 replicas.

---

### ✅ Requirement 2 — Architectural Standards

| Mandate | Status | How PayFlow Satisfies It |
|---|---|---|
| **Functional Decomposition** — No 5 identical copies | ✅ PASS | Each component has a completely different Go package and responsibility: Gateway (routing/auth), Coordinator (election/scheduling), Worker (execution), Payment Log (stateful storage), Monitor (observability). Zero code sharing across service binaries. |
| **Inter-Component Dependency** — No islands | ✅ PASS | A single payment request traverses ALL 5 components in sequence: Client → C1 (validate) → C2 (schedule) → C4 (log commit) → C3 (execute) → C5 (metrics). Remove any one container and the payment flow breaks. |
| **No Fat Clients** — Frontend excluded | ✅ PASS | The dashboard UI is server-side rendered and pushed via WebSocket from C5 (a Go server). The browser is a pure display client. All 5 counted components are server-side services. |
| **Decoupled Data** — No shared database monolith | ✅ PASS | C4 (Payment Log Service) is the only service with a persistent data store (BoltDB inside its container). Coordinator state is in-memory across its cluster. Workers are stateless. Gateway is stateless. Each service boundary is clear. |
| **Network Communication** over REST/gRPC/WS/MQ | ✅ PASS | C1↔C2: gRPC. C2↔C3: gRPC stream. C2↔C4: gRPC. All nodes→C5: HTTP Prometheus scrape. C5→Browser: WebSocket. Zero hard-coded function calls between containers. |
| **Standalone containers / isolated runtimes** | ✅ PASS | `docker-compose.yml` defines 10+ named services. Each has its own Dockerfile, network binding, environment variables, and restart policy. Containers start, crash, and recover independently. |

---

### Single-Request Flow Across All 5 Components

> **Proof of Inter-Component Dependency:** every payment touches all 5 server-side services.

| Step | Component | Action | Network Call | If This Container Dies |
|---|---|---|---|---|
| **1** | **C1 — API Gateway** | Receives `POST /v1/payments`. Validates JWT. Discovers leader coordinator. Forwards request via gRPC. | REST in → gRPC out | Client gets 503. No other component affected. |
| **2** | **C2 — Coordinator (Leader)** | Generates `task_id` + `idempotency_key`. Calls C4 to check for duplicate. Writes task to C4 log. Assigns to a healthy worker (C3). | gRPC to C4, gRPC to C3 | New Bully election. New leader reconstructs task queue from C4. |
| **3** | **C4 — Payment Log Service** | Receives `AppendEntry()` from coordinator. Writes to BoltDB. Returns ACK with `log_index`. Also answers idempotency lookups. | gRPC in/out | Coordinator queues writes in memory; replays when C4 restarts. |
| **4** | **C3 — Worker Service** | Checks idempotency key. Calls Mock Bank API (50–500ms). Returns SUCCESS or FAILURE with result to coordinator. | gRPC in/out | Coordinator detects via heartbeat timeout. Task reassigned. |
| **5** | **C2 — Coordinator** | Receives result. Calls C4 to write final status. Notifies gateway of completion. | gRPC to C4 | Same election recovery as step 2. |
| **6** | **C5 — Monitor Service** | Scrapes `/metrics` from all services every 15s. Pushes live state update to dashboard over WebSocket. | HTTP scrape + WS push | Metrics data gap only. Core payment flow unaffected. |

---

## 2. Project Overview

PayFlow is a production-grade payment processing engine built on a fault-tolerant distributed backbone. It mirrors real-world systems like **Stripe's** charge pipeline, **PayPal's** async settlement queue, and **Razorpay's** retry architecture. When a coordinator crashes mid-transaction, the system detects the failure, elects a new leader via Bully algorithm, and resumes from the Payment Log Service — **zero payments lost**.

---

### Why Payment Processing?

- **Exactly-once semantics** — a charge must never apply twice, even if a worker crashes mid-execution.
- **Strong consistency** — every coordinator must agree on payment success before the client is notified.
- **Automatic failover** — a crashed node must be replaced in under 5 seconds with no manual intervention.
- **Idempotency keys** — duplicate payment submissions must resolve to a single transaction.
- **Audit trail** — every state change must be logged, ordered, and replayable for compliance.

---

### Syllabus Concept Mapping

| Module | Concept | Component | PayFlow Implementation |
|---|---|---|---|
| Module 2 | RPC / Communication | C1, C2, C3, C4 | gRPC for all inter-service calls; Protocol Buffers for payment message serialization; REST at C1 gateway boundary |
| Module 3 | Process Management | C2, C3 | Worker pool with dynamic load balancing; weighted task assignment; coordinator manages worker lifecycle |
| Module 4 | Leader Election | C2 Cluster | Bully Algorithm across 3 coordinator nodes; epoch numbers prevent split-brain; election triggers on heartbeat timeout |
| Module 5 | Consistency & Replication | C2, C4 | C4 (Payment Log Service) maintains append-only log; 2-of-3 quorum writes; coordinator leader forwards all writes to C4 |
| Module 6 | Fault Tolerance | C2, C3, C5 | Heartbeat-based failure detection; automatic task reassignment; coordinator state rebuilt from C4 log on failover |
| Module 6 | Mutual Exclusion | C2, C4 | Only the elected leader may dispatch tasks; epoch-tagged messages block stale leaders; C4 locks payment IDs during 2PC |

---

## 3. System Architecture

> 5 server-side services communicating exclusively over the network — **zero shared function calls, zero shared databases.**

---

### Architecture Diagram

*The system architecture follows a layered approach with clear separation of concerns:*

```
External Client / SDK
        │
        ▼  REST / gRPC
┌─────────────────────────────────┐
│   C1: API Gateway Service       │
│   (auth, rate-limit, routing)   │
└─────────────────────────────────┘
        │
        ▼  gRPC (SubmitTask)
┌─────────────────────────────────────────────────┐
│         C2: Coordinator Cluster (3 containers)  │
│  [coordinator-1 LEADER] [coordinator-2]         │
│  [coordinator-3]  ←—— Bully election ——→        │
└─────────────────────────────────────────────────┘
        │                        │
        ▼ gRPC (AssignTask)       ▼ gRPC (AppendEntry)
┌──────────────────┐    ┌──────────────────────────────┐
│  C3: Worker Svc  │    │  C4: Payment Log Service     │
│  [worker-1..N]   │    │  (Append-only BoltDB log,    │
│                  │    │   owns all data)              │
└──────────────────┘    └──────────────────────────────┘
        │                        │
        └────────────┬───────────┘
                     ▼
        ┌─────────────────────────────┐
        │  C5: Monitoring Service     │
        │  (Prometheus + WebSocket)   │
        └─────────────────────────────┘
```

---

### Docker Compose Service Map

| Container | Component | Port(s) | Replicas | Data Store |
|---|---|---|---|---|
| `api-gateway` | C1 | 8080 (REST), 9090 (gRPC) | 1 | None — stateless |
| `coordinator-1` | C2 (init LEADER) | 50051 | 1 of 3 | In-memory queue + epoch log |
| `coordinator-2` | C2 (FOLLOWER) | 50052 | 1 of 3 | In-memory queue + epoch log |
| `coordinator-3` | C2 (FOLLOWER) | 50053 | 1 of 3 | In-memory queue + epoch log |
| `worker-1 ... worker-5` | C3 | Dynamic (registered) | 1–N (scalable) | None — stateless |
| `payment-log` | C4 | 50054 | 1 | BoltDB volume (owns all payment state) |
| `monitor` | C5 | 3000 (UI), 9091 (Prometheus) | 1 | Prometheus time-series only |
| `jaeger` (optional) | Tracing | 16686 (UI) | 1 | Trace spans only |

---

## 4. Complete Feature List

### Core Features — Must Complete All

| # | Comp. | Feature | Description | Owner | Difficulty |
|---|---|---|---|---|---|
| C01 | C2 | **Bully Leader Election** | 3 coordinator nodes elect a leader using Bully algorithm. Messages: ELECTION, OK, COORDINATOR. Triggers on startup + missed heartbeat. | M1 | ★★★★☆ |
| C02 | C2 | **Epoch-Tagged Messages** | Every gRPC message carries epoch number. Followers reject lower-epoch messages. Prevents split-brain. | M1 | ★★★☆☆ |
| C03 | C2 | **Worker Heartbeat Monitor** | Leader checks heartbeat from each worker every 2s. Worker marked DEAD after 3 missed beats (6s). Triggers task reassignment. | M1 | ★★★☆☆ |
| C04 | C1 | **Payment Submission API** | REST `POST /v1/payments` + gRPC `SubmitTask()`. Accepts amount, currency, merchant_id, idempotency_key. Returns txn_id. | M3 | ★★☆☆☆ |
| C05 | C1 | **Leader Redirect** | If request hits a follower coordinator, it returns the leader's address. Gateway SDK auto-follows redirect transparently. | M3 | ★★☆☆☆ |
| C06 | C3 | **Payment Execution Engine** | Workers poll coordinator for tasks. Call Mock Bank API (50–500ms, 10% fail). Return SUCCESS/FAILURE + result. | M2 | ★★★☆☆ |
| C07 | C3 | **Heartbeat Sender** | Worker pings coordinator every 2s with worker_id + load metrics. Stops pinging on coordinator unreachable. | M2 | ★★☆☆☆ |
| C08 | C4 | **Append-Only Payment Log** | Dedicated gRPC service with BoltDB store. Implements `AppendEntry()`, `GetEntry()`, `GetLogRange()`, `CheckIdempotency()`. Owns all payment state. | M4 | ★★★☆☆ |
| C09 | C4 | **Idempotency Deduplication** | Stores `idempotency_key → (txn_id, result)`. Re-submission returns cached result without re-dispatching. Prevents double charge. | M4 | ★★★☆☆ |
| C10 | C2+C4 | **Leader Failover + Log Recovery** | New leader calls `C4.GetAllPending()` on election win. Reconstructs full task queue. Resumes dispatch without losing any payment. | M1+M4 | ★★★★★ |
| C11 | C5 | **Prometheus Metrics Endpoints** | Each service exposes `/metrics`: election count, task throughput, heartbeat miss rate, queue depth, log append rate. | M5 | ★★★☆☆ |
| C12 | C5 | **Real-Time Dashboard** | Go WebSocket server. Shows: coordinator ring (leader highlighted), worker health grid, queue depth gauge, throughput graph, p50/p99 chart. | M5 | ★★★☆☆ |

---

### Advanced Features — Differentiators

| # | Comp. | Feature | Description | Owner | Difficulty |
|---|---|---|---|---|---|
| A01 | C3 | **Retry + Exponential Backoff** | FAILED payments auto-retry up to 3× with backoff (1s, 2s, 4s). Same `idempotency_key` on each retry. | M2 | ★★★☆☆ |
| A02 | C2+C3 | **Weighted Load Balancing** | Coordinator tracks worker speed and failure rate. Assigns more tasks to faster workers. Rebalances every 30s. | M2 | ★★★★☆ |
| A03 | C4 | **2-Phase Commit (High-Value)** | Payments > $10,000 trigger 2PC: C4 locks txn on PREPARE, then COMMIT or ROLLBACK based on worker result. | M4 | ★★★★★ |
| A04 | C5 | **Fault Injection CLI** | CLI: `kill-coordinator <id>`, `kill-worker <id>`, `delay-network <ms>`, `partition-nodes <ids>`. | M5 | ★★★★☆ |
| A05 | C1 | **Distributed Tracing (Jaeger)** | `trace_id` injected at C1 gateway, propagated via gRPC metadata through C2, C3, C4. Visible end-to-end in Jaeger. | M3 | ★★★☆☆ |
| A06 | C4 | **Audit Log Export** | CLI exports full JSON audit trail for any `txn_id` from C4's BoltDB: all state transitions, epoch, worker_id, timestamps. | M4 | ★★★☆☆ |
| A07 | C5 | **Dynamic Worker Scaling** | Monitor watches `queue_depth` metric. Calls Docker API to spin up new worker container when queue > 50. | M5 | ★★★★☆ |

---

## 5. Team Roles & Responsibilities

> Each member owns one server-side component (C1–C5). Work proceeds **in parallel** after Week 1 (once `.proto` files are defined).

---

### Member 1 · Coordinator Cluster & Leader Election (C2)

**Owns:** `CoordinatorNode`, Bully election, epoch management, heartbeat server, worker pool tracking

**Deliverables:**
- Build `CoordinatorNode` struct: ID, state (LEADER/FOLLOWER/CANDIDATE), epoch counter, worker registry
- Implement Bully election: broadcast ELECTION to higher-ID nodes, handle OK response, announce COORDINATOR
- Handle concurrent elections, node rejoin after crash, epoch persistence across restarts
- Leader dispatches tasks to workers via gRPC; tracks in-progress tasks with `worker_id` + timestamp
- On election win: call C4 (Payment Log Service) via gRPC `GetAllPending()` to reconstruct task queue
- Write integration tests: kill leader → verify new election < 5s → pending payments resume from C4

> **⚠ Hard Challenge:** Preventing split-brain when network is slow (not fully partitioned).
> **✓ Solution:** Epoch numbers in every gRPC message; any node receiving a lower-epoch message ignores it; C4 rejects `AppendEntry()` from stale leaders.

---

### Member 2 · Worker Service & Payment Execution (C3)

**Owns:** `WorkerNode`, mock bank gateway, heartbeat client, retry engine, weighted load reporting

**Deliverables:**
- Build `WorkerNode`: gRPC registration with coordinator, task polling stream, result reporting
- Mock Bank API: configurable latency (50–500ms), failure rate (10%), `idempotency_key` result cache
- Heartbeat sender: ping coordinator every 2s with `worker_id`, `current_load`, `tasks_processed_count`
- Retry engine: exponential backoff (1s, 2s, 4s) for FAILED tasks; same `idempotency_key` on each retry
- Weighted load reporting: expose `avg_task_duration_ms` and `processing_capacity` for coordinator's load balancer
- REVOKE handler: if coordinator reassigns a task mid-execution, worker discards result on completion

> **⚠ Hard Challenge:** Worker succeeds at bank API, then crashes before sending result. New worker checks C4 idempotency → finds PENDING → bank returns cached result (same `idempotency_key`) → no double charge.
> **Must test this scenario explicitly.**

---

### Member 3 · API Gateway, gRPC Layer & Client SDK (C1)

**Owns:** All `.proto` definitions, gRPC stubs, C1 API Gateway, Go client SDK, distributed tracing

**Deliverables:**
- Design all `.proto` files: `payment.proto`, `coordinator.proto`, `worker.proto`, `log.proto` — shared contract for all 5 services
- Build C1 API Gateway (Go HTTP/2 → gRPC): `POST /v1/payments`, `GET /v1/payments/{id}`, `POST /v1/batch`
- Leader redirect: if coordinator returns `NOT_LEADER` error, gateway updates leader cache and retries
- Go client SDK: `SubmitPayment()`, `GetStatus()`, `SubmitBatch()` with auto-retry and exponential backoff
- Inject OpenTelemetry `trace_id` at gateway; propagate via gRPC metadata to C2, C3, C4
- Load test script: 1000 concurrent payments; measure p50/p95/p99; document throughput results

> **⚠ Hard Challenge:** All 5 services depend on the `.proto` files written by M3 — a breaking schema change blocks the whole team.
> **✓ Solution:** Use reserved fields, `oneof`, and versioned service names from Day 1.

---

### Member 4 · Payment Log Service (C4) — Dedicated Data Component

**Owns:** C4 Payment Log Service standalone container, BoltDB store, append-only log, idempotency map, 2PC, audit trail

**Deliverables:**
- Build C4 as a standalone Go gRPC server with its own Dockerfile and BoltDB volume — this is the **Decoupled Data** component
- Implement `AppendEntry(entry) → log_index`; `GetEntry(id)`; `GetLogRange(from,to)`; `GetAllPending()` for recovery
- Idempotency store: `CheckIdempotency(key) → (exists, result)`; `WriteResult(key, result)` — called by coordinator before dispatch
- Log recovery endpoint: `GetAllPending()` returns all QUEUED and IN_PROGRESS entries for new coordinator leader
- 2-Phase Commit: `HandlePrepare(txn_id)` locks entry in BoltDB; `HandleCommit/Rollback(txn_id)` finalizes state
- AuditExport CLI: reads C4 BoltDB and generates JSON audit trail for any `txn_id`

> **⚠ Hard Challenge:** C4 is a single point of failure.
> **✓ Mitigation:** BoltDB fsync on every write (durable, recovers on restart). Coordinator buffers writes in memory during C4 downtime and replays in order on reconnect. Implement back-pressure at C1 if buffer exceeds 60s.

---

### Member 5 · Monitoring Service, Testing & Infrastructure (C5)

**Owns:** C5 Monitoring Service, Prometheus, WebSocket dashboard, fault injection CLI, Docker setup, CI/CD

**Deliverables:**
- Build C5 standalone Go server: Prometheus scraper (pulls `/metrics` from all 4 services), WebSocket live-event broadcaster
- Dashboard panels: coordinator ring (who is leader), worker health grid, task queue depth gauge, live throughput graph, p50/p99 bar
- Fault injection CLI: `kill-coordinator <id>`, `kill-worker <id>`, `delay-network <ms>`, `partition-nodes <ids>`
- Write `docker-compose.yml`: all 10+ containers with health checks, restart policies, network aliases, named BoltDB volume for C4
- Run all 5 failure scenarios from the test matrix; document expected vs actual behavior; fix divergences
- GitHub Actions CI: unit tests + 3 fault integration tests on every PR; block merge if recovery SLA > 5s

> **⚠ Hard Challenge:** Accurately showing distributed state on the dashboard when C1–C4 may have slightly different views at any instant.
> **✓ Solution:** Show 'as of last scrape' timestamp and highlight staleness > 30s clearly in the UI.

---

## 6. Four-Week Sprint Roadmap

> **Daily stand-up:** 15 minutes every morning.
> **Friday review:** demo what you built, identify blockers, unblock each other.

---

### Week 1: Foundation & Infrastructure

> 🎯 **Goal:** All 5 services have a running Docker skeleton. gRPC contracts are defined. Containers can communicate. No payment business logic yet.

| Day | Who | Task |
|---|---|---|
| Mon | All | Create GitHub mono-repo. Define branch strategy (feature branches per component, PR required). Install Go 1.22, Docker, protoc, grpc plugins. |
| Mon–Tue | M3 | **PRIORITY:** Write all `.proto` files (payment, coordinator, worker, log). Generate gRPC stubs. Commit to `/proto`. All other members depend on this contract. |
| Tue–Wed | M5 | Write `docker-compose.yml`: all 10 services defined with placeholder images, health checks, named volumes, `payflow-net` Docker network. |
| Wed | M1 | `CoordinatorNode` skeleton: hard-code node-1 as leader. Expose gRPC server. Return dummy response to `SubmitTask()`. No real logic yet. |
| Wed | M2 | `WorkerNode` skeleton: register with coordinator via gRPC `RegisterWorker()`. Start heartbeat loop. Poll for tasks (empty queue OK). |
| Wed–Thu | M4 | C4 Payment Log Service skeleton: standalone Go gRPC server + BoltDB init. Implement `AppendEntry()` and `GetEntry()`. No coordinator integration yet. |
| Thu | M3 | C1 API Gateway skeleton: `POST /v1/payments` returns dummy `txn_id`. Wires through to coordinator via gRPC. Enough to test the pipe. |
| Fri | All | **Integration day:** dummy payment via Gateway → Coordinator → Worker → C4 log write. All 5 containers must participate. Fix integration blockers. |

---

### Week 2: Core Distributed Logic

> 🎯 **Goal:** Full Bully election works end-to-end. Payments flow through all 5 services. Log replication via C4 is live. Worker heartbeat failure detection triggers reassignment.

| Day | Who | Task |
|---|---|---|
| Mon | M1 | Implement full Bully election: ELECTION/OK/COORDINATOR messages between 3 coordinator containers. Test: `docker kill coordinator-1` → new leader in < 5s. |
| Mon–Tue | M4 | Real C4 log writes: coordinator calls `C4.AppendEntry()` for every payment; C4 writes to BoltDB with fsync; returns `log_index`. Wire in idempotency check. |
| Tue | M2 | Real payment execution: Worker calls mock bank API (50–500ms, 10% fail). Sends `PaymentResult` back to coordinator. Coordinator calls C4 to write final status. |
| Tue–Wed | M3 | Wire full payment flow: C1 Gateway → C2 Coordinator (leader check) → task dispatch → C3 Worker execution → C4 log update → response to client. |
| Wed | All | **Integration checkpoint:** Submit 20 real payments via load script. Verify: (a) all in C4 log, (b) all processed by workers, (c) results returned. |
| Wed–Thu | M1 | Epoch tagging: add epoch to all gRPC messages. Followers and C4 reject lower-epoch messages. Test with concurrent election simulation. |
| Thu | M2 | Heartbeat failure detection: coordinator marks worker DEAD after 3 missed beats. Task state in C4 updated to QUEUED. Reassigned to next healthy worker. |
| Thu–Fri | M4 | Idempotency deduplication in C4: `CheckIdempotency()` before writing new task. Test: submit same `idempotency_key` twice → same `txn_id`, no second dispatch. |
| Fri | M5 | Dashboard skeleton live: coordinator health (leader/follower/dead), worker count, task counter. Prometheus `/metrics` working on C1, C2, C3. |

---

### Week 3: Fault Tolerance & Advanced Features

> 🎯 **Goal:** System survives all 5 failure scenarios. Advanced features implemented. Dashboard shows live fault events.

| Day | Who | Task |
|---|---|---|
| Mon | M1+M4 | Leader failover + C4 log recovery: new leader calls `C4.GetAllPending()` on election win. Reconstructs task queue. Test: kill leader with 30 queued tasks → all complete with no loss. |
| Mon–Tue | M2 | Worker crash + reassignment: kill worker mid-task → coordinator detects via heartbeat → C4 task state → QUEUED → new worker picks up → idempotency prevents double charge. |
| Tue | M5 | Fault injection CLI: implement `kill-coordinator` and `kill-worker`. Test they trigger correct recovery. Add `delay-network` using `tc netem` in container. |
| Tue–Wed | M4 | 2-Phase Commit for high-value payments (> $10,000): C4 PREPARE locks txn; COMMIT/ROLLBACK based on worker result. Test: inject failure between PREPARE and COMMIT. |
| Wed | M3 | Add distributed tracing: inject `trace_id` at C1 gateway, propagate via gRPC metadata through C2, C3, C4. Verify end-to-end in Jaeger UI. |
| Wed–Thu | M2 | Retry engine: FAILED tasks auto-retry 3× with exponential backoff. Verify exactly-once execution via C4 idempotency store. |
| Thu | M5 | Complete monitoring dashboard: coordinator ring animation, worker health grid with heartbeat timestamps, throughput graph, p50/p99 latency chart. |
| Thu–Fri | M5 | Dynamic worker scaling: C5 monitor watches `queue_depth` metric. Calls Docker API to launch new `worker-N` container when queue > 50. |
| Fri | All | **Chaos day:** run all 5 failure scenarios. Document expected vs actual. Every member witnesses their component's recovery behavior. |

---

### Week 4: Integration, Testing & Hardening

> 🎯 **Goal:** All SLAs met. Load test documented. Code reviewed. Demo video recorded. Report complete.

| Day | Who | Task |
|---|---|---|
| Mon | M3 | Load test: 1000 concurrent payments. Target: > 200 tx/s, p99 < 500ms. Profile with `pprof`. Fix top 3 hotspots. |
| Mon–Tue | M4 | Audit log export CLI: reads C4 BoltDB, generates JSON audit trail for any `txn_id` showing all state transitions with epoch, `worker_id`, timestamp. |
| Tue | M5 | GitHub Actions CI: unit tests + 3 fault scenarios on every PR. Block merge if recovery time exceeds 5s SLA. |
| Tue–Wed | M1 | Edge cases: coordinator rejoin after crash (prevent epoch reset), concurrent elections, stale COORDINATOR messages after partition heal. |
| Wed | All | **Code review:** each member reviews another's PR. Enforce doc comments on all exported functions, no unguarded globals, no TODOs in payment path. |
| Thu | All | **Record 5-min demo:** start cluster → batch payments → kill coordinator → show election in dashboard → payments resume → kill worker → show reassignment. |
| Thu–Fri | All | Write final report: architecture diagram, algorithm choice justification, CAP trade-off, consistency model, performance data, lessons learned. |
| Fri | All | Tag `v1.0.0`. Verify: `docker compose up --build` starts all 10+ containers on a fresh machine cleanly. **Submit report + demo link.** |

---

## 7. Milestones & Failure Scenario Test Matrix

### Project Milestones

| M# | Deadline | Deliverable | Success Criteria |
|---|---|---|---|
| **M1** | End Week 1 | All 5 components in Docker; gRPC stubs generated; all containers communicate over `payflow-net` | `curl POST /v1/payments` → C1 → C2 → C3 → C4 log write; all 5 containers involved |
| **M2** | Mid Week 2 | Bully election working + real payment flow end-to-end through all 5 services | Kill `coordinator-1` → new election < 5s; payment API returns real `txn_id` logged in C4 |
| **M3** | End Week 2 | C4 quorum log writes + worker heartbeat failure detection + idempotency dedup | Dead worker's task reassigned within 6s; duplicate `idempotency_key` returns same `txn_id` |
| **M4** | Mid Week 3 | Full fault tolerance: coordinator failover with C4 log recovery + worker crash recovery | Kill leader with 30 queued tasks → new leader recovers all from C4 → all 30 complete; 0 lost |
| **M5** | End Week 3 | All 5 advanced features + monitoring dashboard live | Dashboard shows live coordinator ring; fault injection CLI works; 2PC verified on $10k+ payments |
| **M6** | Mid Week 4 | Load test: 1000 concurrent payments | ≥ 200 tx/s; p99 < 500ms; 0 duplicate charges in 10,000 submission run |
| **M7** | End Week 4 | Final code tag, demo video, written report | `v1.0.0` tagged; single `docker compose up` starts everything; 5-min demo video uploaded |

---

### Failure Scenario Test Matrix (Member 5 Runs All 5)

| # | Scenario | CLI Command | Expected Behavior | SLA | Components |
|---|---|---|---|---|---|
| **S1** | Leader coordinator crashes | `kill-coordinator 1` | Bully election → new leader reads C4 `GetAllPending()` → queue resumes | Recovery < 5s; 0 lost | C2, C4 |
| **S2** | Worker crashes mid-payment | `kill-worker 3` | Heartbeat timeout (6s) → C4 task → QUEUED → reassigned → idempotency prevents double charge | Reassign < 6s; executes once | C2, C3, C4 |
| **S3** | Duplicate payment submission | Same `idempotency_key` twice | C4 `CheckIdempotency()` → returns existing result; bank not called again | 100% dedup accuracy | C1, C4 |
| **S4** | Payment Log Service (C4) crashes | `docker kill payment-log` | Coordinator buffers writes in-memory; on C4 restart replays buffered entries in order | 0 payments lost; replay < 10s | C2, C4 |
| **S5** | All workers die simultaneously | `kill-worker all` | Tasks remain QUEUED in C4; on worker restart + re-register, coordinator dispatches full queue | Queue fully preserved | C2, C3, C4 |

---

## 8. Technology Stack & Repository Layout

| Category | Technology | Justification |
|---|---|---|
| **Language** | Go 1.22 | Goroutines handle concurrent heartbeat loops, gRPC servers, and background watchers elegantly. Built-in race detector catches distributed bugs. |
| **RPC** | gRPC + Protocol Buffers 3 | Industry standard. Binary format is fast. Strong typing enforces service contracts between all 5 components at compile time — satisfies Network Communication mandate. |
| **API Gateway** | Go HTTP/2 + grpc-gateway | C1 exposes REST for external clients; internally calls C2 via gRPC. `grpc-gateway` auto-generates REST from `.proto` definitions. |
| **C4 Data Store** | BoltDB (embedded key-value) | C4 is the ONLY service with a persistent store. BoltDB is embedded (no separate DB container), append-only transactions, fsync on every write. Satisfies Decoupled Data mandate. |
| **Coordinator State** | In-memory Go map + mutex | C2 state is ephemeral and reconstructed from C4 on election win. Keeps C2 stateless across restarts. |
| **Containerization** | Docker + Docker Compose | 10+ services in one `docker-compose.yml`. Kill/restart individual containers to test failures. Each container is a truly isolated runtime. |
| **Monitoring** | Prometheus + Go WebSocket | Prometheus scrapes `/metrics` from each service. C5 pushes live events over WebSocket to dashboard. No shared state between C5 and other services. |
| **Tracing** | OpenTelemetry + Jaeger | `trace_id` propagated via gRPC metadata across all 5 services. End-to-end latency breakdown in Jaeger UI. |
| **Testing** | Go testing + Testcontainers-Go | Testcontainers spins up real Docker containers in CI — tests actual network failures, not mocks. |
| **CI/CD** | GitHub Actions | Unit tests + 3 fault integration scenarios on every PR. Merge blocked if recovery SLA > 5s. |

---

### Repository Structure

| Path | Contents & Owner |
|---|---|
| `payflow/proto/` | All `.proto` files: payment, coordinator, worker, log services — M3 (shared contract) |
| `payflow/gateway/` | C1: API Gateway (REST + gRPC client, auth, tracing) — M3 |
| `payflow/coordinator/` | C2: CoordinatorNode, Bully election, task dispatcher, heartbeat server — M1 |
| `payflow/worker/` | C3: WorkerNode, mock bank API, heartbeat client, retry engine — M2 |
| `payflow/payment-log/` | C4: Payment Log Service standalone gRPC server + BoltDB store — M4 |
| `payflow/monitor/` | C5: Monitoring server (Prometheus scraper + WebSocket dashboard) — M5 |
| `payflow/chaos/` | Fault injection CLI (kill, delay, partition) — M5 |
| `payflow/sdk/` | Go client SDK: `SubmitPayment()`, `GetStatus()`, `SubmitBatch()` — M3 |
| `payflow/tests/` | Integration tests + 5 failure scenario tests — M5 |
| `docker-compose.yml` | Full cluster: api-gateway, coordinator-1/2/3, worker-1..5, payment-log, monitor — M5 |
| `.github/workflows/` | CI pipeline: unit tests + fault scenario tests — M5 |

---

## 9. Hard Challenges & Solutions

> These are the distributed systems traps that catch most student teams. **Study these before starting.**

---

### 1. Split-Brain During Leader Election (C2)

**⚠ Problem:** Network is slow but not partitioned. C2 misses C1's heartbeat → starts election → declares itself leader. Both C1 and C2 accept writes simultaneously. Payments dispatched twice.

**✓ Solution:** Epoch numbers. Every election increments the epoch. Every gRPC message embeds the sender's epoch. Any node receiving a lower-epoch message ignores it. C4 (Payment Log Service) rejects `AppendEntry()` calls carrying an outdated epoch — it acts as the final arbiter.

**★ Gotcha:** Epoch must survive restarts. Store epoch in C4's BoltDB so a restarting coordinator reads back the current epoch before rejoining the cluster.

---

### 2. Exactly-Once Payment Execution Across C3 ↔ C4

**⚠ Problem:** Worker calls bank API (succeeds, card charged). Crashes before sending result to coordinator. Coordinator reassigns task. New worker checks C4 idempotency — finds PENDING (coordinator wrote it before dispatch). Bank API is called again → double charge.

**✓ Solution:** Worker always checks C4 `CheckIdempotency()` BEFORE calling bank API. Mock bank also caches result by `idempotency_key`. Coordinator writes FINAL status to C4 only after receiving worker ACK — not before dispatch. Coordinator writes PENDING to C4 at dispatch time; worker sees PENDING, calls bank, bank returns cached SUCCESS, worker reports SUCCESS to coordinator.

**★ Gotcha:** The race window between coordinator writing PENDING and worker checking it. Worker must always check C4 first — never assume a task is fresh.

---

### 3. C4 (Payment Log Service) Is a Single Point of Failure

**⚠ Problem:** C4 is the source of truth for all payment state. If it crashes, coordinators cannot confirm writes, cannot check idempotency, and cannot support log recovery. The payment pipeline stalls entirely.

**✓ Solution:** BoltDB uses fsync on every transaction — C4 recovers its full state from disk with zero data loss on restart. Coordinator queues writes in-memory during C4 downtime and replays them in order on C4 reconnect. Implement back-pressure at C1: if write buffer exceeds 60s of capacity, C1 returns 503 with `retry-after` header rather than silently dropping payments.

**★ Gotcha:** The in-memory buffer has a size limit. Communicate this limit clearly in the API docs so clients know to retry rather than assuming the payment was lost.

---

### 4. Slow Worker vs. Dead Worker — Timing Assumptions

**⚠ Problem:** A worker is in a GC pause (not dead). Coordinator misses 3 heartbeats (6s) → marks worker DEAD → reassigns task. GC pause ends → original worker finishes → sends result. Two results arrive for the same task.

**✓ Solution:** When coordinator marks a worker DEAD and reassigns, it sends `REVOKE(task_id)` gRPC call to the old worker. Worker sets `revoked[task_id]=true`. After bank call completes, worker checks the revoked flag — if set, discards result and reports nothing. C4 idempotency key is the fallback defense if REVOKE is lost.

**★ Gotcha:** REVOKE is best-effort. The C4 idempotency key is the **absolute last line of defense**. It must be treated as non-negotiable, not an optimization.

---

## 10. Resume Bullets & Interview Talking Points

### Resume Bullet Points

- Built **PayFlow** — a 5-service fault-tolerant distributed payment system processing 200+ tx/s, auto-recovering from coordinator failures in < 5s using Bully leader election.
- Designed a dedicated **Payment Log Service (C4)** as a decoupled stateful data layer with append-only BoltDB storage, serving idempotency deduplication and log recovery for coordinator failover.
- Implemented **epoch-tagged gRPC messaging** and 2-of-3 quorum writes to prevent split-brain and guarantee zero payment loss during coordinator failover scenarios.
- Deployed **10+ microservices** via Docker Compose with automated fault injection testing and GitHub Actions CI pipeline enforcing < 5s recovery SLAs.

---

### Viva Questions to Prepare

**Q1: Why Bully algorithm over Ring algorithm?**

Ring has lower message complexity (O(n) vs O(n²)) but requires stable ring topology and sequential message passing. For a 3-node cluster where fast failover matters, Bully's slightly higher cost (just 9 messages max at n=3) delivers faster completion. In production at higher scale, we'd graduate to Raft.

---

**Q2: Why a dedicated C4 Payment Log Service instead of embedding state in coordinators?**

Embedding state in C2 couples state management with election logic. Extracting C4 makes failover simple: new leader just calls `C4.GetAllPending()`. C4 also satisfies the faculty's Decoupled Data mandate — no shared database monolith. It's a clean separation of concerns.

---

**Q3: How do you prove the system meets the 5-Component mandate?**

Every payment traverses all 5 server-side containers: C1 validates, C2 schedules, C4 commits state, C3 executes, C5 records metrics. Remove any one container and the payment either fails or has no audit record. That is true Inter-Component Dependency.

---

**Q4: How did you handle the CAP theorem trade-off?**

We chose **CP** (Consistency over Availability). A coordinator that cannot reach quorum refuses new payment writes rather than risk an inconsistent state. Clients get 503 with `retry-after`. The gateway SDK retries automatically when the partition heals.

---

### Final Submission Checklist

| | Item | Requirement |
|---|---|---|
| ☐ | **Faculty Mandate** | 5-Component compliance table in `README.md`. Each service in its own container. No shared DB. No fat clients. |
| ☐ | **Code Quality** | All exported Go functions have doc comments. No global mutable state without mutex. No TODOs in the payment critical path. |
| ☐ | **Testing** | Unit tests per service. Integration tests for all 5 failure scenarios. Code coverage > 60%. |
| ☐ | **Documentation** | `README.md`: architecture diagram, quick-start (`docker compose up`), how to trigger failures, how to run tests. |
| ☐ | **Demo Video** | 5-min recording: normal operation → kill coordinator → election in dashboard → payments resume → kill worker → reassignment. |
| ☐ | **Report** | Algorithm choices + justification, CAP trade-off, consistency model, performance numbers, lessons learned. |
| ☐ | **Docker** | `docker compose up --build` starts all 10+ containers cleanly on a fresh machine in < 2 minutes. |

---

*PayFlow — Project Plan · Page 23 of 23*
