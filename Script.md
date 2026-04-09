# PayFlow Faculty Presentation Script (Current Progress Only)

This script is written for your current project state.
It explains only what is implemented and tested right now.

---

## 0) Opening (30-45 seconds)

Good morning/afternoon sir/ma'am.
Our project is PayFlow, a fault-tolerant distributed payment processing system.
It is planned as a 5-component distributed architecture.
Today I will explain three things:

1. Full architecture in simple language.
2. My work as Member 5.
3. What is completed and demo-ready right now.

Important note: I will clearly separate implemented features from planned future features.

---

## 1) Architecture From Scratch (Simple, Full)

### 1.1 What problem this project solves

In real payment systems, failures happen at any time:
- service crash
- network delay
- partial downtime

So we designed PayFlow as multiple independent services instead of one big monolith.
This helps us test reliability, monitoring, and recovery.

### 1.2 Logical architecture (5 components)

PayFlow is designed with these logical components:

- C1: API Gateway
  - receives client requests
  - exposes HTTP endpoints

- C2: Coordinator cluster
  - intended to do leader election and scheduling

- C3: Worker services
  - intended to execute payment tasks

- C4: Payment Log service
  - intended to store payment state and idempotency data

- C5: Monitoring service
  - scrapes metrics
  - serves dashboard and live status

### 1.3 Deployment architecture

We run everything with Docker Compose.
The stack currently runs as 10+ containers, including:

- api-gateway
- coordinator-1, coordinator-2, coordinator-3
- worker-1 to worker-5
- payment-log
- monitor
- plus support containers like mock-bank and jaeger

Each service runs in its own container, with health checks and restart policy.

### 1.4 Current runtime reality (very important)

At the current stage:
- C1, C2, C3, and C4 are still placeholder implementations.
- C5 monitoring and chaos tooling are the most developed implemented parts.

So the architecture is complete at container/infrastructure level,
but full business logic for payment lifecycle in C1-C4 is not complete yet.

### 1.5 Data and observability flow currently working

What works now in live environment:

- monitor scrapes metrics from gateway/coordinators/workers/payment-log
- monitor exposes:
  - dashboard on port 3000
  - metrics on port 9091
  - state API at /api/state
  - websocket stream at /ws

This gives real-time visibility of reachability and health across the cluster.

---

## 2) My Part (Member 5) - Full Work Done Till Now

### 2.1 Infrastructure and orchestration ownership

I prepared and maintained the compose-based infra for multi-container local deployment:

- service orchestration for all components
- health checks and restart behavior
- network and volume setup for the cluster
- reproducible run from clean start

I also validated and fixed runtime issues during integration runs.

### 2.2 Monitoring service (C5) implementation

I worked on the C5 monitor service, including:

- config-driven startup
- Prometheus scrape pipeline
- state aggregation logic
- dashboard backend
- websocket updates
- health endpoint
- API state endpoint
- monitor metrics endpoint

Observable proof available now:

- monitor health endpoint returns 200
- monitor metrics include C5-specific metrics such as:
  - payflow_monitor_scrape_duration_seconds
  - payflow_monitor_target_up
- /api/state shows coordinators, workers, gateway, payment-log reachability

### 2.3 Dashboard and real-time observability

I prepared dashboard behavior for:

- coordinator ring visibility
- worker status cards
- queue and throughput panels
- stale/live status indicator based on scrape freshness

Because C1-C4 are placeholders, some panels show limited values,
which is expected at current stage.

### 2.4 Chaos engineering tooling (fault injection)

I implemented and tested chaos CLI flows:

- kill coordinator
- kill worker
- kill payment-log
- delay network
- partition nodes

Safety model implemented and demonstrated:

- dry-run is default
- live fault requires explicit --dry-run=false

I validated both dry-run and live operations.

### 2.5 Test and verification work

I created and refined a Windows PowerShell 5.1-friendly test guide and validation workflow.

Done and verified:

- focused integration tests for monitoring/dashboard/connectivity pass
- monitor scaling unit tests pass
- chaos tool build and command flows pass
- websocket validation works in PowerShell 5.1

Also clarified expected failures:

- full payment-flow integration tests fail in placeholder mode for C1-C4
- this is not hidden; it is explicitly documented and demonstrated

### 2.6 Practical debugging/fixes completed during execution

During live runs, I diagnosed and fixed project-level issues in my scope/integration layer, including:

- monitor startup route conflict crash
- worker port binding conflict in compose setup
- PowerShell command compatibility issues for testing
- integration assertions aligned with actual monitor websocket behavior

---

## 3) What I Can Show Faculty Right Now (Completed Evidence)

This is the exact scope I can confidently demonstrate now.

### 3.1 Live stack and health proof

Show:
- docker compose ps
- service health status for monitor and others

Explain:
- multi-container distributed setup is running
- monitor remains stable while scraping all targets

### 3.2 Monitoring proof

Show:
- browser dashboard at localhost:3000
- API snapshot from /api/state
- monitor metrics from :9091/metrics

Explain:
- reachability is visible per component
- fault status changes are visible quickly
- monitor service provides observability layer for the cluster

### 3.3 Chaos testing proof

Show in order:

1. dry-run kill and partition commands
2. one or more live kill commands with --dry-run=false
3. monitor detects unreachable services
4. recovery by restarting killed services
5. monitor shows recovered reachability

Explain:
- this validates fault-injection and observability loop
- we can simulate realistic distributed failures safely

### 3.4 Testing proof

Show:
- monitor unit tests passing
- focused integration tests related to C5 passing

Explain clearly:
- full payment flow tests are expected to fail until C1-C4 are fully implemented
- this is a known project stage limitation, not hidden technical debt

### 3.5 Command Demo Script (What I will run, what it will show)

I will present this exactly in front of faculty:

1. Cluster status

```powershell
Set-Location $repo
docker compose ps
```

Expected output I will explain:
- most containers in `Up` and `healthy`
- monitor container is stable

2. Monitor health endpoint

```powershell
(Invoke-WebRequest -UseBasicParsing "http://localhost:3000/health").StatusCode
```

Expected output I will explain:
- `200`

3. Monitor metrics endpoint

```powershell
(Invoke-WebRequest -UseBasicParsing "http://localhost:9091/metrics").Content | Select-String "payflow_monitor_scrape_duration_seconds|payflow_monitor_target_up"
```

Expected output I will explain:
- metrics names are visible
- target-up lines show which services are reachable

4. Dashboard/API state proof

```powershell
Invoke-RestMethod "http://localhost:3000/api/state" | ConvertTo-Json -Depth 6
```

Expected output I will explain:
- coordinators/workers list appears
- each node has reachable true/false and last_seen

5. Dry-run chaos proof (safe)

```powershell
Set-Location "$repo\payflow\chaos"
& "$repo\bin\chaos.exe" kill coordinator 1
& "$repo\bin\chaos.exe" delay-network coordinator-1 --ms 200
```

Expected output I will explain:
- output contains `[DRY-RUN]`
- no real container is stopped

6. Live chaos proof (real fault)

```powershell
& "$repo\bin\chaos.exe" kill coordinator 1 --dry-run=false
docker compose ps
Invoke-RestMethod "http://localhost:3000/api/state" | ConvertTo-Json -Depth 6
```

Expected output I will explain:
- killed target may be missing/stopped in compose list
- monitor shows unreachable/dead status for that node

7. Recovery proof

```powershell
Set-Location $repo
docker compose up -d coordinator-1
docker compose ps
Invoke-RestMethod "http://localhost:3000/api/state" | ConvertTo-Json -Depth 6
```

Expected output I will explain:
- restarted service comes back to `Up`
- monitor shows it reachable again

8. Unit test proof (C5 monitor)

```powershell
Set-Location "$repo\payflow\monitor"
go test ./... -v -count=1
```

Expected output I will explain:
- monitor scaling tests pass
- `[no test files]` in some folders is normal and not a failure

9. Focused integration proof (C5 scope)

```powershell
Set-Location "$repo\payflow\tests"
go test ./integration/... -run "TestAllServicesHealthy|TestMonitorWebSocketConnects|TestPrometheusMetricsExposed|TestSnapshotHasThreeCoordinators|TestSnapshotHasFiveWorkers|TestExactlyOneLeader|TestDashboardHTMLContainsExpectedElements" -v -timeout 5m
```

Expected output I will explain:
- these selected monitoring/dashboard tests pass
- full payment flow tests are intentionally not claimed complete yet

### 3.6 Code Walkthrough Script (This code does this)

When faculty asks "what does your code do", I will explain file by file:

1. `payflow/monitor/main.go`
- starts the C5 monitor service
- loads config from environment
- registers Prometheus metrics
- starts scraper + dashboard + optional autoscaler watcher
- serves HTTP routes on port 3000 and metrics on 9091

2. `payflow/monitor/dashboard/server.go`
- registers dashboard routes (`/`, `/ws`, `/api/state`)
- pushes snapshot updates to browser over WebSocket
- sends periodic ping to keep client connections alive
- computes UI data like live workers and throughput display

3. `payflow/monitor/scraper/scraper.go`
- scrapes all configured metrics targets periodically
- parses Prometheus text metrics and builds one cluster snapshot
- marks each node reachable/unreachable based on scrape result
- exposes latest snapshot for API and dashboard

4. `payflow/monitor/scaling/scaler.go`
- contains dynamic scaling logic for queue-threshold based scale-up
- has tested behavior for threshold, cooldown, and max-worker guard
- in current placeholder runtime, real queue signal is usually zero, so runtime scale-up may not trigger

5. `payflow/chaos/cmd/root.go`
- defines chaos CLI root and help text
- enables dry-run by default for safety
- warns before live operations

6. `payflow/chaos/cmd/kill.go`
- implements kill commands for coordinator/worker/payment-log
- supports live and dry-run modes
- used to demonstrate failure and recovery behavior on running cluster

7. `docker-compose.yml`
- defines complete multi-container local cluster
- health checks, restart behavior, network wiring, and ports
- enables repeatable demos from clean startup

8. Placeholder services in `payflow/gateway/main.go`, `payflow/coordinator/main.go`, `payflow/worker/main.go`, `payflow/payment-log/main.go`
- currently return placeholder responses and metrics
- this is why full payment semantics are not yet claimed complete
- still useful for monitoring, infra, and chaos validation at this stage

---

## 4) What Is Not Claimed As Complete Yet

I should explicitly state these are planned but not fully complete now:

- real end-to-end payment lifecycle semantics in C1-C4
- true leader election behavior with full coordinator logic
- production-grade task execution and persistence semantics
- runtime autoscaling proof based on real queue growth from non-placeholder services

This makes the presentation honest and technically correct.

---

## 5) Suggested Faculty Demo Narration (Short Version)

You can speak this directly:

Our project architecture is distributed and containerized with 5 logical components. At this stage, the strongest implemented part is Member 5 scope: monitoring, chaos testing, and infrastructure integration. I can demonstrate live cluster observability, fault injection, and recovery visibility in real time. Focused monitoring-related tests are passing. Full payment-flow semantics are still limited by placeholder implementations in C1-C4, and we are explicitly tracking that as next development work.

---

## 6) Closing (20-30 seconds)

Current status summary:

- Architecture skeleton and deployment are operational.
- Monitoring and fault-testing layer is implemented and demonstrable.
- Verification workflow is complete for current stage.
- Remaining work is mainly full business logic completion in C1-C4.

Thank you.
