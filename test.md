# PayFlow Member 5 (C5) Week 1-3 Verification Guide (Windows PowerShell)

This guide does two things:
1. Shows the current audit status of Member 5 work up to Week 3.
2. Gives PowerShell-friendly commands to test everything from scratch.

## 1) Current Audit Status (Based on Repository + Executed Checks)

### Done
- Docker Compose infrastructure is present with 10+ services, healthchecks, named volume, and custom network.
- C5 monitor service exists with:
  - Prometheus scraper
  - WebSocket dashboard
  - `/api/state` endpoint
  - Dynamic autoscaling code path
- Chaos CLI exists with commands for:
  - kill coordinator/worker/payment-log
  - delay-network
  - partition-nodes
- Monitor scaling unit tests pass.
- Chaos CLI builds/tests compile.

### Partial / Blocked
- True Week 2/3 end-to-end behavior cannot be fully confirmed because C1-C4 runtime services are still placeholders in this repo (same placeholder server in gateway/coordinator/worker/payment-log).
- Integration tests were skipped in my run because Docker was not available in this session.
- Full integration tests include payment-flow assertions that can fail with placeholder C1-C4 services.

### Verdict for "done till week 3"
- Member 5 code scaffolding and major implementation pieces are largely present.
- Full Week 3 completion is **not verifiable as complete** yet because dependent services (C1-C4) are placeholders, so several fault-tolerance scenarios cannot be proven end-to-end.

---

## 2) Prerequisites (Windows)

Run in PowerShell (preferably as Administrator for Docker/network operations):

```powershell
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

# PowerShell 5.1: avoid Invoke-WebRequest script parsing prompts
$PSDefaultParameterValues['Invoke-WebRequest:UseBasicParsing'] = $true

# Check Docker
Docker --version
docker compose version

# Check Go
go version
```

Expected:
- Docker Desktop running
- Go installed

---

## 3) Fresh Start Setup

```powershell
$repo = "C:\Users\gandh\Downloads\DC-Project\payflow"
Set-Location $repo

# Clean old stack
docker compose down --volumes --remove-orphans

# Build and start
docker compose up --build -d

# Show status
docker compose ps
```

Expected output pattern:
- Most services show `Up ... (healthy)`.
- `monitor` should NOT be in `Restarting` state.
- `worker-1..5` should NOT be in `Restarting` state.

Optional wait loop until services become healthy/running:

```powershell
$deadline = (Get-Date).AddMinutes(3)
do {
    $ps = docker compose ps
    $healthyCount = ($ps | Select-String -Pattern "healthy").Count
  $upCount = ($ps | Select-String -Pattern " Up ").Count
  $restartingCount = ($ps | Select-String -Pattern "Restarting").Count
  Write-Host "Healthy=$healthyCount Up=$upCount Restarting=$restartingCount"
  if ($healthyCount -ge 10 -and $restartingCount -eq 0) { break }
    Start-Sleep -Seconds 5
} while ((Get-Date) -lt $deadline)

docker compose ps
```

Expected output pattern:
- Final line should look like `Healthy=10+ Up=10+ Restarting=0`.

---

## 4) Week 1 Verification (Member 5 Infrastructure)

### 4.1 Confirm Compose syntax and core topology

```powershell
Set-Location $repo
docker compose config --quiet
```

If no output and no error, config is valid.

Expected output pattern:
- No output, and command exits successfully.

### 4.2 Verify required endpoints

```powershell
# C1 Gateway health
(Invoke-WebRequest -UseBasicParsing "http://localhost:8080/health").StatusCode

# C5 Monitor health
(Invoke-WebRequest -UseBasicParsing "http://localhost:3000/health").StatusCode

# C5 Prometheus endpoint
(Invoke-WebRequest -UseBasicParsing "http://localhost:9091/metrics").StatusCode

# gRPC TCP ports exposed
Test-NetConnection localhost -Port 50051
Test-NetConnection localhost -Port 50052
Test-NetConnection localhost -Port 50053
Test-NetConnection localhost -Port 50054
```

Expected output pattern:
- `StatusCode` for all three `Invoke-WebRequest` checks should be `200`.
- `Test-NetConnection` should show `TcpTestSucceeded : True` for each port.

### 4.3 Verify network and volume objects

```powershell
docker network ls | Select-String "payflow"
docker volume ls | Select-String "payflow-bolt"
```

Expected output pattern:
- A network named similar to `payflow_payflow-net`.
- A volume named similar to `payflow_payflow-bolt`.

---

## 5) Week 2 Verification (Monitoring + Metrics + Dashboard)

### 5.1 Verify C1/C2/C5 metrics endpoints

```powershell
# C1
(Invoke-WebRequest -UseBasicParsing "http://localhost:8080/metrics").Content | Select-String "# HELP|payflow|placeholder"

# C2 (host mapped metrics ports)
(Invoke-WebRequest -UseBasicParsing "http://localhost:2112/metrics").StatusCode
(Invoke-WebRequest -UseBasicParsing "http://localhost:2153/metrics").StatusCode
(Invoke-WebRequest -UseBasicParsing "http://localhost:2154/metrics").StatusCode

# C5
(Invoke-WebRequest -UseBasicParsing "http://localhost:9091/metrics").Content | Select-String "payflow_monitor_scrape_duration_seconds|payflow_monitor_target_up"
```

Expected output pattern:
- C2 endpoint status codes are `200`.
- C5 metrics contain both:
  - `payflow_monitor_scrape_duration_seconds`
  - `payflow_monitor_target_up`

### 5.2 Verify dashboard page and API state

```powershell
# HTML page
(Invoke-WebRequest -UseBasicParsing "http://localhost:3000/").StatusCode

# JSON state
$state = Invoke-RestMethod "http://localhost:3000/api/state"
$state | ConvertTo-Json -Depth 8

"Coordinators: $($state.coordinators.Count)"
"Workers: $($state.workers.Count)"
```

Expected output pattern:
- HTML request returns `200`.
- `Coordinators: 3`
- `Workers: 5` (or more if autoscaling already added workers)
- In placeholder mode, all coordinators may show `FOLLOWER` with epoch `0`.

### 5.3 Verify WebSocket receives monitor messages (snapshot/ping)

PowerShell 5.1 note: use this snippet as-is; it is already compatible and does not require manual assembly loading.

```powershell
$uri = [Uri]"ws://localhost:3000/ws"
$ws = New-Object System.Net.WebSockets.ClientWebSocket
$cts = New-Object System.Threading.CancellationTokenSource
$ws.ConnectAsync($uri, $cts.Token).GetAwaiter().GetResult()

$buffer = New-Object byte[] 4096
$segment = [System.ArraySegment[byte]]::new($buffer, 0, $buffer.Length)
$result = $ws.ReceiveAsync($segment, $cts.Token).GetAwaiter().GetResult()
$msg = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count)
$msg

$ws.Dispose()
$cts.Dispose()
```

Expected message type should be `snapshot` or `ping` in the JSON.

### 5.4 Run integration tests

Member 5 focused tests (recommended first):

```powershell
Set-Location "$repo\payflow\tests"
go test ./integration/... -run "TestAllServicesHealthy|TestMonitorWebSocketConnects|TestPrometheusMetricsExposed|TestSnapshotHasThreeCoordinators|TestSnapshotHasFiveWorkers|TestExactlyOneLeader|TestDashboardHTMLContainsExpectedElements" -v -timeout 5m
```

Expected output pattern:
- Selected tests should show `--- PASS`.
- Final summary should include `PASS` and `ok github.com/payflow/tests/integration`.

Run full integration suite:

```powershell
Set-Location "$repo\payflow\tests"
go test ./integration/... -v -timeout 5m
```

Notes:
- If Docker is not running, tests are skipped by design.
- Full suite may fail while C1-C4 remain placeholder services.

---

## 6) Week 3 Verification (Chaos CLI + Dashboard Rich View + Autoscaling)

## 6.1 Build chaos CLI

```powershell
Set-Location "$repo\payflow\chaos"
go build -o "$repo\bin\chaos.exe" .

# Help
& "$repo\bin\chaos.exe" --help
```

Expected output pattern:
- Help includes subcommands like `kill`, `delay-network`, `partition-nodes`.

### 6.2 Dry-run fault commands (safe)

```powershell
& "$repo\bin\chaos.exe" kill coordinator 1
& "$repo\bin\chaos.exe" kill worker 3
& "$repo\bin\chaos.exe" delay-network coordinator-1 --ms 200
& "$repo\bin\chaos.exe" partition-nodes coordinator-1,coordinator-2
```

By default these are dry-run and should not kill anything.

Expected output pattern:
- Logs include `[DRY-RUN]`.
- Commands print planned actions and complete without container restarts.

### 6.3 Live fault commands (real)

Use only when cluster is running and you want real fault injection:

PowerShell 5.1 note: keep the leading `&` before `"$repo\bin\chaos.exe"`; without `&`, PowerShell treats `kill` as invalid syntax and throws `Unexpected token 'kill'`.

```powershell
# Kill one coordinator
& "$repo\bin\chaos.exe" kill coordinator 1 --dry-run=false

# Kill one worker
& "$repo\bin\chaos.exe" kill worker 2 --dry-run=false

# Kill payment-log
& "$repo\bin\chaos.exe" kill payment-log --dry-run=false
```

Observe recovery:

```powershell
docker compose ps
docker compose logs monitor --tail 100
Invoke-RestMethod "http://localhost:3000/api/state" | ConvertTo-Json -Depth 8
```

Expected output pattern:
- Killed targets become unreachable in monitor state (for example `reachable: false`, `state: DEAD`) and may disappear from `docker compose ps` while stopped.
- `monitor` should remain `Up` and not crash-loop.

Bring killed services back after live-fault tests:

```powershell
Set-Location $repo
docker compose up -d coordinator-1 worker-2 payment-log
docker compose ps
Invoke-RestMethod "http://localhost:3000/api/state" | ConvertTo-Json -Depth 8
```

Recovery expected output pattern:
- `docker compose ps` shows the restarted services in `Up` state.
- Monitor state shows those services as reachable again.

### 6.4 Dashboard rich UI checks (manual)

Open in browser:

```powershell
Start-Process "http://localhost:3000/"
```

Check these panels visually:
- Coordinator Ring
- Worker Health
- Queue Depth
- Throughput sparkline
- stale/live connection badge

### 6.5 Autoscaling check

First run monitor unit tests (already passing in audit):

```powershell
Set-Location "$repo\payflow\monitor"
go test ./... -v
```

Expected output pattern:
- `scaling` tests pass and final output includes `PASS`.

Then attempt runtime autoscaling check:

```powershell
Set-Location $repo

# Baseline workers
$before = (docker compose ps --services | Select-String "worker-").Count
"Workers before: $before"

# Try generating load (depends on non-placeholder worker/coordinator behavior)
for ($i=1; $i -le 80; $i++) {
    $body = @{ amount = 100 + $i; currency = "INR"; merchant_id = "m1"; idempotency_key = "ps-load-$i-$(Get-Date -Format yyyyMMddHHmmssfff)" } | ConvertTo-Json
  Invoke-WebRequest -UseBasicParsing "http://localhost:8080/v1/payments" -Method POST -ContentType "application/json" -Body $body | Out-Null
}

Start-Sleep -Seconds 45
$after = (docker compose ps --services | Select-String "worker-").Count
"Workers after: $after"
```

Expected for full Week 3: `after > before` when queue is above threshold.

Current repo caveat: with placeholder C1-C4 logic, queue depth metrics may not reflect real backlog, so autoscale trigger may not occur in live run.
In placeholder mode, seeing `Workers after: 5` (same as before) is expected and is not an error.

---

## 7) Quick Pass/Fail Checklist

Mark each after running:

- [ ] Compose validates (`docker compose config --quiet`)
- [ ] Core services are up (`docker compose ps`)
- [ ] Monitor health and metrics endpoints work
- [ ] Dashboard HTML and `/api/state` work
- [ ] WebSocket emits snapshot/ping
- [ ] Chaos CLI builds and dry-run commands work
- [ ] Chaos live kill commands work safely
- [ ] Monitor scaling unit tests pass
- [ ] Runtime autoscaling verified under load

---

## 8) Cleanup

```powershell
Set-Location $repo
docker compose down --volumes --remove-orphans
```

This returns your machine to a clean state.
