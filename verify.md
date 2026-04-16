# PayFlow Member 5 Verification Guide (Windows PowerShell 5.1)

This guide verifies Member 5 scope:
- C5 monitor service (health, metrics, dashboard state, websocket)
- Fault injection CLI (kill, delay-network, partition-nodes)
- Docker compose infrastructure behavior
- Integration checks for monitoring/infrastructure paths

## 0) Open project root

```powershell
Set-Location "c:\Users\gandh\Downloads\DC-Project\payflow"
```

Expected output:
- No output (path changes successfully)

## 1) Build and start the stack

```powershell
docker compose up --build -d
```

Expected output (tail):
- `Image payflow-... Built`
- `Container payflow-... Healthy` or `Started`

## 2) Verify container status

```powershell
docker compose ps
```

Expected output:
- `api-gateway`, `coordinator-1`, `coordinator-2`, `coordinator-3`, `payment-log`, `monitor`, `worker-1..5`, `jaeger`
- Status should be `Up` (and mostly `healthy`)

## 3) Verify C5 monitor endpoints

```powershell
(Invoke-WebRequest "http://localhost:3000/health").Content
(Invoke-WebRequest "http://localhost:3000/api/state").StatusCode
(Invoke-WebRequest "http://localhost:9091/metrics").Content | Select-String "payflow_monitor_scrape_duration_seconds|payflow_monitor_target_up"
```

Expected output:
- Health JSON contains `"status":"ok"`
- API state status code is `200`
- Metrics output includes both `payflow_monitor_scrape_duration_seconds` and `payflow_monitor_target_up`

## 4) Verify dashboard WebSocket emits data

```powershell
$ws = New-Object System.Net.WebSockets.ClientWebSocket
$uri = [Uri]"ws://localhost:3000/ws"
$ws.ConnectAsync($uri, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
$buffer = New-Object byte[] 4096
$segment = New-Object System.ArraySegment[byte] -ArgumentList @(,$buffer)
$result = $ws.ReceiveAsync($segment, [Threading.CancellationToken]::None).GetAwaiter().GetResult()
$msg = [Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count)
$msg
$ws.Dispose()
```

Expected output:
- JSON payload containing `"type":"snapshot"` or `"type":"ping"`

## 5) Verify chaos CLI compiles

```powershell
Set-Location ".\payflow\chaos"
go test ./...
Set-Location "..\.."
```

Expected output:
- `? github.com/payflow/chaos ... [no test files]`
- No build failures

## 6) Fault injection verification

### 6.1 Kill worker (live)

```powershell
Set-Location ".\payflow\chaos"
go run . kill worker 3 --dry-run=false
Set-Location "..\.."
docker ps -a --filter name=worker-3 --format "table {{.Names}}`t{{.Status}}"
```

Expected output:
- Chaos CLI prints `Killed container ...`
- Worker-3 status becomes `Exited (137)`

Recover worker-3:

```powershell
docker compose up -d worker-3
```

Expected output:
- `Container payflow-worker-3-1 Started`

### 6.2 Inject and remove delay (live)

```powershell
Set-Location ".\payflow\chaos"
go run . delay-network worker-1 --ms 200 --dry-run=false
go run . delay-network worker-1 --ms 0 --dry-run=false
Set-Location "..\.."
```

Expected output:
- `Added 200ms latency to worker-1`
- `Removed network delay from worker-1`

### 6.3 Partition nodes (live)

```powershell
Set-Location ".\payflow\chaos"
go run . partition-nodes worker-4,worker-5 --dry-run=false
Set-Location "..\.."
```

Expected output:
- `Partition applied`
- `Selected nodes were moved to an isolated partition network`

Recover partitioned workers:

```powershell
docker compose up -d --force-recreate worker-4 worker-5
```

Expected output:
- `Container payflow-worker-4-1 Started`
- `Container payflow-worker-5-1 Started`

## 7) Run Member 5 integration test set (monitor/infra focused)

```powershell
Set-Location ".\payflow\tests"
New-Item -ItemType Directory -Force -Path .gotmp | Out-Null
$env:GOTMPDIR = (Resolve-Path .gotmp).Path
go test ./integration -run "TestAllServicesHealthy|TestMonitorWebSocketConnects|TestPrometheusMetricsExposed|TestPayflowNetworkConnectivity|TestWebSocketReceivesSnapshotType|TestSnapshotHasThreeCoordinators|TestSnapshotHasFiveWorkers|TestExactlyOneLeader|TestDashboardHTMLContainsExpectedElements" -v
Set-Location "..\.."
```

Expected output:
- Each listed test shows `--- PASS:`
- Final line: `PASS`

## 8) Optional: full integration suite snapshot

```powershell
Set-Location ".\payflow\tests"
$env:GOTMPDIR = (Resolve-Path .gotmp).Path
go test ./integration/... -v -timeout 8m
Set-Location "..\.."
```

Expected output right now:
- Monitor/network/dashboard tests pass
- Payment flow tests can fail until Member 1-4 business logic fully converges (leader routing, exactly-once flow, idempotency path)

## 9) Local CI parity commands

```powershell
Set-Location ".\payflow\monitor"
go test ./...
Set-Location "..\chaos"
go test ./...
Set-Location "..\.."
docker compose config --quiet
```

Expected output:
- Go tests complete without build errors
- `docker compose config --quiet` returns with no error output

## 10) Cleanup

```powershell
docker compose down --volumes --remove-orphans
```

Expected output:
- Containers, network, and volume removed
