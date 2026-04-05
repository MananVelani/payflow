# PayFlow Integration Tests

## Prerequisites

- **Docker** (Docker Desktop or Docker Engine)
- **Go 1.22+**
- Running PayFlow cluster: `docker compose up --build -d` (from repo root)

## Running Tests

**Run all integration tests:**

```bash
cd payflow/tests
go test ./integration/... -v -timeout 5m
```

**Run a specific test:**

```bash
go test ./integration/... -run TestAllServicesHealthy -v
go test ./integration/... -run TestMonitorWebSocketConnects -v
```

## Test Descriptions

| Test | What It Validates |
|------|-------------------|
| `TestAllServicesHealthy` | API Gateway and Monitor both respond to `/health` with HTTP 200 and `"status":"ok"` |
| `TestMonitorWebSocketConnects` | Monitor WebSocket endpoint accepts connections and sends heartbeat messages |
| `TestPrometheusMetricsExposed` | Prometheus `/metrics` endpoint exposes C5 scrape duration and target_up metrics |
| `TestPayflowNetworkConnectivity` | All coordinator, payment-log, gateway, and monitor TCP ports are reachable |

## Troubleshooting

If containers don't start or tests fail:

```bash
# Check which containers are running
docker compose ps

# View logs for a specific service
docker compose logs <service>

# Examples:
docker compose logs api-gateway
docker compose logs monitor
docker compose logs coordinator-1

# Restart everything
docker compose down --volumes --remove-orphans
docker compose up --build -d
```

## Important Notes

- Tests assume the Docker Compose stack is **already running**. They do NOT start/stop containers themselves.
- Tests will skip gracefully if Docker is not available.
- Network connectivity tests use `t.Errorf` (not `t.Fatalf`) so all results are reported even if some fail.
