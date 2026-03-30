#!/usr/bin/env bash
# PayFlow Integration Day Script — Week 1
# Owner: Member 5 (Monitoring, Testing & Infrastructure)
#
# This script starts the full PayFlow cluster, runs 12 health/connectivity
# checks, and reports a pass/fail summary. Run this on Friday of Week 1
# to verify all 5 services are healthy and communicating.
#
# Usage: bash scripts/integration-day.sh
set -euo pipefail

# ─── Colors ───────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Counters ─────────────────────────────────────────
PASS=0
FAIL=0

# ─── Helper Functions ─────────────────────────────────

# check: evaluates a test result and prints pass/fail
# Usage: check "description" $?
check() {
    local desc="$1"
    local result="$2"
    if [ "$result" -eq 0 ]; then
        echo -e "  ${GREEN}✅ PASS:${NC} $desc"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}❌ FAIL:${NC} $desc"
        FAIL=$((FAIL + 1))
    fi
}

# section: prints a bold section header
section() {
    echo ""
    echo -e "${BOLD}━━━ $1 ━━━${NC}"
    echo ""
}

# ─── STARTUP ──────────────────────────────────────────

section "Starting PayFlow Cluster"
echo "Building and starting all containers..."
docker compose up --build -d

echo ""
echo -e "${YELLOW}Waiting 30s for services to initialize...${NC}"
sleep 30

# ─── HEALTH CHECKS ───────────────────────────────────

section "Running 12 Integration Checks"

# Check 1: Container health count
echo "Check 1: Container health count"
HEALTHY_COUNT=$(docker compose ps 2>/dev/null | grep -c "(healthy)" || echo "0")
if [ "$HEALTHY_COUNT" -ge 10 ]; then
    check "At least 10 containers are healthy ($HEALTHY_COUNT found)" 0
else
    check "At least 10 containers are healthy (only $HEALTHY_COUNT found)" 1
fi

# Check 2: API Gateway health
echo "Check 2: API Gateway /health"
if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    check "API Gateway (C1) /health returns HTTP 200" 0
else
    check "API Gateway (C1) /health returns HTTP 200" 1
fi

# Check 3: Monitor health
echo "Check 3: Monitor /health"
if curl -sf http://localhost:3000/health > /dev/null 2>&1; then
    check "Monitor (C5) /health returns HTTP 200" 0
else
    check "Monitor (C5) /health returns HTTP 200" 1
fi

# Check 4: Prometheus metrics
echo "Check 4: Prometheus /metrics"
if curl -sf http://localhost:9091/metrics > /dev/null 2>&1; then
    check "Prometheus /metrics endpoint returns HTTP 200" 0
else
    check "Prometheus /metrics endpoint returns HTTP 200" 1
fi

# Check 5: Coordinator-1 TCP
echo "Check 5: Coordinator-1 TCP"
if nc -z localhost 50051 2>/dev/null; then
    check "coordinator-1 gRPC port 50051 reachable" 0
else
    check "coordinator-1 gRPC port 50051 reachable" 1
fi

# Check 6: Coordinator-2 TCP
echo "Check 6: Coordinator-2 TCP"
if nc -z localhost 50052 2>/dev/null; then
    check "coordinator-2 gRPC port 50052 reachable" 0
else
    check "coordinator-2 gRPC port 50052 reachable" 1
fi

# Check 7: Coordinator-3 TCP
echo "Check 7: Coordinator-3 TCP"
if nc -z localhost 50053 2>/dev/null; then
    check "coordinator-3 gRPC port 50053 reachable" 0
else
    check "coordinator-3 gRPC port 50053 reachable" 1
fi

# Check 8: Payment-Log TCP
echo "Check 8: Payment-Log TCP"
if nc -z localhost 50054 2>/dev/null; then
    check "payment-log gRPC port 50054 reachable" 0
else
    check "payment-log gRPC port 50054 reachable" 1
fi

# Check 9: Payment submission stub
echo "Check 9: Payment submission (stub)"
PAYMENT_RESPONSE=$(curl -sf -X POST http://localhost:8080/v1/payments \
    -H "Content-Type: application/json" \
    -d '{"amount":100,"currency":"INR","merchant_id":"test"}' 2>&1) || true
if echo "$PAYMENT_RESPONSE" | grep -q "txn_id"; then
    check "POST /v1/payments returns txn_id (stub)" 0
else
    check "POST /v1/payments returns txn_id (stub)" 1
fi

# Check 10: Prometheus scrape duration metric
echo "Check 10: Prometheus scrape_duration metric"
if curl -sf http://localhost:9091/metrics 2>/dev/null | grep -q "payflow_monitor_scrape_duration_seconds"; then
    check "Prometheus has payflow_monitor_scrape_duration_seconds metric" 0
else
    check "Prometheus has payflow_monitor_scrape_duration_seconds metric" 1
fi

# Check 11: Prometheus target_up metric
echo "Check 11: Prometheus target_up metric"
if curl -sf http://localhost:9091/metrics 2>/dev/null | grep -q "payflow_monitor_target_up"; then
    check "Prometheus has payflow_monitor_target_up metric" 0
else
    check "Prometheus has payflow_monitor_target_up metric" 1
fi

# Check 12: WebSocket upgrade
echo "Check 12: WebSocket upgrade"
WS_RESULT=$(timeout 5 curl -sf --include --no-buffer \
    -H "Connection: Upgrade" \
    -H "Upgrade: websocket" \
    -H "Sec-WebSocket-Key: dGVzdA==" \
    -H "Sec-WebSocket-Version: 13" \
    http://localhost:3000/ws 2>&1) || true
if echo "$WS_RESULT" | grep -q "101"; then
    check "WebSocket upgrade returns 101 Switching Protocols" 0
else
    check "WebSocket upgrade returns 101 Switching Protocols" 1
fi

# ─── SUMMARY ─────────────────────────────────────────

section "Integration Day Summary"

TOTAL=$((PASS + FAIL))
echo -e "  ${GREEN}PASS: $PASS${NC} / $TOTAL"
echo -e "  ${RED}FAIL: $FAIL${NC} / $TOTAL"
echo ""

if [ "$FAIL" -gt 0 ]; then
    echo -e "${RED}━━━ SOME CHECKS FAILED ━━━${NC}"
    echo ""
    echo "Debug commands:"
    echo "  docker compose ps                     # check container status"
    echo "  docker compose logs api-gateway       # C1 gateway logs"
    echo "  docker compose logs coordinator-1     # C2 coordinator logs"
    echo "  docker compose logs payment-log       # C4 payment log logs"
    echo "  docker compose logs monitor           # C5 monitor logs"
    echo "  docker compose logs worker-1          # C3 worker logs"
    echo ""
    exit 1
else
    echo -e "${GREEN}━━━ ALL CHECKS PASSED ━━━${NC}"
    echo ""
    echo "🎉 PayFlow Week 1 integration day complete!"
    echo "   All 5 services are running and communicating over payflow-net."
    echo ""
    exit 0
fi
