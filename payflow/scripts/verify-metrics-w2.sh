#!/usr/bin/env bash
# PayFlow Week 2 — Metrics Verification Script
# Owner: Member 5 (Monitoring, Testing & Infrastructure)
#
# Verifies Prometheus /metrics is reachable and correctly instrumented on
# C1 (api-gateway), C2 (coordinators), C3 (workers via docker exec), and
# C5 (monitor). Satisfies the Friday milestone: "Prometheus /metrics
# working on C1, C2, C3."
#
# Usage: bash scripts/verify-metrics-w2.sh
#        EXEC_WORKERS=1 bash scripts/verify-metrics-w2.sh   # also test workers
set -euo pipefail

# ─── Colors ───────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Counters ─────────────────────────────────────────
PASS=0
FAIL=0

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

section() {
    echo ""
    echo -e "${BOLD}━━━ $1 ━━━${NC}"
    echo ""
}

curl_metric() {
    local url="$1"
    local metric="$2"
    if curl -sf "$url" 2>/dev/null | grep -q "^${metric}"; then
        echo 0
    else
        echo 1
    fi
}

# ═══════════════════════════════════════════════════════
# C1 — api-gateway
# ═══════════════════════════════════════════════════════

section "C1 — api-gateway metrics"

# Note: The gateway exposes /metrics on its main HTTP port (8080)
check "C1 /metrics responds HTTP 200" \
    $(curl -sf http://localhost:8080/metrics -o /dev/null 2>/dev/null && echo 0 || echo 1)

check "C1 metrics contain '# HELP' (valid Prometheus format)" \
    $(curl -sf http://localhost:8080/metrics 2>/dev/null | grep -q '^# HELP' && echo 0 || echo 1)

check "C1 metrics contain at least one 'payflow_' metric" \
    $(curl -sf http://localhost:8080/metrics 2>/dev/null | grep -q '^payflow_' && echo 0 || echo 1)

# ═══════════════════════════════════════════════════════
# C2 — coordinator-1 (host port 2112)
# ═══════════════════════════════════════════════════════

section "C2 — coordinator-1 metrics (host port 2112)"

# coordinator-1 maps 2112:2112 to host
check "coordinator-1 /metrics responds HTTP 200" \
    $(curl -sf http://localhost:2112/metrics -o /dev/null 2>/dev/null && echo 0 || echo 1)

check "coordinator-1 exposes payflow_election_count_total" \
    $(curl_metric http://localhost:2112/metrics payflow_election_count_total)

check "coordinator-1 exposes payflow_current_epoch" \
    $(curl_metric http://localhost:2112/metrics payflow_current_epoch)

check "coordinator-1 exposes payflow_is_leader" \
    $(curl_metric http://localhost:2112/metrics payflow_is_leader)

# ═══════════════════════════════════════════════════════
# C2 — coordinator-2 and coordinator-3
# ═══════════════════════════════════════════════════════

section "C2 — coordinator-2 (host port 2153) & coordinator-3 (host port 2154)"

# coordinator-2 maps 2153:2112, coordinator-3 maps 2154:2112
check "coordinator-2 /metrics responds HTTP 200" \
    $(curl -sf http://localhost:2153/metrics -o /dev/null 2>/dev/null && echo 0 || echo 1)

check "coordinator-3 /metrics responds HTTP 200" \
    $(curl -sf http://localhost:2154/metrics -o /dev/null 2>/dev/null && echo 0 || echo 1)

# ═══════════════════════════════════════════════════════
# Exactly one leader check
# ═══════════════════════════════════════════════════════

section "Leader election verification"

LEADER_COUNT=0
for PORT in 2112 2153 2154; do
    VAL=$(curl -sf "http://localhost:$PORT/metrics" 2>/dev/null \
        | grep '^payflow_is_leader ' | awk '{print int($2)}' || echo 0)
    LEADER_COUNT=$((LEADER_COUNT + VAL))
done

check "Exactly 1 coordinator reports payflow_is_leader=1 (got: $LEADER_COUNT)" \
    $([ "$LEADER_COUNT" -eq 1 ] && echo 0 || echo 1)

# ═══════════════════════════════════════════════════════
# C3 — workers
# ═══════════════════════════════════════════════════════

section "C3 — worker metrics"

# Workers run on the internal Docker network only — they do NOT expose
# their metrics port (2112) to the host. To verify worker metrics, we
# must use 'docker compose exec' to reach them from inside the network.
#
# Set EXEC_WORKERS=1 to enable these checks:
#   EXEC_WORKERS=1 bash scripts/verify-metrics-w2.sh
#
# Without EXEC_WORKERS, we print the manual commands for the team.

if [ "${EXEC_WORKERS:-0}" = "1" ]; then
    echo "  Running worker metrics checks via docker compose exec..."
    for W in worker-1 worker-2 worker-3 worker-4 worker-5; do
        RESULT=$(docker compose exec "$W" wget -qO- http://localhost:2112/metrics 2>/dev/null \
            | grep -q payflow_tasks_processed_total && echo 0 || echo 1)
        check "$W exposes payflow_tasks_processed_total" "$RESULT"
    done
else
    echo "  Workers have no published host ports (by design)."
    echo "  To verify manually, run these inside the cluster:"
    echo ""
    for W in worker-1 worker-2 worker-3 worker-4 worker-5; do
        echo "    docker compose exec $W wget -qO- http://localhost:2112/metrics | grep payflow_tasks_processed_total"
    done
    echo ""
    echo "  Or set EXEC_WORKERS=1 to automate:"
    echo "    EXEC_WORKERS=1 bash scripts/verify-metrics-w2.sh"
fi

# ═══════════════════════════════════════════════════════
# C5 — monitor self-metrics
# ═══════════════════════════════════════════════════════

section "C5 — monitor self-metrics"

check "monitor :9091/metrics responds HTTP 200" \
    $(curl -sf http://localhost:9091/metrics -o /dev/null 2>/dev/null && echo 0 || echo 1)

check "monitor metrics contain payflow_monitor_scrape_duration_seconds" \
    $(curl_metric http://localhost:9091/metrics payflow_monitor_scrape_duration_seconds)

check "monitor metrics contain payflow_monitor_target_up" \
    $(curl_metric http://localhost:9091/metrics payflow_monitor_target_up)

check "monitor :3000/ responds HTTP 200 (dashboard HTML served)" \
    $(curl -sf http://localhost:3000/ -o /dev/null 2>/dev/null && echo 0 || echo 1)

check "monitor :3000/ws upgrades to WebSocket (HTTP 101)" \
    $(curl -sf --include \
        -H "Connection: Upgrade" \
        -H "Upgrade: websocket" \
        -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
        -H "Sec-WebSocket-Version: 13" \
        http://localhost:3000/ws 2>&1 | grep -q "101" && echo 0 || echo 1)

# ═══════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════

section "Metrics Verification Summary"

TOTAL=$((PASS + FAIL))
echo -e "  ${GREEN}PASS: $PASS${NC} / $TOTAL"
echo -e "  ${RED}FAIL: $FAIL${NC} / $TOTAL"
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}  ✅ All metrics checks passed — Prometheus /metrics working on C1, C2, C5${NC}"
    exit 0
else
    echo -e "${RED}  ❌ Some checks failed — see above${NC}"
    echo "  Debug: docker compose ps"
    echo "  Debug: docker compose logs <service> | tail -30"
    exit 1
fi
