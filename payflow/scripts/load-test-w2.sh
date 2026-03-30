#!/usr/bin/env bash
# PayFlow Week 2 — Load Test Script
# Owner: Member 5 (Monitoring, Testing & Infrastructure)
#
# Submits 20 payments through the API Gateway and verifies they are processed
# by the coordinator/worker pipeline and appear in the payment log.
#
# Usage: bash scripts/load-test-w2.sh
set -euo pipefail

# ─── Colors ───────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

# ─── Counters ─────────────────────────────────────────
PASS=0
FAIL=0
SUBMITTED=0
CONFIRMED=0
PROCESSED=0

# ─── Helpers ──────────────────────────────────────────

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

# ═══════════════════════════════════════════════════════
# STEP 1: Pre-flight checks
# ═══════════════════════════════════════════════════════

section "Pre-flight checks"

if ! command -v curl &>/dev/null; then
    echo -e "${RED}ERROR: curl is not installed. Install it first.${NC}"
    exit 1
fi
echo -e "  ${GREEN}✅${NC} curl found"

if ! command -v jq &>/dev/null; then
    echo -e "${RED}ERROR: jq is not installed. Install it first.${NC}"
    exit 1
fi
echo -e "  ${GREEN}✅${NC} jq found"

GW_HEALTH=$(curl -sf http://localhost:8080/health 2>/dev/null || echo '{}')
if echo "$GW_HEALTH" | jq -e '.status == "ok"' >/dev/null 2>&1; then
    check "API Gateway /health is ok" 0
else
    echo -e "${RED}❌ Gateway not healthy. Is docker compose up?${NC}"
    echo "  Response: $GW_HEALTH"
    exit 1
fi

MON_HEALTH=$(curl -sf http://localhost:3000/health 2>/dev/null || echo '{}')
if echo "$MON_HEALTH" | jq -e '.status == "ok"' >/dev/null 2>&1; then
    check "Monitor /health is ok" 0
else
    echo -e "${RED}❌ Monitor not healthy.${NC}"
    echo "  Response: $MON_HEALTH"
    exit 1
fi

# ═══════════════════════════════════════════════════════
# STEP 2: Submit 20 payments
# ═══════════════════════════════════════════════════════

section "Submitting 20 payments"

declare -a TXN_IDS

for i in $(seq 1 20); do
    AMOUNT=$(( RANDOM % 9900 + 100 ))
    IDEM_KEY="load-test-w2-$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "idem-$(date +%s%N)-$i")"

    RESPONSE=$(curl -sf -X POST http://localhost:8080/v1/payments \
        -H "Content-Type: application/json" \
        -d "{\"amount\":$AMOUNT,\"currency\":\"INR\",\"merchant_id\":\"merchant-test-01\",\"idempotency_key\":\"$IDEM_KEY\"}" \
        --max-time 10 2>&1 || echo "CURL_FAILED")

    if [ "$RESPONSE" = "CURL_FAILED" ]; then
        echo -e "  ${RED}❌ Payment $i/20 — request failed${NC}"
        FAIL=$((FAIL+1))
    else
        TXN=$(echo "$RESPONSE" | jq -r '.txn_id // empty' 2>/dev/null)
        if [ -z "$TXN" ]; then
            echo -e "  ${RED}❌ Payment $i/20 — no txn_id in response: $RESPONSE${NC}"
            FAIL=$((FAIL+1))
        else
            echo -e "  ${GREEN}✅ Payment $i/20 → txn_id: $TXN${NC}"
            TXN_IDS+=("$TXN")
            SUBMITTED=$((SUBMITTED+1))
        fi
    fi
done

echo ""
echo -e "  Submitted: ${BOLD}$SUBMITTED/20${NC}"

# ═══════════════════════════════════════════════════════
# STEP 3: Wait for async processing
# ═══════════════════════════════════════════════════════

section "Waiting 10s for async processing"
sleep 10

# ═══════════════════════════════════════════════════════
# STEP 4: Verify all txn_ids appear in log
# ═══════════════════════════════════════════════════════

section "Verifying payments in log"

for TXN in "${TXN_IDS[@]}"; do
    STATUS_RESP=$(curl -sf "http://localhost:8080/v1/payments/$TXN" \
        --max-time 5 2>/dev/null || echo "NOT_FOUND")

    if [ "$STATUS_RESP" = "NOT_FOUND" ]; then
        echo -e "  ${RED}❌ txn $TXN — NOT FOUND in log${NC}"
        FAIL=$((FAIL+1))
    else
        echo -e "  ${GREEN}✅ txn $TXN — found${NC}"
        CONFIRMED=$((CONFIRMED+1))
        STATUS=$(echo "$STATUS_RESP" | jq -r '.status // "unknown"' 2>/dev/null)
        if [ "$STATUS" = "SUCCESS" ] || [ "$STATUS" = "FAILED" ]; then
            PROCESSED=$((PROCESSED+1))
        fi
    fi
done

# ═══════════════════════════════════════════════════════
# STEP 5: Check Prometheus counter
# ═══════════════════════════════════════════════════════

section "Verifying Prometheus task counter"

PROM_VAL=$(curl -sf http://localhost:9091/metrics 2>/dev/null \
    | grep '^payflow_tasks_processed_total' \
    | awk '{print $2}' | head -1 || echo "0")

if [ "${PROM_VAL%.*}" -ge 20 ] 2>/dev/null; then
    check "payflow_tasks_processed_total >= 20 (got: $PROM_VAL)" 0
else
    check "payflow_tasks_processed_total >= 20 (got: $PROM_VAL)" 1
fi

# ═══════════════════════════════════════════════════════
# STEP 6: Check dashboard shows data
# ═══════════════════════════════════════════════════════

section "Verifying dashboard state endpoint"

STATE=$(curl -sf http://localhost:3000/api/state --max-time 5 2>/dev/null || echo "{}")

# Verify valid JSON
if echo "$STATE" | jq -e . >/dev/null 2>&1; then
    check "Dashboard /api/state returns valid JSON" 0
else
    check "Dashboard /api/state returns valid JSON" 1
fi

LEADERS=$(echo "$STATE" | jq '[.coordinators[]? | select(.is_leader==true)] | length' 2>/dev/null || echo "0")
if [ "$LEADERS" -eq 1 ] 2>/dev/null; then
    check "Exactly 1 coordinator is LEADER (got: $LEADERS)" 0
else
    check "Exactly 1 coordinator is LEADER (got: $LEADERS)" 1
fi

# ═══════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════

section "Week 2 Integration Results"

printf "  Payments submitted : %d/20\n" $SUBMITTED
printf "  Confirmed in log   : %d/%d\n" $CONFIRMED $SUBMITTED
printf "  Fully processed    : %d/%d\n" $PROCESSED $SUBMITTED
printf "  CI checks passed   : %d\n" $PASS
printf "  CI checks failed   : %d\n" $FAIL
echo ""

if [ $FAIL -eq 0 ]; then
    echo -e "${GREEN}  RESULT: ✅ PASS — Week 2 integration complete${NC}"
    exit 0
else
    echo -e "${RED}  RESULT: ❌ FAIL — see failures above${NC}"
    echo "  Debug: docker compose logs api-gateway | tail -50"
    echo "  Debug: docker compose logs monitor | tail -20"
    exit 1
fi
