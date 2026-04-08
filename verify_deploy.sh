#!/usr/bin/env bash
# verify_deploy.sh — Smoke-test efter portal-api deploy
# Dynamisk endpoint-lista fran OpenAPI. Testar alla GET-endpoints.
# Anvandning: ./verify_deploy.sh [--verbose]
# Exit 0 = OK, Exit 1 = fail

set -uo pipefail

API_BASE="http://localhost:8090"
TOKEN="daniel-vd-2026"
INTERNAL_TOKEN="rm-internal-2026"
VERBOSE="${1:-}"
TIMEOUT=5

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo "======================================"
echo "  RM Portal API — Deploy Verification"
echo "======================================"
echo ""

# --- Steg 1: Vanta pa att API:t svarar ---
echo -n "Vantar pa att API:t startar..."
for i in $(seq 1 30); do
    STATUS=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$API_BASE/api/health" 2>/dev/null)
    if [ "$STATUS" = "200" ]; then
        echo -e " ${GREEN}OK${NC} (${i}s)"
        break
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then
        echo -e " ${RED}TIMEOUT${NC}"
        exit 1
    fi
done

# --- Steg 2: Hamta GET-endpoints utan path-params ---
OPENAPI=$(curl -s --max-time $TIMEOUT "$API_BASE/openapi.json")
if [ -z "$OPENAPI" ]; then
    echo -e "${RED}FAIL${NC}: /openapi.json otillganglig"
    exit 1
fi

ENDPOINTS=$(echo "$OPENAPI" | python3 -c "
import sys, json
spec = json.load(sys.stdin)
for path in sorted(spec.get('paths', {}).keys()):
    if '{' not in path and 'get' in spec['paths'][path]:
        print(path)
")

TOTAL=$(echo "$ENDPOINTS" | wc -l | tr -d ' ')
echo "Testar $TOTAL GET-endpoints..."
echo ""

# --- Steg 3: Testa ---
PASS=0
FAIL=0
WARN=0
FAILURES=""

# Kanda problem (loggar varning, racker inte fail)
KNOWN_ISSUES="/api/ata/documents"

while IFS= read -r ep; do
    [ -z "$ep" ] && continue

    # Bestam auth
    if [ "$ep" = "/api/morning-summary" ]; then
        HEADER="Authorization: Bearer $INTERNAL_TOKEN"
    else
        HEADER="X-Portal-Token: $TOKEN"
    fi

    STATUS=$(curl -s -o /dev/null -w '%{http_code}' --max-time $TIMEOUT \
        -H "$HEADER" "$API_BASE$ep" 2>/dev/null)

    if [ "$STATUS" -ge 200 ] 2>/dev/null && [ "$STATUS" -lt 400 ] 2>/dev/null; then
        PASS=$((PASS + 1))
        [ -n "$VERBOSE" ] && echo -e "  ${GREEN}$STATUS${NC} $ep"
    else
        # Kolla om det ar ett kant problem
        IS_KNOWN=0
        for ki in $KNOWN_ISSUES; do
            if [ "$ep" = "$ki" ]; then
                IS_KNOWN=1
                break
            fi
        done
        if [ "$IS_KNOWN" -eq 1 ]; then
            WARN=$((WARN + 1))
            [ -n "$VERBOSE" ] && echo -e "  ${YELLOW}$STATUS${NC} $ep (kant problem)"
        else
            FAIL=$((FAIL + 1))
            FAILURES="$FAILURES\n  $STATUS $ep"
            echo -e "  ${RED}$STATUS${NC} $ep"
        fi
    fi

done <<< "$ENDPOINTS"

# --- Steg 4: Rapport ---
echo ""
echo "======================================"
echo -e "  PASS: ${GREEN}$PASS${NC}  FAIL: ${RED}$FAIL${NC}  WARN: ${YELLOW}$WARN${NC}  TOTAL: $TOTAL"
echo "======================================"

if [ "$FAIL" -gt 0 ]; then
    echo -e "\n${RED}NYA fel (inte kanda):${NC}"
    echo -e "$FAILURES"
    echo ""
    exit 1
else
    [ "$WARN" -gt 0 ] && echo -e "\n${YELLOW}Kanda problem kvarstar ($WARN st)${NC}"
    echo -e "\n${GREEN}Deploy OK — inga nya fel.${NC}"
    exit 0
fi
