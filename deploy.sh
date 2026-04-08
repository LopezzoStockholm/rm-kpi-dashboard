#!/usr/bin/env bash
# deploy.sh — Bygg, deploya och verifiera portal-api
# Anvandning: ./deploy.sh [service] [--skip-verify]
#
# Exempel:
#   ./deploy.sh                    # Bygger och deployar portal-api (default)
#   ./deploy.sh rm-portal-api      # Samma som ovan
#   ./deploy.sh rm-dashboard-v2    # Bygger och deployar dashboard
#   ./deploy.sh --skip-verify      # Skippa smoke-test

set -uo pipefail

SERVICE="${1:-rm-portal-api}"
SKIP_VERIFY="${2:-}"

# Om forsta arg ar --skip-verify, shifta
if [ "$SERVICE" = "--skip-verify" ]; then
    SERVICE="rm-portal-api"
    SKIP_VERIFY="--skip-verify"
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo -e "${YELLOW}=== RM Deploy: $SERVICE ===${NC}"
echo ""

# --- Steg 1: Build ---
echo -e "1/3 ${YELLOW}Bygger image...${NC}"
docker compose build "$SERVICE"
if [ $? -ne 0 ]; then
    echo -e "${RED}BUILD FAILED${NC}"
    exit 1
fi
echo -e "    ${GREEN}Build OK${NC}"

# --- Steg 2: Deploy ---
echo -e "2/3 ${YELLOW}Startar container...${NC}"
docker compose up -d "$SERVICE"
if [ $? -ne 0 ]; then
    echo -e "${RED}DEPLOY FAILED${NC}"
    exit 1
fi
echo -e "    ${GREEN}Container startad${NC}"

# --- Steg 3: Verify ---
if [ "$SKIP_VERIFY" = "--skip-verify" ]; then
    echo -e "3/3 ${YELLOW}Verifiering skippas${NC}"
else
    echo -e "3/3 ${YELLOW}Kor smoke-test...${NC}"
    sleep 3  # Vanta pa att API:t startar
    if [ -f "$SCRIPT_DIR/verify_deploy.sh" ]; then
        "$SCRIPT_DIR/verify_deploy.sh"
        if [ $? -ne 0 ]; then
            echo ""
            echo -e "${RED}VARNING: Smoke-test misslyckades!${NC}"
            echo -e "${RED}Overlag rollback: docker compose up -d --force-recreate $SERVICE${NC}"
            exit 1
        fi
    else
        echo -e "    ${YELLOW}verify_deploy.sh saknas, skippar${NC}"
    fi
fi

# --- Steg 4: Endpoint-regression ---
REGISTRY="$SCRIPT_DIR/endpoint_registry.txt"
if [ -f "$REGISTRY" ]; then
    echo -e "4/4 ${YELLOW}Kontrollerar endpoint-registret...${NC}"
    REG_FAIL=0
    REG_TOTAL=0
    TOKEN="daniel-vd-2026"
    while IFS= read -r ep; do
        [[ "$ep" =~ ^#.*$ || -z "$ep" ]] && continue
        [[ "$ep" == *"{"* ]] && continue
        REG_TOTAL=$((REG_TOTAL + 1))
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "X-Portal-Token: $TOKEN" "http://localhost:8090${ep}" 2>/dev/null)
        if [ "$HTTP_CODE" = "404" ]; then
            echo -e "  ${RED}MISSING${NC}: $ep (404)"
            REG_FAIL=$((REG_FAIL + 1))
        fi
    done < "$REGISTRY"

    if [ $REG_FAIL -gt 0 ]; then
        echo ""
        echo -e "${RED}======================================${NC}"
        echo -e "${RED}  REGRESSION: $REG_FAIL endpoints FORSVUNNA!${NC}"
        echo -e "${RED}  Deploy AVBRUTEN. Kontrollera router-filer.${NC}"
        echo -e "${RED}======================================${NC}"
        exit 1
    else
        echo -e "    ${GREEN}$REG_TOTAL registrerade endpoints OK${NC}"
    fi
fi

echo ""
echo -e "${GREEN}Deploy klar: $SERVICE${NC}"
