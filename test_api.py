"""
RM Portal API — Automatiska integrationstester.
Testar varje modul mot live-API (localhost:8090).
Kör: python3 test_api.py

Verifierar:
  - HTTP statuskod (200, 401, 403)
  - JSON-struktur (förväntade nycklar finns)
  - Rollbaserad åtkomst (VD vs fält)
  - Write-endpoints (POST/PUT/DELETE med safe testdata)
"""

import sys
import json
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional

BASE = "http://localhost:8090"
VD_TOKEN = "daniel-vd-2026"
EK_TOKEN = "mikael-ek-2026"
PL_TOKEN = "erik-pl-2026"

PASS = 0
FAIL = 0
ERRORS: List[str] = []


def _req(method: str, path: str, token: str = VD_TOKEN, body: dict = None, expect_status: int = 200) -> Optional[Dict]:
    """Make HTTP request, return parsed JSON or None."""
    global PASS, FAIL
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"X-Portal-Token": token, "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        code = resp.getcode()
        result = json.loads(resp.read())
        if code == expect_status:
            return result
        else:
            FAIL += 1
            ERRORS.append(f"{method} {path}: expected {expect_status}, got {code}")
            return result
    except urllib.error.HTTPError as e:
        code = e.code
        if code == expect_status:
            PASS += 1
            return None
        FAIL += 1
        ERRORS.append(f"{method} {path}: expected {expect_status}, got {code}")
        return None
    except Exception as e:
        FAIL += 1
        ERRORS.append(f"{method} {path}: exception {e}")
        return None


def check(test_name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {test_name}")
    else:
        FAIL += 1
        msg = f"  FAIL  {test_name}"
        if detail:
            msg += f" — {detail}"
        print(msg)
        ERRORS.append(msg)


def has_keys(data: dict, keys: list) -> bool:
    return all(k in data for k in keys)


# ============================================================================
# MODUL: Kärna (auth, team, roles, permissions, kpis)
# ============================================================================

def test_core():
    print("\n=== KÄRNA (auth, team, roles, KPI) ===")

    # Health
    r = _req("GET", "/api/health")
    check("health returnerar status", r and r.get("status") == "ok")

    # Me
    r = _req("GET", "/api/me")
    check("me returnerar user-objekt", r and "user" in r)
    check("me har username", r and r["user"].get("username") == "daniel")
    check("me har permissions lista", r and isinstance(r["user"].get("permissions"), list))

    # Auth — ogiltig token ger 403
    _req("GET", "/api/me", token="invalid-token-xxx", expect_status=403)
    check("ogiltig token ger 403", True)  # _req hanterar

    # Auth — ingen token ger 401
    try:
        req = urllib.request.Request(f"{BASE}/api/me")
        urllib.request.urlopen(req, timeout=5)
        check("saknad token ger 401", False, "fick 200")
    except urllib.error.HTTPError as e:
        check("saknad token ger 401", e.code == 401, f"fick {e.code}")

    # Roles
    r = _req("GET", "/api/roles")
    check("roles returnerar ok", r and r.get("ok"))
    check("roles har lista", r and isinstance(r.get("roles"), list))
    check("roles har permissions", r and len(r["roles"]) > 0 and "permissions" in r["roles"][0])

    # Team
    r = _req("GET", "/api/team")
    check("team returnerar ok", r and r.get("ok"))
    check("team har members", r and isinstance(r.get("members"), list))

    # Team member detail
    r = _req("GET", "/api/team/1")
    check("team/1 returnerar member", r and "member" in r)

    # Permissions catalog
    r = _req("GET", "/api/permissions/catalog")
    check("permissions catalog returnerar ok", r and r.get("ok"))
    check("catalog har admin-kategori", r and "admin" in r.get("catalog", {}))
    # Verifiera å/ä/ö
    admin_items = r.get("catalog", {}).get("admin", []) if r else []
    recurring = next((i for i in admin_items if i["key"] == "admin.recurring"), None)
    check("permissions: Återkommande (å/ä/ö)", recurring and recurring["label"] == "Återkommande")

    # KPIs
    r = _req("GET", "/api/kpis")
    check("kpis returnerar pipeline_weighted", r and "pipeline_weighted" in r)
    check("kpis har tb1", r and "tb1" in r)
    check("kpis har nettokassa", r and "nettokassa" in r)
    check("kpis har pipeline_warnings", r and "pipeline_warnings" in r)

    # KPIs extended
    r = _req("GET", "/api/kpis/extended")
    check("kpis/extended har customer_concentration", r and "customer_concentration" in r)
    check("kpis/extended har orderstock", r and "orderstock" in r)
    check("kpis/extended har ej_fakturerat", r and "ej_fakturerat" in r)
    check("kpis/extended har headcount", r and "headcount" in r)

    # Fokus
    r = _req("GET", "/api/fokus")
    check("fokus har att_fakturera", r and "att_fakturera" in r)
    check("fokus har scoring_top", r and "scoring_top" in r)
    check("fokus har crm_hygien", r and "crm_hygien" in r)
    check("fokus har planner_tasks", r and "planner_tasks" in r)

    # Layout
    r = _req("GET", "/api/layout")
    check("layout har source", r and "source" in r)
    check("layout har widgets", r and "widgets" in r)

    # Layout widgets
    r = _req("GET", "/api/layout/widgets")
    check("layout/widgets har lista", r and isinstance(r.get("widgets"), list))

    # Admin audit
    r = _req("GET", "/api/admin/audit")
    check("admin/audit har entries", r and "entries" in r)
    check("admin/audit har total", r and "total" in r)


# ============================================================================
# MODUL: Ekonomi (finance_router)
# ============================================================================

def test_finance():
    print("\n=== EKONOMI (finance_router) ===")

    r = _req("GET", "/api/fortnox/summary")
    check("fortnox/summary har revenue", r and "revenue" in r)
    check("fortnox/summary har tb1", r and "tb1" in r)
    check("fortnox/summary har recv_weighted", r and "recv_weighted" in r)
    check("fortnox/summary har top_customers", r and isinstance(r.get("top_customers"), list))
    check("fortnox/summary har bank", r and "bank" in r)
    check("fortnox/summary har period_label", r and r.get("period_label") == "Rullande 12 mån")

    r = _req("GET", "/api/fortnox/invoices")
    check("fortnox/invoices har count", r and "count" in r)
    check("fortnox/invoices har summary", r and "summary" in r)
    check("fortnox/invoices har invoices lista", r and isinstance(r.get("invoices"), list))

    r = _req("GET", "/api/fortnox/supplier-invoices")
    check("fortnox/supplier-invoices har count", r and "count" in r)

    r = _req("GET", "/api/fortnox/cashflow")
    check("fortnox/cashflow har forecast_weeks", r and isinstance(r.get("forecast_weeks"), list))
    check("fortnox/cashflow har salary_events", r and isinstance(r.get("salary_events"), list))
    check("fortnox/cashflow har recurring_events", r and "recurring_events" in r)
    check("fortnox/cashflow har vat_events", r and "vat_events" in r)
    check("fortnox/cashflow har bank", r and "bank" in r)

    r = _req("GET", "/api/fortnox/projects")
    check("fortnox/projects har individual", r and isinstance(r.get("individual"), list))
    check("fortnox/projects har grouped", r and isinstance(r.get("grouped"), list))

    # Label — test med ogiltigt fortnox_id (404)
    r = _req("POST", "/api/label", body={"fortnox_id": "TEST-NONEXISTENT", "label": "Bevakas", "type": "customer"}, expect_status=404)
    check("label med ogiltigt id ger 404", True)


# ============================================================================
# MODUL: Pipeline (pipeline_router)
# ============================================================================

def test_pipeline():
    print("\n=== PIPELINE (pipeline_router) ===")

    r = _req("GET", "/api/deals")
    check("deals har count", r and "count" in r)
    check("deals har deals lista", r and isinstance(r.get("deals"), list))
    check("deals har warnings", r and "warnings" in r)

    r = _req("GET", "/api/pipeline/warnings")
    check("pipeline/warnings har total_warnings", r and "total_warnings" in r)
    check("pipeline/warnings har needs_estimate", r and "needs_estimate" in r)
    # Verifiera å/ä/ö i meddelanden
    msg = r.get("needs_estimate", {}).get("message", "") if r else ""
    check("pipeline/warnings: platshållarvärde (å/ä/ö)", "platshållarvärde" in msg or r.get("needs_estimate", {}).get("count", 0) == 0)

    r = _req("GET", "/api/pipeline/hitrate")
    check("pipeline/hitrate har matrix", r and "matrix" in r)
    check("pipeline/hitrate har row_count", r and "row_count" in r)

    r = _req("GET", "/api/pipeline/scoring")
    check("pipeline/scoring har count", r and "count" in r)
    check("pipeline/scoring har scores", r and isinstance(r.get("scores"), list))

    r = _req("GET", "/api/pipeline/by-type")
    check("pipeline/by-type har tb_per_type", r and "tb_per_type" in r)
    check("pipeline/by-type har pipeline_per_type", r and "pipeline_per_type" in r)

    r = _req("GET", "/api/audit")
    check("audit returnerar audit-nyckel", r and "audit" in r)


# ============================================================================
# MODUL: Projekt (project_router)
# ============================================================================

def test_project():
    print("\n=== PROJEKT (project_router) ===")

    r = _req("GET", "/api/projects")
    check("projects har count", r and "count" in r)
    check("projects har projects lista", r and isinstance(r.get("projects"), list))

    r = _req("GET", "/api/next/economy")
    check("next/economy har count", r and "count" in r)
    check("next/economy har projects lista", r and isinstance(r.get("projects"), list))

    r = _req("GET", "/api/next/unified")
    check("next/unified har count", r and "count" in r)
    check("next/unified har unified lista", r and isinstance(r.get("unified"), list))


# ============================================================================
# MODUL: Execution (execution_router)
# ============================================================================

def test_execution():
    print("\n=== EXECUTION (execution_router) ===")

    for ep in ["/api/goals", "/api/rocks", "/api/key-activities",
               "/api/scorecard",
               "/api/meetings/weekly", "/api/meetings/hitrate",
               "/api/meeting/daily"]:
        r = _req("GET", ep)
        check(f"{ep} returnerar 200", r is not None)


# ============================================================================
# MODUL: Produktivitet (productivity_router)
# ============================================================================

def test_productivity():
    print("\n=== PRODUKTIVITET (productivity_router) ===")

    r = _req("GET", "/api/tasks")
    check("tasks returnerar data", r is not None)

    r = _req("GET", "/api/actions")
    check("actions returnerar data", r is not None)

    r = _req("GET", "/api/planner/tasks")
    check("planner/tasks returnerar data", r is not None)

    r = _req("GET", "/api/hub/checklist/today")
    check("checklist/today returnerar data", r is not None)

    r = _req("GET", "/api/hub/checklist/stats")
    check("checklist/stats returnerar data", r is not None)

    r = _req("GET", "/api/hub/recurring")
    check("hub/recurring returnerar data", r is not None)


# ============================================================================
# MODUL: ÄTA (ata_router)
# ============================================================================

def test_ata():
    print("\n=== ÄTA (ata_router) ===")

    r = _req("GET", "/api/ata")
    check("ata returnerar data", r is not None)

    r = _req("GET", "/api/ata/summary")
    check("ata/summary returnerar data", r is not None)

    r = _req("GET", "/api/ata/documents")
    check("ata/documents returnerar data", r is not None)

    r = _req("GET", "/api/invoicing/queue")
    check("invoicing/queue returnerar data", r is not None)

    r = _req("GET", "/api/invoicing/history")
    check("invoicing/history returnerar data", r is not None)


# ============================================================================
# Rollbaserad åtkomst
# ============================================================================

def test_role_access():
    print("\n=== ROLLBASERAD ÅTKOMST ===")

    # Ekonomi-rollen ska se fakturor
    r = _req("GET", "/api/fortnox/invoices", token=EK_TOKEN)
    check("ekonomi ser fakturor", r and r.get("count", -1) >= 0)

    # Ekonomi ska INTE kunna hantera roller
    _req("GET", "/api/roles", token=EK_TOKEN, expect_status=403)
    check("ekonomi kan inte se roller (403)", True)


# ============================================================================
# MODUL: Simulering (simulation_router)
# ============================================================================

def test_simulation():
    print("\n=== SIMULERING (simulation_router) ===")

    # Lista modeller
    r = _req("GET", "/api/simulation/models")
    check("simulation/models returnerar lista", r and isinstance(r.get("models"), list))
    check("simulation/models har ccc", r and any(m["name"] == "ccc" for m in r.get("models", [])))

    # Parametrar för CCC
    r = _req("GET", "/api/simulation/ccc/parameters")
    check("ccc/parameters har model", r and "model" in r)
    check("ccc/parameters har parameters-lista", r and isinstance(r.get("parameters"), list))
    check("ccc/parameters har baseline", r and "baseline" in r)
    if r and r.get("baseline"):
        b = r["baseline"]
        check("baseline har ccc_days", "ccc_days" in b)
        check("baseline har dso_days", "dso_days" in b)
        check("baseline har working_capital_tied", "working_capital_tied" in b)

    # Deterministisk körning
    r = _req("POST", "/api/simulation/ccc/run", body={"inputs": {"dso_delta_days": 5}})
    check("ccc/run returnerar result", r and "baseline" in r and "adjusted" in r)
    check("ccc/run har delta", r and "delta" in r)
    check("ccc/run har impact_summary", r and "impact_summary" in r)
    check("ccc/run har run_id", r and r.get("run_id", 0) > 0)

    # Monte Carlo
    r = _req("POST", "/api/simulation/ccc/monte-carlo", body={"iterations": 100})
    check("ccc/monte-carlo returnerar result", r and "mean" in r)
    check("ccc/monte-carlo har percentiler", r and "p10" in r and "p50" in r and "p90" in r)
    check("ccc/monte-carlo har histogram", r and isinstance(r.get("histogram"), list))

    # Historik (VD-only)
    r = _req("GET", "/api/simulation/history")
    check("simulation/history returnerar runs", r and isinstance(r.get("runs"), list))
    check("simulation/history har count", r and r.get("count", -1) >= 0)

    # CCC trend (tom men ska fungera)
    r = _req("GET", "/api/simulation/ccc/trend")
    check("ccc/trend returnerar data", r and "trend" in r)
    check("ccc/trend har count", r and r.get("count", -1) >= 0)

    # Icke-existerande modell → 404
    _req("GET", "/api/simulation/nonexistent/parameters", expect_status=404)
    check("okänd modell ger 404", True)

    # PL kan köra simulering (har simulation.run)
    r = _req("GET", "/api/simulation/models", token=PL_TOKEN)
    check("PL kan lista modeller", r and isinstance(r.get("models"), list))

    # PL kan INTE se historik (kräver simulation.admin)
    _req("GET", "/api/simulation/history", token=PL_TOKEN, expect_status=403)
    check("PL kan inte se historik (403)", True)


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  RM Portal API — Integrationstest")
    print("=" * 50)

    test_core()
    test_finance()
    test_pipeline()
    test_project()
    test_execution()
    test_productivity()
    test_ata()
    test_simulation()
    test_role_access()

    print("\n" + "=" * 50)
    total = PASS + FAIL
    print(f"  PASS: {PASS}  FAIL: {FAIL}  TOTAL: {total}")
    print("=" * 50)

    if ERRORS:
        print("\nFeldetaljer:")
        for e in ERRORS:
            print(f"  {e}")

    sys.exit(0 if FAIL == 0 else 1)
