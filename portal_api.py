

import os
import io
import csv
import json
import subprocess
from datetime import datetime, date, timedelta
import logging
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Databasmodul — ersatter run_psql/docker exec
from rm_data import query_dicts, query_one, execute, get_conn


# ============================================================================
# Configuration
# ============================================================================

# Users are loaded from rm_central.portal_user at auth time
_user_cache: Dict[str, Dict[str, Any]] = {}
_user_cache_ts: float = 0

# REMOVED: DB_CONTAINER = "rm-postgres"  # Migrated to rm_data
# REMOVED: DB_USER = "rmadmin"  # Migrated to rm_data

TWENTY_DB = "twenty"
TWENTY_SCHEMA = "workspace_13e0qz9uia3v9w5dx0mk6etm5"

RM_CENTRAL_DB = "rm_central"
# Permission dependencies: granting key auto-includes deps
PERM_DEPS = {
    'ata.approve': ['ata.view_all'],
    'ata.create': ['ata.view_all'],
    'tasks.assign': ['tasks.read_all'],
    'tasks.delete': ['tasks.read_all', 'tasks.assign'],
    'deals.edit': ['deals.view_all'],
    'deals.delete': ['deals.view_all', 'deals.edit'],
    'kpi.edit': ['kpi.view'],
    'checklist.manage': ['checklist.view'],
    'ekonomi.write': ['ekonomi.read'],
    'projekt.edit': ['projekt.view'],
    'admin.config': ['admin.view'],
    'roles.manage': ['admin.view'],
    'users.manage': ['admin.view'],
}

def enforce_perm_deps(perms):
    """Auto-add dependency permissions."""
    result = set(perms)
    for p in list(result):
        for dep in PERM_DEPS.get(p, []):
            result.add(dep)
    return sorted(result)


STATIC_DIR = "/opt/rm-infra/portal"

app = FastAPI(title="RM Portal API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ÄTA approval flow (token-baserat kundgodkännande)
from ata_approval import router as ata_approval_router
app.include_router(ata_approval_router)




# ============================================================================
# Pydantic Models
# ============================================================================



# ============================================================================
# Database Helper
# ============================================================================

# --- run_psql REMOVED — all calls migrated to rm_data.query_dicts/execute ---
# Original function preserved as comment for reference.
#     """Execute PostgreSQL query via docker exec, return rows as dicts."""
#     try:
#         cmd = [
#             "docker", "exec", DB_CONTAINER, "psql",
#             "-U", DB_USER,
#             "-d", database,
#             "-c", query,
#             "--csv",
#         ]
#         result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

#         if result.returncode != 0:
#             raise Exception(f"DB error: {result.stderr.strip()}")

#         output = result.stdout.strip()
#         if not output:
#             return []

#         reader = csv.DictReader(io.StringIO(output))
#         rows = []
#         for raw in reader:
#             row = {}
#             for k, v in raw.items():
#                 if v is None or v == "":
#                     row[k] = None
#                 else:
#                     try:
#                         row[k] = int(v)
#                     except ValueError:
#                         try:
#                             row[k] = float(v)
#                         except ValueError:
#                             row[k] = v
#             rows.append(row)
#         return rows

#     except subprocess.TimeoutExpired:
#         raise Exception("Database query timeout")
#     except Exception as e:
#         raise Exception(str(e))


# ============================================================================
# Auth dependency
# ============================================================================

def _load_users() -> Dict[str, Dict[str, Any]]:
    """Load users from portal_user table with permissions from rm_role. Cache for 60s."""
    global _user_cache, _user_cache_ts
    import time
    now = time.time()
    if _user_cache and (now - _user_cache_ts) < 60:
        return _user_cache
    rows = query_dicts("""
        SELECT pu.id, pu.token, pu.username, pu.display_name, pu.role, pu.email, pu.owner_alias,
               pu.twenty_member_id, pu.next_user_id, pu.teams_user_id, pu.planner_email,
               pu.phone, pu.manager_email,
               r.permissions::text as permissions_json, r.display_name as role_display
        FROM portal_user pu
        LEFT JOIN rm_role r ON r.id = pu.role_id
        WHERE pu.active = true
    """)
    cache = {}
    for r in rows:
        import json as _json
        try:
            perms = _json.loads(r.get("permissions_json") or "[]")
        except Exception:
            perms = []
        cache[r["token"]] = {
            "id": r["id"],
            "username": r["username"],
            "name": r["display_name"],
            "role": r["role"],
            "role_display": r.get("role_display", r["role"]),
            "email": r.get("email"),
            "owner_alias": r.get("owner_alias"),
            "twenty_member_id": r.get("twenty_member_id"),
            "next_user_id": r.get("next_user_id"),
            "teams_user_id": r.get("teams_user_id"),
            "planner_email": r.get("planner_email"),
            "phone": r.get("phone"),
            "manager_email": r.get("manager_email"),
            "permissions": perms,
        }
    # Load company-role mappings
    ucr_rows = query_dicts("""
        SELECT ucr.user_id, pu.token, ucr.company_code, ucr.role_id, ucr.is_default, ucr.active,
               c.name as company_name, r2.display_name as company_role_display,
               r2.permissions::text as company_permissions_json
        FROM user_company_role ucr
        JOIN portal_user pu ON pu.id = ucr.user_id
        JOIN company c ON c.code = ucr.company_code
        LEFT JOIN rm_role r2 ON r2.id = ucr.role_id
        WHERE ucr.active = true AND pu.active = true
    """)
    for ucr in ucr_rows:
        token = ucr["token"]
        if token not in cache:
            continue
        if "companies" not in cache[token]:
            cache[token]["companies"] = []
        import json as _json2
        try:
            cperms = _json2.loads(ucr.get("company_permissions_json") or "[]")
        except Exception:
            cperms = []
        cache[token]["companies"].append({
            "code": ucr["company_code"],
            "name": ucr["company_name"],
            "role": ucr.get("company_role_display", ""),
            "is_default": ucr.get("is_default", False),
            "permissions": cperms,
        })
    # Ensure all users have companies key
    for token in cache:
        if "companies" not in cache[token]:
            cache[token]["companies"] = []
    _user_cache = cache
    _user_cache_ts = now
    return cache


def has_perm(user: dict, permission: str) -> bool:
    """Check if user has a specific permission. Supports wildcard prefix matching.
    Examples: has_perm(user, "admin.scaling_up"), has_perm(user, "deals.read_all")
    """
    perms = user.get("permissions", [])
    if permission in perms:
        return True
    # Check wildcard: "admin.*" matches "admin.scaling_up"
    prefix = permission.rsplit(".", 1)[0] + ".*"
    return prefix in perms


def require_perm(user: dict, permission: str):
    """Raise 403 if user lacks the permission."""
    if not has_perm(user, permission):
        raise HTTPException(status_code=403, detail=f"Permission denied: {permission}")


async def get_current_user(request: Request) -> Dict[str, Any]:
    token = request.headers.get("x-portal-token")
    if not token:
        raise HTTPException(status_code=401, detail="Missing X-Portal-Token")
    users = _load_users()
    if token not in users:
        raise HTTPException(status_code=403, detail="Invalid token")
    return users[token]


def get_company_code(request: Request, user: dict) -> str:
    """Resolve active company from X-Company-Code header or user default.
    Returns company_code string (e.g. 'RM'). Validates that user has access.
    """
    header = request.headers.get("x-company-code", "").strip()
    companies = user.get("companies", [])
    allowed = {c["code"] for c in companies}
    if header and header in allowed:
        return header
    # Fall back to default company
    for c in companies:
        if c.get("is_default"):
            return c["code"]
    # Fall back to first company or 'RM'
    if companies:
        return companies[0]["code"]
    return "RM"




def audit_log(actor: str, action: str, target_type: str, target_id: int, target_name: str = "", details: dict = None):
    """Log an admin action to audit trail."""
    details_json = json.dumps(details or {})
    try:
        execute(
            "INSERT INTO admin_audit_log (actor_username, action, target_type, target_id, target_name, details) "
            "VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
            (actor, action, target_type, target_id, target_name, details_json),
            db=RM_CENTRAL_DB
        )
    except Exception as e:
        logging.error(f"audit_log error: {e}")

# ÄTA CRUD, PDF, dokument, fakturering (utbruten modul)
from ata_router import router as ata_crud_router, init_ata_router
init_ata_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(ata_crud_router)

# Scaling Up: Goals, Rocks, Scorecard, Key Activities (strategilager)
from scaling_router import router as scaling_crud_router, init_scaling_router
init_scaling_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(scaling_crud_router)

# Meeting: Weekly Acceleration, Hit-rate, Daily Huddle (operativt lager)
from meeting_router import router as meeting_crud_router, init_meeting_router
init_meeting_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(meeting_crud_router)

# Produktivitet: Tasks, Planner, Actions, Årshjul, Checklista
from productivity_router import router as productivity_crud_router, init_productivity_router
init_productivity_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(productivity_crud_router)

# Task Hub: Kanban board, blockers, waiting-for, recurring
from task_hub_router import router as task_hub_crud_router, init_task_hub_router
init_task_hub_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(task_hub_crud_router)

# Ekonomi: Fortnox summary, fakturor, leverantörsfakturor, kassaflöde, label, projektlönsamhet
from finance_router import router as finance_crud_router, init_finance_router
init_finance_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(finance_crud_router)

# Affär & Projekt: Pipeline, deals, scoring, CRM audit + Projektöversikt, Next economy, unified
from pipeline_project_router import router as pipeline_project_crud_router, init_pipeline_project_router
init_pipeline_project_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(pipeline_project_crud_router)

# Simulering: CCC, Power of One, projektlönsamhet, kapacitet, finansiering
from simulation_router import router as simulation_crud_router, init_simulation_router
init_simulation_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(simulation_crud_router)
# Tidrapportering: CRUD, godkännande, attestering, summering, profession_rate
from time_router import router as time_crud_router, init_time_router
init_time_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
from project_chain_router import router as project_chain_router, init_project_chain_router
init_project_chain_router(get_current_user, has_perm, require_perm, audit_log, get_company_code)
app.include_router(project_chain_router)
app.include_router(time_crud_router)

# ============================================================================
# API endpoints
# ============================================================================

@app.get("/api/health")
async def health_check():
    return {"status": "ok", "ts": datetime.utcnow().isoformat()}


@app.get("/api/me")
async def get_me(user: dict = Depends(get_current_user)):
    """Return current user profile with permissions. Used by frontend for dynamic UI."""
    return {
        "ok": True,
        "user": {
            "username": user["username"],
            "name": user["name"],
            "email": user.get("email"),
            "role": user["role"],
            "role_display": user.get("role_display", user["role"]),
            "permissions": user.get("permissions", []),
            "phone": user.get("phone"),
            "companies": user.get("companies", []),
        },
    }


@app.get("/api/roles")
async def list_roles(user: dict = Depends(get_current_user)):
    """List all roles and their permissions. VD/roles.manage only."""
    require_perm(user, "roles.manage")
    rows = query_dicts("""
        SELECT id, name, display_name, permissions::text as permissions_json, company_code
        FROM rm_role ORDER BY id
    """)
    import json as _json
    for r in rows:
        try:
            r["permissions"] = _json.loads(r.pop("permissions_json", "[]"))
        except Exception:
            r["permissions"] = []
    return {"ok": True, "roles": rows}


@app.get("/api/team")
async def list_team(user: dict = Depends(get_current_user)):
    """List all team members. Accessible to all authenticated users."""
    rows = query_dicts("""
        SELECT pu.id, pu.username, pu.display_name, pu.email, pu.role,
               r.display_name as role_display, pu.phone, pu.manager_email,
               pu.planner_email, pu.active
        FROM portal_user pu
        LEFT JOIN rm_role r ON r.id = pu.role_id
        WHERE pu.active = true
        ORDER BY r.id, pu.display_name
    """)
    return {"ok": True, "members": rows}


# ── Role & Team Management (CRUD) ──────────────────────────────────

@app.put("/api/roles/{role_id}")
async def update_role(role_id: int, request: Request, user: dict = Depends(get_current_user)):
    """Update role name, display_name, or permissions."""
    require_perm(user, "roles.manage")
    import json as _json
    body = await request.json()
    sets, params = [], []
    if "display_name" in body:
        sets.append("display_name = %s")
        params.append(body["display_name"])
    if "permissions" in body:
        enforced = enforce_perm_deps(body["permissions"])
        sets.append("permissions = %s::jsonb")
        params.append(_json.dumps(enforced))
    if not sets:
        raise HTTPException(400, "Nothing to update")
    sets.append("updated_at = now()")
    params.append(role_id)
    sql = f"UPDATE rm_role SET {', '.join(sets)} WHERE id = %s"
    execute(sql, params, db=RM_CENTRAL_DB)
    audit_log(user["username"], "update_role", "role", role_id, "", {"display_name": body.get("display_name"), "permissions": body.get("permissions")})
    # Invalidate user cache
    global _user_cache_ts
    _user_cache_ts = 0
    return {"ok": True}


@app.post("/api/roles")
async def create_role(request: Request, user: dict = Depends(get_current_user)):
    """Create a new role."""
    require_perm(user, "roles.manage")
    import json as _json
    body = await request.json()
    name = body.get("name", "").strip().lower()
    display_name = body.get("display_name", "").strip()
    permissions = body.get("permissions", [])
    company_code = body.get("company_code", "RM")
    if not name or not display_name:
        raise HTTPException(400, "name and display_name required")
    rows = query_dicts(
        "INSERT INTO rm_role (name, display_name, permissions, company_code) VALUES (%s, %s, %s::jsonb, %s) RETURNING id, name, display_name",
        (name, display_name, _json.dumps(permissions), company_code)
    )
    if rows:
        audit_log(user["username"], "create_role", "role", rows[0]["id"], rows[0]["name"], {"display_name": rows[0]["display_name"], "permissions": permissions})
    return {"ok": True, "role": rows[0] if rows else None}


@app.delete("/api/roles/{role_id}")
async def delete_role(role_id: int, user: dict = Depends(get_current_user)):
    """Delete a role (only if no users assigned)."""
    require_perm(user, "roles.manage")
    assigned = query_dicts("SELECT COUNT(*) as cnt FROM portal_user WHERE role_id = %s", (role_id,))
    if assigned and int(assigned[0].get("cnt", 0)) > 0:
        raise HTTPException(400, f"Cannot delete role: {assigned[0]['cnt']} users still assigned")
    # Fetch role name before deletion for audit trail
    role_rows = query_dicts("SELECT name FROM rm_role WHERE id = %s", (role_id,))
    role_name = role_rows[0]["name"] if role_rows else ""
    execute("DELETE FROM rm_role WHERE id = %s", (role_id,), db=RM_CENTRAL_DB)
    audit_log(user["username"], "delete_role", "role", role_id, role_name, {})
    return {"ok": True}


@app.put("/api/team/{user_id}")
async def update_team_member(user_id: int, request: Request, user: dict = Depends(get_current_user)):
    """Update a team member's role, email, phone, etc."""
    require_perm(user, "users.manage")
    body = await request.json()
    sets = []
    params = []
    safe_fields = ["display_name", "email", "phone", "manager_email", "planner_email", "owner_alias", "role_id", "active"]
    for field in safe_fields:
        if field in body:
            val = body[field]
            if val is None:
                sets.append(f"{field} = NULL")
            elif isinstance(val, bool):
                sets.append(f"{field} = %s")
                params.append(val)
            elif isinstance(val, int):
                sets.append(f"{field} = %s")
                params.append(val)
            else:
                sets.append(f"{field} = %s")
                params.append(str(val))
    if not sets:
        raise HTTPException(400, "Nothing to update")
    params.append(user_id)
    sql = f"UPDATE portal_user SET {', '.join(sets)} WHERE id = %s"
    execute(sql, params, db=RM_CENTRAL_DB)
    audit_log(user["username"], "update_user", "user", user_id, "", {"changes": {k: body.get(k) for k in ["display_name", "email", "phone", "role_id"] if k in body}})
    # Invalidate user cache
    _user_cache_ts = 0
    return {"ok": True}


@app.post("/api/team")
async def create_team_member(request: Request, user: dict = Depends(get_current_user)):
    """Create a new team member with auto-generated token."""
    require_perm(user, "users.manage")
    import uuid
    body = await request.json()
    username = body.get("username", "").strip().lower()
    display_name = body.get("display_name", "").strip()
    role_id = body.get("role_id")
    email = body.get("email", "")
    phone = body.get("phone", "")
    if not username or not display_name:
        raise HTTPException(400, "username and display_name required")
    # Auto-generate token
    token = f"{username}-{uuid.uuid4().hex[:8]}"
    # Get role name for legacy role field
    role_name = "falt"
    if role_id:
        role_rows = query_dicts("SELECT name FROM rm_role WHERE id = %s", (role_id,))
        if role_rows:
            role_name = role_rows[0]["name"]
    rows = query_dicts(
        "INSERT INTO portal_user (username, display_name, token, role, role_id, email, phone, active) VALUES (%s, %s, %s, %s, %s, %s, %s, true) RETURNING id, username, display_name, token",
        (username, display_name, token, role_name, role_id, email, phone)
    )
    if rows:
        audit_log(user["username"], "create_user", "user", rows[0]["id"], rows[0]["username"], {"display_name": rows[0]["display_name"], "email": email, "role_id": role_id})
    return {"ok": True, "member": rows[0] if rows else None, "token": token}


@app.get("/api/team/{user_id}")
async def get_team_member(user_id: int, user: dict = Depends(get_current_user)):
    """Get detailed team member info including token (VD only)."""
    require_perm(user, "users.manage")
    rows = query_dicts("""
        SELECT pu.id, pu.username, pu.display_name, pu.email, pu.role, pu.token,
               pu.phone, pu.manager_email, pu.planner_email, pu.owner_alias,
               pu.role_id, pu.active, pu.twenty_member_id, pu.next_user_id,
               pu.teams_user_id,
               r.display_name as role_display, r.name as role_name
        FROM portal_user pu
        LEFT JOIN rm_role r ON r.id = pu.role_id
        WHERE pu.id = %s
    """, (user_id,))
    if not rows:
        raise HTTPException(404, "User not found")
    return {"ok": True, "member": rows[0]}


@app.get("/api/permissions/catalog")
async def get_permissions_catalog(user: dict = Depends(get_current_user)):
    """Return all available permissions grouped by category."""
    require_perm(user, "roles.manage")
    catalog = {
        "admin": [
            {"key": "admin.scaling_up", "label": "Scaling Up", "desc": "Targets, årsmål, rocks"},
            {"key": "admin.recurring", "label": "Återkommande", "desc": "Mallar och schema"},
            {"key": "admin.meeting", "label": "Veckomöte", "desc": "Commitments och åtag"},
            {"key": "admin.hit_rate", "label": "Hit-rate", "desc": "Prestanda och at-risk"},
            {"key": "admin.dashboard_layout", "label": "Dashboard-layout", "desc": "Widgets per roll"},
            {"key": "admin.threshold", "label": "Trösklar", "desc": "Grön/gul/röd-konfig"},
            {"key": "admin.notifications", "label": "Notiseringar", "desc": "Log och kö"},
            {"key": "admin.config", "label": "Systemkonfig", "desc": "Övrig konfiguration"},
        ],
        "deals": [
            {"key": "deals.read_all", "label": "Se alla deals", "desc": "Pipeline, alla affärers data"},
            {"key": "deals.read_own", "label": "Se egna deals", "desc": "Bara tilldelade affärer"},
            {"key": "deals.write", "label": "Skapa/redigera deals", "desc": "CRM-write"},
        ],
        "tasks": [
            {"key": "tasks.read_all", "label": "Se alla uppgifter", "desc": "Hela teamets tasks"},
            {"key": "tasks.read_own", "label": "Se egna uppgifter", "desc": "Bara tilldelade tasks"},
            {"key": "tasks.write", "label": "Skapa/redigera", "desc": "Skapa och stänga tasks"},
            {"key": "tasks.assign", "label": "Tilldela andra", "desc": "Tilldela tasks till teamet"},
        ],
        "checklist": [
            {"key": "checklist.read_all", "label": "Se all checklist", "desc": "Hela teamets checklist"},
            {"key": "checklist.read_own", "label": "Se egen checklist", "desc": "Bara sin egen"},
            {"key": "checklist.write", "label": "Bocka av", "desc": "Markera som klar"},
        ],
        "kpi": [
            {"key": "kpi.read_all", "label": "Se alla KPI:er", "desc": "Alla nyckeltal"},
            {"key": "kpi.read_own", "label": "Se egna KPI:er", "desc": "Bara egna scorecard"},
            {"key": "kpi.write", "label": "Redigera KPI", "desc": "Uppdatera manuella värden"},
        ],
        "ata": [
            {"key": "ata.read_all", "label": "Se alla ATA", "desc": "Alla ändrings- och tilläggsarbeten"},
            {"key": "ata.read_own", "label": "Se egna ATA", "desc": "Bara egna projekt"},
            {"key": "ata.write", "label": "Skapa ATA", "desc": "Registrera nya ATA"},
            {"key": "ata.approve", "label": "Godkänna ATA", "desc": "Godkänna/neka ATA"},
        ],
        "projekt": [
            {"key": "projects.read_all", "label": "Se alla projekt", "desc": "Alla projekt och lönsamhet"},
            {"key": "projects.read_own", "label": "Se egna projekt", "desc": "Bara tilldelade projekt"},
            {"key": "projects.write", "label": "Redigera projekt", "desc": "Uppdatera projektdata"},
        ],
        "ekonomi": [
            {"key": "cashflow.read", "label": "Kassaflöde", "desc": "Se kassaflödesprognos"},
            {"key": "invoices.read_all", "label": "Alla fakturor", "desc": "Kund- och leverantörsfakturor"},
            {"key": "invoices.read_own", "label": "Egna fakturor", "desc": "Bara egna projekt"},
        ],
        "system": [
            {"key": "users.manage", "label": "Hantera team", "desc": "Lägga till/ta bort personer"},
            {"key": "roles.manage", "label": "Hantera roller", "desc": "Skapa och redigera roller"},
        ],
    }
    return {"ok": True, "catalog": catalog}


@app.get("/api/kpis")
async def get_kpis(request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    try:
        pipeline = query_dicts("""
            SELECT COALESCE(SUM(estimated_value * COALESCE(hit_rate, 25) / 100),0)::bigint as pipeline_weighted,
                   COALESCE(SUM(estimated_value),0)::bigint as pipeline_total,
                   COUNT(*) as deal_count
            FROM pipeline_deal WHERE company_code=%s
        """, (company,))

        profit = query_dicts("""
            SELECT COALESCE(SUM(net_revenue),0) as revenue,
                   COALESCE(SUM(supplier_costs),0) as costs
            FROM project_profitability WHERE company_code=%s
        """, (company,))

        recv = query_dicts("""
            SELECT COALESCE(SUM(balance),0) as receivables
            FROM fortnox_invoice WHERE balance > 0 AND company_code=%s
        """, (company,))

        pay = query_dicts("""
            SELECT COALESCE(SUM(balance),0) as payables
            FROM fortnox_supplier_invoice WHERE balance > 0 AND company_code=%s
        """, (company,))

        # Bank balance
        bank = query_dicts("""
            SELECT balance::numeric(14,2) as balance, balance_date::text, updated_at::text
            FROM bank_balance WHERE company_code=%s
        """, (company,))

        # Orderstock (kontrakterat + leverans)
        os_row = query_dicts("""
            SELECT COALESCE(SUM(estimated_value),0) as val
            FROM pipeline_deal
            WHERE company_code=%s AND stage IN ('kontrakterat','leverans')
        """, (company,))

        # Ej fakturerat
        ef = query_dicts("""
            SELECT COALESCE(SUM(earned_revenue_not_invoiced),0) as val
            FROM next_project_economy
            WHERE company_code=%s AND earned_revenue_not_invoiced > 0
        """, (company,))

        # Headcount
        try:
            with open("/opt/rm-infra/rm-config.json") as f:
                cfg = json.load(f)
            headcount = cfg.get("headcount", 5)
        except Exception:
            headcount = 5

        # Pipeline warnings count
        pw = query_dicts("""
            SELECT
                COUNT(*) FILTER (WHERE estimated_value = 1000000) as needs_estimate,
                COUNT(*) FILTER (WHERE estimated_value IS NULL OR estimated_value = 0) as no_value
            FROM pipeline_deal WHERE company_code=%s
        """, (company,))

        # Ej fakturerat arbete (från tidrapporter, ej Next)
        ej_fakt_tid = query_dicts("""
            SELECT COALESCE(SUM(
                CASE WHEN tr.attested AND NOT tr.invoiced
                     THEN tr.total_revenue ELSE 0 END
            ), 0) as val
            FROM time_report tr
            WHERE tr.company_code=%s
        """, (company,))

        # UE-skuld villkorad: UE-kostnad på projekt där kund ej betalat fullt
        # Inkluderar: (a) kundfaktura finns men ej fullt betald, (b) ingen kundfaktura alls
        ue_villkorad = query_dicts("""
            WITH project_paid AS (
                SELECT project_code,
                       COALESCE(SUM(total), 0) as invoiced,
                       COALESCE(SUM(balance), 0) as outstanding
                FROM fortnox_invoice
                WHERE company_code=%s AND status != 'cancelled'
                GROUP BY project_code
            )
            SELECT COALESCE(SUM(tr.total_cost), 0) as val
            FROM time_report tr
            LEFT JOIN project_paid pp ON tr.project_code = pp.project_code
            WHERE tr.company_code=%s
              AND tr.report_type = 'subcontractor'
              AND (pp.project_code IS NULL OR pp.outstanding > 0)
        """, (company, company))

        p = pipeline[0] if pipeline else {}
        pr = profit[0] if profit else {}
        r = recv[0] if recv else {}
        pp = pay[0] if pay else {}
        bk = bank[0] if bank else {}
        os_v = float(os_row[0]["val"]) if os_row and os_row[0].get("val") else 0
        ef_v = float(ef[0]["val"]) if ef and ef[0].get("val") else 0
        pw_d = pw[0] if pw else {}

        revenue = float(pr.get("revenue") or 0)
        costs = float(pr.get("costs") or 0)
        tb1 = revenue - costs
        margin = round((tb1 / revenue * 100), 2) if revenue > 0 else 0
        payables = float(pp.get("payables") or 0)
        bank_balance = float(bk.get("balance") or 0)
        nettokassa = bank_balance - payables

        # Total revenue (all time, for oms/anställd)
        total_rev = query_dicts("""
            SELECT COALESCE(SUM(total),0) as total
            FROM fortnox_invoice
            WHERE company_code=%s AND NOT is_credit AND total > 0
                AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
        """, (company,))
        total_revenue = float(total_rev[0]["total"]) if total_rev and total_rev[0].get("total") else 0
        rev_per_employee = round(total_revenue / headcount) if headcount else 0

        ej_fakt_tid_v = float(ej_fakt_tid[0]["val"]) if ej_fakt_tid and ej_fakt_tid[0].get("val") else 0
        ue_villkorad_v = float(ue_villkorad[0]["val"]) if ue_villkorad and ue_villkorad[0].get("val") else 0

        return {
            "pipeline_weighted": float(p.get("pipeline_weighted") or 0),
            "pipeline_total": float(p.get("pipeline_total") or 0),
            "deal_count": int(p.get("deal_count") or 0),
            "revenue": revenue,
            "costs": costs,
            "tb1": tb1,
            "tb1_margin": margin,
            "receivables": float(r.get("receivables") or 0),
            "payables": payables,
            "bank_balance": bank_balance,
            "bank_balance_date": bk.get("balance_date"),
            "bank_updated_at": bk.get("updated_at"),
            "nettokassa": nettokassa,
            "orderstock": os_v,
            "ej_fakturerat": ef_v,
            "headcount": headcount,
            "total_revenue_12m": total_revenue,
            "revenue_per_employee": rev_per_employee,
            "pipeline_warnings": {
                "needs_estimate": int(pw_d.get("needs_estimate") or 0),
                "no_value": int(pw_d.get("no_value") or 0),
                "total": int(pw_d.get("needs_estimate") or 0) + int(pw_d.get("no_value") or 0),
            },
            "ej_fakturerat_tid": ej_fakt_tid_v,
            "ue_skuld_villkorad": ue_villkorad_v,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




# ============================================================================

# Morning Summary — internal endpoint for scheduled reports
# ============================================================================

INTERNAL_TOKEN = os.environ.get("RM_INTERNAL_TOKEN", "rm-internal-2026")

def _verify_internal(request: Request):
    auth = request.headers.get("authorization", "")
    if auth != f"Bearer {INTERNAL_TOKEN}":
        raise HTTPException(status_code=403, detail="Forbidden")


@app.get("/api/morning-summary")
async def morning_summary(request: Request):
    company = request.headers.get("x-company-code", "RM")
    """Full morning summary: pipeline, financials, audit, stage changes, tasks."""
    _verify_internal(request)
    try:
        data: Dict[str, Any] = {"company": company, "generated": datetime.utcnow().isoformat()}

        # 1. Pipeline overview
        pipeline = query_dicts("""
            SELECT stage,
                   COUNT(*) as count,
                   COALESCE(SUM(estimated_value),0)::bigint as total_value,
                   COALESCE(SUM(calculated_value),0)::bigint as weighted_value
            FROM pipeline_deal WHERE company_code=%s
            GROUP BY stage ORDER BY stage
        """, (company,))
        data["pipeline_by_stage"] = pipeline

        pipeline_total = query_dicts("""
            SELECT COUNT(*) as deal_count,
                   COALESCE(SUM(estimated_value),0)::bigint as pipeline_total,
                   COALESCE(SUM(calculated_value),0)::bigint as pipeline_weighted
            FROM pipeline_deal WHERE company_code=%s
        """, (company,))
        data["pipeline_summary"] = pipeline_total[0] if pipeline_total else {}

        # 2. Top deals (top 10 by value)
        top_deals = query_dicts("""
            SELECT name, customer_name, stage, deal_type,
                   COALESCE(estimated_value,0)::bigint as estimated_value,
                   COALESCE(calculated_value,0)::bigint as weighted_value,
                   COALESCE(lead_source,'') as lead_source,
                   COALESCE(margin,0) as margin
            FROM pipeline_deal WHERE company_code=%s
            ORDER BY estimated_value DESC LIMIT 10
        """, (company,))
        data["top_deals"] = top_deals

        # 3. Financials (Fortnox)
        fin = {}
        try:
            rows = query_dicts("""
                SELECT
                    COALESCE(SUM(CASE WHEN NOT is_credit AND status != 'cancelled' THEN total ELSE 0 END),0)::numeric(14,0) as revenue,
                    COALESCE(SUM(CASE WHEN is_credit AND status != 'cancelled' THEN total ELSE 0 END),0)::numeric(14,0) as credit_notes,
                    COALESCE(SUM(CASE WHEN balance > 0 AND status != 'cancelled' THEN balance ELSE 0 END),0)::numeric(14,0) as receivables,
                    COALESCE(SUM(CASE WHEN balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled' THEN balance ELSE 0 END),0)::numeric(14,0) as overdue_receivables
                FROM fortnox_invoice WHERE company_code=%s
                    AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
            """, (company,))
            if rows:
                fin = rows[0]
                fin["net_revenue"] = float(fin.get("revenue",0)) - abs(float(fin.get("credit_notes",0)))
        except:
            pass

        try:
            sup = query_dicts("""
                SELECT
                    COALESCE(SUM(total),0)::numeric(14,0) as supplier_costs,
                    COALESCE(SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END),0)::numeric(14,0) as payables,
                    COALESCE(SUM(CASE WHEN balance > 0 AND due_date < CURRENT_DATE THEN balance ELSE 0 END),0)::numeric(14,0) as overdue_payables
                FROM fortnox_supplier_invoice WHERE company_code=%s
                    AND status != 'cancelled'
                    AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
            """, (company,))
            if sup:
                fin.update(sup[0])
                net_rev = float(fin.get("net_revenue",0))
                costs = float(fin.get("supplier_costs",0))
                fin["tb1"] = net_rev - costs
                fin["tb1_margin"] = round((fin["tb1"] / net_rev * 100), 1) if net_rev > 0 else 0
        except:
            pass

        # Bank balance
        try:
            bank = query_dicts("""
                SELECT balance::numeric(14,0) as balance, fetched_at::text
                FROM fortnox_bank_balance WHERE company_code=%s
                ORDER BY fetched_at DESC LIMIT 1
            """, (company,))
            fin["bank_balance"] = bank[0] if bank else None
        except:
            pass

        data["financials"] = fin

        # 4. CRM Audit (latest)
        try:
            audit = query_dicts("""
                SELECT result FROM crm_audit ORDER BY run_at DESC LIMIT 1
            """)
            if audit and audit[0].get("result"):
                import json as _json
                data["audit"] = _json.loads(audit[0]["result"]) if isinstance(audit[0]["result"], str) else audit[0]["result"]
            else:
                data["audit"] = None
        except:
            data["audit"] = None

        # 5. Stage changes (last 24h)
        try:
            changes = query_dicts("""
                SELECT deal_name, old_stage, new_stage, changed_at::text
                FROM deal_history
                WHERE field = stage AND changed_at >= NOW() - INTERVAL 24 hours
                ORDER BY changed_at DESC
            """)
            data["stage_changes_24h"] = changes
        except:
            data["stage_changes_24h"] = []

        # 6. Overdue invoices (top 10)
        try:
            overdue = query_dicts("""
                SELECT invoice_number, customer_name, balance::numeric(14,0) as balance,
                       due_date::text, (CURRENT_DATE - due_date) as days_overdue,
                       COALESCE(label,'') as label
                FROM fortnox_invoice
                WHERE company_code=%s AND balance > 0 AND due_date < CURRENT_DATE
                    AND status != 'cancelled'
                ORDER BY balance DESC LIMIT 10
            """, (company,))
            data["overdue_invoices"] = overdue
        except:
            data["overdue_invoices"] = []

        # 7. Active projects (Next)
        try:
            projects = query_dicts("""
                SELECT deal_name, next_project_no, next_status, next_customer,
                       budget_revenue::numeric(14,0), booked_revenue::numeric(14,0),
                       earned_revenue_not_invoiced::numeric(14,0) as not_invoiced,
                       contribution_margin_pct::numeric(6,1) as margin_pct
                FROM crm_next_unified
                WHERE next_status IS NOT NULL AND next_status NOT IN (Avslutat,Avbrutet)
                ORDER BY budget_revenue DESC
            """)
            data["active_projects"] = projects
        except:
            data["active_projects"] = []

        return data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ============================================================================
# Fas 1 — API-paritet: alla datakällor från generate_dashboard.py
# ============================================================================




@app.get("/api/kpis/extended")
async def kpis_extended(request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Extended KPIs: customer concentration, orderstock, ej fakturerat, orderingång, headcount."""
    try:
        data: Dict[str, Any] = {}

        # Customer concentration (top 10)
        cust = query_dicts("""
            SELECT customer_name, round(sum(total))::bigint as revenue, count(*)::int as invoices
            FROM fortnox_invoice
            WHERE company_code=%s AND NOT is_credit AND total > 0
            GROUP BY customer_name ORDER BY sum(total) DESC LIMIT 10
        """, (company,))
        data["customer_concentration"] = cust

        total_rev = query_dicts("""
            SELECT round(sum(total))::bigint as total
            FROM fortnox_invoice
            WHERE company_code=%s AND NOT is_credit AND total > 0
        """, (company,))
        data["total_revenue"] = int(total_rev[0]["total"]) if total_rev and total_rev[0].get("total") else 0

        # Orderstock
        os_row = query_dicts("""
            SELECT round(sum(estimated_value))::bigint as val
            FROM pipeline_deal
            WHERE company_code=%s AND stage IN ('kontrakterat','leverans')
        """, (company,))
        data["orderstock"] = int(os_row[0]["val"]) if os_row and os_row[0].get("val") else 0

        # Ej fakturerat
        ef = query_dicts("""
            SELECT round(sum(COALESCE(earned_revenue_not_invoiced,0)))::bigint as val
            FROM next_project_economy
            WHERE company_code=%s AND earned_revenue_not_invoiced > 0
        """, (company,))
        data["ej_fakturerat"] = int(ef[0]["val"]) if ef and ef[0].get("val") else 0

        # Orderingång per månad (senaste 6)
        oi = query_dicts("""
            SELECT to_char(event_date, 'YYYY-MM') as month, count(*)::int as deals,
                   round(sum(COALESCE(estimated_value,0)))::bigint as value
            FROM deal_history WHERE event_type='WON'
            GROUP BY to_char(event_date, 'YYYY-MM') ORDER BY month DESC LIMIT 6
        """)
        data["orderingang"] = oi

        # Headcount
        try:
            with open("/opt/rm-infra/rm-config.json") as f:
                cfg = json.load(f)
            headcount = cfg.get("headcount", 5)
        except Exception:
            headcount = 5
        data["headcount"] = headcount
        data["revenue_per_employee"] = round(data["total_revenue"] / headcount) if data["total_revenue"] and headcount else 0

        # Pipeline warnings
        placeholder = query_dicts("""
            SELECT name, stage FROM pipeline_deal
            WHERE company_code=%s AND estimated_value = 1000000
        """, (company,))
        no_val = query_dicts("""
            SELECT name, stage FROM pipeline_deal
            WHERE company_code=%s AND (estimated_value IS NULL OR estimated_value = 0)
        """, (company,))
        data["pipeline_warnings"] = {
            "needs_estimate": len(placeholder),
            "no_value": len(no_val),
            "total": len(placeholder) + len(no_val),
            "deals": [{"name": d["name"], "stage": d["stage"], "reason": "placeholder_1msek"} for d in placeholder]
                    + [{"name": d["name"], "stage": d["stage"], "reason": "no_value"} for d in no_val]
        }

        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/fokus")
async def fokus(request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Focus data: att fakturera, scoring top 3, CRM hygiene, planner tasks."""
    try:
        data: Dict[str, Any] = {}

        # Att fakturera — topp 5
        att_fakt = query_dicts("""
            SELECT project_no, project_name,
                   earned_revenue_not_invoiced::numeric(14,0) as ej_fakturerat,
                   project_manager, customer_name, status_name
            FROM next_project_economy
            WHERE company_code=%s AND earned_revenue_not_invoiced > 50000
            ORDER BY earned_revenue_not_invoiced DESC LIMIT 5
        """, (company,))
        data["att_fakturera"] = att_fakt

        # Scoring topp 3 (deals med action)
        score_top = query_dicts("""
            SELECT twenty_id, deal_name, score, action
            FROM deal_score
            WHERE action IS NOT NULL AND action != ''
            ORDER BY score DESC LIMIT 3
        """)
        data["scoring_top"] = score_top

        # CRM Hygien
        hygien: Dict[str, Any] = {}
        hygien["utan_varde"] = query_dicts("""
            SELECT name, twenty_id as id, stage
            FROM pipeline_deal
            WHERE company_code=%s
                AND stage IN ('kontrakterat','leverans','forhandling','offert_skickad')
                AND (estimated_value IS NULL OR estimated_value = 0)
        """, (company,))
        hygien["utan_foretag"] = query_dicts("""
            SELECT name, twenty_id as id, stage
            FROM pipeline_deal
            WHERE company_code=%s
                AND (customer_name IS NULL OR customer_name = '')
                AND stage NOT IN ('fakturerat')
        """, (company,))
        hygien["utan_leadkalla"] = query_dicts("""
            SELECT name, twenty_id as id, stage
            FROM pipeline_deal
            WHERE company_code=%s
                AND (lead_source IS NULL OR lead_source = '')
                AND stage NOT IN ('fakturerat')
        """, (company,))
        data["crm_hygien"] = hygien

        # Planner tasks
        planner = query_dicts("""
            SELECT title, assignee_name, bucket_name, plan_name,
                   percent_complete, due_date::text, priority,
                   CASE
                       WHEN due_date < CURRENT_DATE AND percent_complete < 100 THEN 'overdue'
                       WHEN due_date = CURRENT_DATE THEN 'today'
                       WHEN due_date <= CURRENT_DATE + 7 THEN 'this_week'
                       ELSE 'later'
                   END as urgency
            FROM planner_task
            WHERE percent_complete < 100 AND company_code = %s
            ORDER BY
                CASE WHEN due_date IS NULL THEN 1 ELSE 0 END,
                due_date ASC, priority DESC
        """, (company,))
        data["planner_tasks"] = planner[:10]
        data["planner_overdue"] = len([t for t in planner if t.get("urgency") == "overdue"])
        data["planner_today"] = len([t for t in planner if t.get("urgency") == "today"])
        data["planner_total"] = len(planner)

        # Message-to-task recent
        msg = query_dicts("""
            SELECT source, parsed_title as title, parsed_assignee as assignee,
                   parsed_project as project, confidence, created_at::text
            FROM message_task_log
            WHERE planner_task_id IS NOT NULL AND skipped = false
            ORDER BY created_at DESC LIMIT 5
        """)
        data["msg_task_recent"] = msg

        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




# ============================================================================
# Layout API — config-driven dashboard
# ============================================================================

@app.get("/api/layout")
async def get_layout(request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Get resolved layout for current user. 3-tier cascade: user > role > system default.
    VD can pass ?view_as=projektledare to preview another role's layout."""
    view_as = request.query_params.get("view_as")
    if view_as and has_perm(user, "admin.dashboard_layout") and view_as in ("vd", "ekonomi", "projektledare", "falt"):
        role = view_as
        # Skip personal layout when previewing another role
        username = None
    else:
        role = user["role"]
        username = user["username"]

    # Get user_id
    user_id = None
    if username:
        user_rows = query_dicts("SELECT id FROM portal_user WHERE username=%s", (username,))
        user_id = user_rows[0]["id"] if user_rows else None

    # Tier 1: Personal layout
    personal = []
    if user_id:
        personal = query_dicts("""
            SELECT dl.widget_key, dl.sort_order, dl.visible, dl.config,
                   dw.display_name, dw.component_name, dw.category, dw.requires_data
            FROM dashboard_layout dl
            JOIN dashboard_widget dw ON dw.widget_key = dl.widget_key AND dw.is_active = true
            WHERE dl.company_code=%s AND dl.user_id=%s
            ORDER BY dl.sort_order
        """, (company, user_id))

    if personal:
        return {"source": "personal", "role": role, "widgets": personal}

    # Tier 2: Role-based layout
    role_layout = query_dicts("""
        SELECT dl.widget_key, dl.sort_order, dl.visible, dl.config,
               dw.display_name, dw.component_name, dw.category, dw.requires_data
        FROM dashboard_layout dl
        JOIN dashboard_widget dw ON dw.widget_key = dl.widget_key AND dw.is_active = true
        WHERE dl.company_code=%s AND dl.role=%s AND dl.user_id IS NULL
        ORDER BY dl.sort_order
    """, (company, role))

    if role_layout:
        return {"source": "role", "role": role, "widgets": role_layout}

    # Tier 3: System default (from dashboard_widget.default_roles)
    system_default = query_dicts("""
        SELECT widget_key, default_sort_order as sort_order, true as visible, '{}'::text as config,
               display_name, component_name, category, requires_data
        FROM dashboard_widget
        WHERE is_active = true AND %s = ANY(default_roles)
        ORDER BY default_sort_order
    """, (role,))

    return {"source": "system_default", "role": role, "widgets": system_default}


@app.get("/api/layout/widgets")
async def get_all_widgets(user: dict = Depends(get_current_user)):
    """Get all available widgets (for admin page)."""
    widgets = query_dicts("""
        SELECT widget_key, display_name, component_name, category,
               default_roles, default_sort_order, requires_data, is_active
        FROM dashboard_widget ORDER BY default_sort_order
    """)
    return {"widgets": widgets}


@app.get("/api/layout/role/{role}")
async def get_role_layout(role: str, request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Get layout for a specific role (admin use)."""
    require_perm(user, "admin.dashboard_layout")

    layout = query_dicts("""
        SELECT dl.widget_key, dl.sort_order, dl.visible, dl.config,
               dw.display_name, dw.component_name, dw.category
        FROM dashboard_layout dl
        JOIN dashboard_widget dw ON dw.widget_key = dl.widget_key
        WHERE dl.company_code=%s AND dl.role=%s AND dl.user_id IS NULL
        ORDER BY dl.sort_order
    """, (company, role))

    # If no role layout, return system defaults for this role
    if not layout:
        layout = query_dicts("""
            SELECT widget_key, default_sort_order as sort_order, true as visible,
                   '{}'::text as config, display_name, component_name, category
            FROM dashboard_widget
            WHERE is_active = true AND %s = ANY(default_roles)
            ORDER BY default_sort_order
        """, (role,))

    return {"role": role, "source": "role" if layout else "system_default", "widgets": layout}


class LayoutItem(BaseModel):
    widget_key: str
    sort_order: int
    visible: bool = True
    config: Optional[dict] = None


class LayoutUpdate(BaseModel):
    widgets: List[LayoutItem]


@app.put("/api/layout/role/{role}")
async def update_role_layout(role: str, body: LayoutUpdate, request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Set role layout (VD only). Replaces all widgets for this role."""
    require_perm(user, "admin.dashboard_layout")

    # Delete existing
    execute("DELETE FROM dashboard_layout WHERE company_code=%s AND role=%s AND user_id IS NULL", (company, role), db=RM_CENTRAL_DB)

    # Insert new
    for w in body.widgets:
        cfg = json.dumps(w.config or {})
        execute(
            "INSERT INTO dashboard_layout (company_code, role, user_id, widget_key, sort_order, visible, config) VALUES (%s, %s, NULL, %s, %s, %s, %s::jsonb)",
            (company, role, w.widget_key, w.sort_order, w.visible, cfg),
            db=RM_CENTRAL_DB
        )

    return {"status": "updated", "role": role, "widget_count": len(body.widgets)}


@app.put("/api/layout")
async def update_personal_layout(body: LayoutUpdate, request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Set personal layout for current user."""
    username = user["username"]
    user_rows = query_dicts("SELECT id FROM portal_user WHERE username=%s", (username,))
    if not user_rows:
        raise HTTPException(status_code=404, detail="User not found")
    user_id = user_rows[0]["id"]

    # Delete existing personal layout
    execute("DELETE FROM dashboard_layout WHERE company_code=%s AND user_id=%s", (company, user_id), db=RM_CENTRAL_DB)

    # Insert new
    for w in body.widgets:
        cfg = json.dumps(w.config or {})
        execute(
            "INSERT INTO dashboard_layout (company_code, role, user_id, widget_key, sort_order, visible, config) VALUES (%s, %s, %s, %s, %s, %s::jsonb)",
            (company, user["role"], user_id, w.widget_key, w.sort_order, w.visible, cfg),
            db=RM_CENTRAL_DB
        )

    return {"status": "updated", "source": "personal", "widget_count": len(body.widgets)}


@app.delete("/api/layout")
async def reset_personal_layout(request: Request, user: dict = Depends(get_current_user)):
    company = get_company_code(request, user)
    """Reset personal layout (fall back to role default)."""
    username = user["username"]
    user_rows = query_dicts("SELECT id FROM portal_user WHERE username=%s", (username,))
    if not user_rows:
        raise HTTPException(status_code=404, detail="User not found")
    user_id = user_rows[0]["id"]

    execute("DELETE FROM dashboard_layout WHERE company_code=%s AND user_id=%s", (company, user_id), db=RM_CENTRAL_DB)
    return {"status": "reset", "source": "role_default"}

@app.get("/api/admin/audit")
async def get_audit_log(user: dict = Depends(get_current_user), limit: int = 100, offset: int = 0):
    """Retrieve audit log entries with optional filtering."""
    require_perm(user, "admin.config")
    # Limit to reasonable values
    limit = min(max(limit, 1), 500)
    offset = max(offset, 0)
    rows = query_dicts("""
        SELECT id, actor_username, action, target_type, target_id, target_name,
               details, created_at::text
        FROM admin_audit_log
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))
    # Get total count
    total_rows = query_dicts("SELECT COUNT(*) as cnt FROM admin_audit_log")
    total = int(total_rows[0]["cnt"]) if total_rows else 0
    return {"entries": rows, "total": total, "limit": limit, "offset": offset}
