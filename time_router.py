"""
time_router.py — Tidrapporteringsmodul (del av Modul 1: Fält)

CRUD för tidrapporter, godkännande/attestering, summering, profession_rate.
Ersätter Next Tech Field bookedhours.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from rm_data import query_dicts, execute

logger = logging.getLogger(__name__)

router = APIRouter(tags=["time"])

# --- Dependency injection (set by init_time_router) ---
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_time_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn, get_company_code_fn=None):
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user_fn
    _has_perm = has_perm_fn
    _require_perm = require_perm_fn
    _audit_log = audit_log_fn
    _get_company_code = get_company_code_fn


def _cc(request, user):
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"


# --- Pydantic models ---

class TimeReportCreate(BaseModel):
    work_date: str
    hours: float = Field(gt=0, le=24)
    project_code: Optional[str] = None
    next_project_no: Optional[str] = None
    category: Optional[str] = "arbete"
    notes: Optional[str] = None
    profession_code: Optional[str] = None
    start_time: Optional[str] = None
    stop_time: Optional[str] = None
    break_minutes: Optional[int] = None
    travel_km: Optional[float] = None
    invoiceable: Optional[bool] = True
    is_absence: Optional[bool] = False
    absence_type: Optional[str] = None
    source: Optional[str] = "manual"
    user_id: Optional[int] = None  # admin can set for others


class TimeReportUpdate(BaseModel):
    work_date: Optional[str] = None
    hours: Optional[float] = None
    project_code: Optional[str] = None
    next_project_no: Optional[str] = None
    category: Optional[str] = None
    notes: Optional[str] = None
    profession_code: Optional[str] = None
    start_time: Optional[str] = None
    stop_time: Optional[str] = None
    break_minutes: Optional[int] = None
    travel_km: Optional[float] = None
    invoiceable: Optional[bool] = None
    is_absence: Optional[bool] = None
    absence_type: Optional[str] = None


class ProfessionRateCreate(BaseModel):
    profession_code: str
    display_name: str
    cost_per_hour: float
    price_per_hour: float


# --- Helpers ---

def _auto_cost(company: str, profession_code: Optional[str], hours: float):
    """Look up profession rate and calculate cost/revenue."""
    if not profession_code:
        return None, None, None, None
    rows = query_dicts(
        "SELECT cost_per_hour, price_per_hour FROM profession_rate WHERE company_code=%s AND profession_code=%s AND active=true",
        (company, profession_code)
    )
    if not rows:
        return None, None, None, None
    r = rows[0]
    cost_unit = float(r["cost_per_hour"])
    price_unit = float(r["price_per_hour"])
    return cost_unit, price_unit, round(cost_unit * hours, 2), round(price_unit * hours, 2)


def _travel_cost_calc(km: Optional[float]) -> Optional[float]:
    """Calculate travel cost at 25 kr/km (skatteverket 2026)."""
    if km and km > 0:
        return round(km * 25.0, 2)
    return None


# ============================================================================
# GET /api/time-reports — lista med filter
# ============================================================================

@router.get("/api/time-reports")
async def list_time_reports(
    request: Request,
    user_id: Optional[int] = None,
    project_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    approved: Optional[bool] = None,
    attested: Optional[bool] = None,
    invoiced: Optional[bool] = None,
    report_type: Optional[str] = None,
    supplier_id: Optional[int] = None,
    trade_code: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
):
    user = await _get_current_user(request)
    company = _cc(request, user)

    has_read_all = _has_perm(user, "time.read_all")

    conditions = ["t.company_code = %s"]
    params = [company]

    # Role-based filtering
    if not has_read_all:
        conditions.append("t.user_id = %s")
        params.append(user["id"])
    elif user_id:
        conditions.append("t.user_id = %s")
        params.append(user_id)

    if project_code:
        conditions.append("(t.project_code = %s OR t.next_project_no = %s)")
        params.extend([project_code, project_code])
    if date_from:
        conditions.append("t.work_date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("t.work_date <= %s")
        params.append(date_to)
    if approved is not None:
        conditions.append("t.approved = %s")
        params.append(approved)
    if attested is not None:
        conditions.append("t.attested = %s")
        params.append(attested)
    if invoiced is not None:
        conditions.append("t.invoiced = %s")
        params.append(invoiced)
    if report_type:
        conditions.append("t.report_type = %s")
        params.append(report_type)
    if supplier_id:
        conditions.append("t.supplier_id = %s")
        params.append(supplier_id)
    if trade_code:
        conditions.append("t.trade_code = %s")
        params.append(trade_code)

    where = " AND ".join(conditions)
    params.extend([limit, offset])

    rows = query_dicts(f"""
        SELECT t.*, p.display_name as user_display_name
        FROM time_report t
        LEFT JOIN portal_user p ON p.id = t.user_id
        WHERE {where}
        ORDER BY t.work_date DESC, t.id DESC
        LIMIT %s OFFSET %s
    """, tuple(params))

    count_rows = query_dicts(f"SELECT COUNT(*)::int as cnt FROM time_report t WHERE {where}", tuple(params[:-2]))
    total = count_rows[0]["cnt"] if count_rows else 0

    return {"items": rows, "total": total}


# ============================================================================
# GET /api/time-reports/my — mina rapporter (senaste 4 veckor)
# ============================================================================

@router.get("/api/time-reports/my")
async def my_time_reports(request: Request, weeks: int = 4):
    user = await _get_current_user(request)
    company = _cc(request, user)

    date_from = (date.today() - timedelta(weeks=weeks)).isoformat()
    uid = user.get("id")
    if not uid:
        return {"weeks": [], "total_entries": 0}

    rows = query_dicts("""
        SELECT t.*, np.project_name
        FROM time_report t
        LEFT JOIN next_project np ON np.project_no = t.project_code AND np.company_code = t.company_code
        WHERE t.company_code = %s AND t.user_id = %s AND t.work_date >= %s
        ORDER BY t.work_date DESC, t.id DESC
    """, (company, uid, date_from))

    # Group by week
    weekly = {}
    for r in rows:
        wd = r["work_date"]
        if hasattr(wd, 'isocalendar'):
            iso = wd.isocalendar()
        else:
            iso = date.fromisoformat(str(wd)).isocalendar()
        week_key = f"{iso[0]}-W{iso[1]:02d}"
        if week_key not in weekly:
            weekly[week_key] = {"week": week_key, "total_hours": 0, "entries": []}
        weekly[week_key]["total_hours"] += float(r["hours"] or 0)
        weekly[week_key]["entries"].append(r)

    return {"weeks": list(weekly.values()), "total_entries": len(rows)}


# ============================================================================
# GET /api/time-reports/summary — summering per projekt/person/period
# ============================================================================

@router.get("/api/time-reports/summary")
async def time_report_summary(
    request: Request,
    group_by: str = "project",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    report_type: Optional[str] = None,
):
    user = await _get_current_user(request)
    company = _cc(request, user)
    has_read_all = _has_perm(user, "time.read_all")

    conditions = ["t.company_code = %s"]
    params = [company]

    if not has_read_all:
        conditions.append("t.user_id = %s")
        params.append(user["id"])

    if date_from:
        conditions.append("t.work_date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("t.work_date <= %s")
        params.append(date_to)
    if report_type:
        conditions.append("t.report_type = %s")
        params.append(report_type)

    where = " AND ".join(conditions)

    if group_by == "project":
        rows = query_dicts(f"""
            SELECT COALESCE(t.project_code, t.next_project_no, 'Okänt') as group_key,
                   COALESCE(np.project_name, t.project_code, 'Okänt') as group_label,
                   SUM(t.hours)::numeric(10,2) as total_hours,
                   SUM(COALESCE(t.total_cost,0))::numeric(12,2) as total_cost,
                   SUM(COALESCE(t.total_revenue,0))::numeric(12,2) as total_revenue,
                   COUNT(*)::int as entry_count
            FROM time_report t
            LEFT JOIN next_project np ON np.project_no = COALESCE(t.project_code, t.next_project_no) AND np.company_code = t.company_code
            WHERE {where}
            GROUP BY 1, 2 ORDER BY total_hours DESC
        """, tuple(params))
    elif group_by == "person":
        rows = query_dicts(f"""
            SELECT t.user_id as group_key,
                   COALESCE(p.display_name, t.person, 'Okänd') as group_label,
                   SUM(t.hours)::numeric(10,2) as total_hours,
                   SUM(COALESCE(t.total_cost,0))::numeric(12,2) as total_cost,
                   SUM(COALESCE(t.total_revenue,0))::numeric(12,2) as total_revenue,
                   COUNT(*)::int as entry_count
            FROM time_report t
            LEFT JOIN portal_user p ON p.id = t.user_id
            WHERE {where}
            GROUP BY 1, 2 ORDER BY total_hours DESC
        """, tuple(params))
    elif group_by == "week":
        rows = query_dicts(f"""
            SELECT to_char(t.work_date, 'IYYY-\"W\"IW') as group_key,
                   to_char(t.work_date, 'IYYY-\"W\"IW') as group_label,
                   SUM(t.hours)::numeric(10,2) as total_hours,
                   SUM(COALESCE(t.total_cost,0))::numeric(12,2) as total_cost,
                   SUM(COALESCE(t.total_revenue,0))::numeric(12,2) as total_revenue,
                   COUNT(*)::int as entry_count
            FROM time_report t
            WHERE {where}
            GROUP BY 1, 2 ORDER BY 1 DESC
        """, tuple(params))
    elif group_by == "month":
        rows = query_dicts(f"""
            SELECT to_char(t.work_date, 'YYYY-MM') as group_key,
                   to_char(t.work_date, 'YYYY Mon') as group_label,
                   SUM(t.hours)::numeric(10,2) as total_hours,
                   SUM(COALESCE(t.total_cost,0))::numeric(12,2) as total_cost,
                   SUM(COALESCE(t.total_revenue,0))::numeric(12,2) as total_revenue,
                   COUNT(*)::int as entry_count
            FROM time_report t
            WHERE {where}
            GROUP BY 1, 2 ORDER BY 1 DESC
        """, tuple(params))
    elif group_by == "supplier":
        rows = query_dicts(f"""
            SELECT COALESCE(s.id::text, 'intern') as group_key,
                   COALESCE(s.name, 'Intern personal') as group_label,
                   SUM(t.hours)::numeric(10,2) as total_hours,
                   SUM(COALESCE(t.total_cost,0))::numeric(12,2) as total_cost,
                   SUM(COALESCE(t.total_revenue,0))::numeric(12,2) as total_revenue,
                   COUNT(*)::int as entry_count
            FROM time_report t
            LEFT JOIN supplier s ON s.id = t.supplier_id
            WHERE {where}
            GROUP BY 1, 2 ORDER BY total_hours DESC
        """, tuple(params))
    elif group_by == "trade":
        rows = query_dicts(f"""
            SELECT COALESCE(t.trade_code, 'okant') as group_key,
                   COALESCE(tr.name, t.trade_code, 'Okänt') as group_label,
                   SUM(t.hours)::numeric(10,2) as total_hours,
                   SUM(COALESCE(t.total_cost,0))::numeric(12,2) as total_cost,
                   SUM(COALESCE(t.total_revenue,0))::numeric(12,2) as total_revenue,
                   COUNT(*)::int as entry_count
            FROM time_report t
            LEFT JOIN trade tr ON tr.code = t.trade_code
            WHERE {where}
            GROUP BY 1, 2 ORDER BY total_hours DESC
        """, tuple(params))
    else:
        raise HTTPException(400, f"Invalid group_by: {group_by}. Use project/person/week/month/supplier/trade.")

    total_hours = sum(float(r["total_hours"] or 0) for r in rows)
    total_cost = sum(float(r["total_cost"] or 0) for r in rows)
    total_revenue = sum(float(r["total_revenue"] or 0) for r in rows)

    return {
        "group_by": group_by,
        "groups": rows,
        "totals": {"hours": total_hours, "cost": total_cost, "revenue": total_revenue}
    }


# ============================================================================
# POST /api/time-reports — skapa ny tidrapport
# ============================================================================

@router.post("/api/time-reports")
async def create_time_report(request: Request, body: TimeReportCreate):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.write")

    # Determine target user
    target_user_id = body.user_id if body.user_id and _has_perm(user, "time.admin") else user["id"]
    target_user_name = user.get("name", user.get("display_name", user.get("username", "")))
    if body.user_id and body.user_id != user["id"]:
        target_rows = query_dicts("SELECT display_name FROM portal_user WHERE id=%s", (body.user_id,))
        if target_rows:
            target_user_name = target_rows[0]["display_name"]

    # Auto-calculate cost/revenue from profession
    cost_unit, price_unit, total_cost, total_revenue = _auto_cost(company, body.profession_code, body.hours)
    travel_cost = _travel_cost_calc(body.travel_km)

    project_code = body.project_code or body.next_project_no

    new_id = execute("""
        INSERT INTO time_report (
            company_code, user_id, person, project_code, next_project_no, work_date, hours,
            category, notes, profession_code, cost_unit, price_unit, total_cost, total_revenue,
            travel_km, travel_cost, start_time, stop_time, break_minutes,
            invoiceable, is_absence, absence_type, source, whatsapp_message_id, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, NULL, now()
        ) RETURNING id
    """, (
        company, target_user_id, target_user_name, project_code, body.next_project_no, body.work_date, body.hours,
        body.category, body.notes, body.profession_code, cost_unit, price_unit, total_cost, total_revenue,
        body.travel_km, travel_cost, body.start_time, body.stop_time, body.break_minutes,
        body.invoiceable, body.is_absence, body.absence_type, body.source
    ), returning=True)
    _audit_log(user.get("username",""), "create", "time_report", new_id or 0, "", {"hours": body.hours, "project": project_code})

    return {"id": new_id, "status": "created"}


# ============================================================================
# PATCH /api/time-reports/{id} — uppdatera
# ============================================================================

@router.patch("/api/time-reports/{report_id}")
async def update_time_report(request: Request, report_id: int, body: TimeReportUpdate):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.write")

    # Fetch existing
    existing = query_dicts("SELECT * FROM time_report WHERE id=%s AND company_code=%s", (report_id, company))
    if not existing:
        raise HTTPException(404, "Tidrapport hittades inte")
    existing = existing[0]

    # Only owner or admin can edit
    is_admin = _has_perm(user, "time.admin")
    if existing["user_id"] != user["id"] and not is_admin:
        raise HTTPException(403, "Kan bara redigera egna tidrapporter")

    # Can't edit locked/attested reports
    if existing.get("locked") or existing.get("attested"):
        raise HTTPException(409, "Kan inte redigera låst/attesterad tidrapport")

    # Build update
    updates = {}
    data = body.dict(exclude_none=True)
    for key, val in data.items():
        updates[key] = val

    # Recalculate cost if hours or profession changed
    new_hours = updates.get("hours", float(existing["hours"]))
    new_prof = updates.get("profession_code", existing.get("profession_code"))
    if "hours" in updates or "profession_code" in updates:
        cost_unit, price_unit, total_cost, total_revenue = _auto_cost(company, new_prof, new_hours)
        if cost_unit is not None:
            updates["cost_unit"] = cost_unit
            updates["price_unit"] = price_unit
            updates["total_cost"] = total_cost
            updates["total_revenue"] = total_revenue

    if "travel_km" in updates:
        updates["travel_cost"] = _travel_cost_calc(updates["travel_km"])

    if not updates:
        return {"status": "no_changes"}

    updates["updated_at"] = "now()"
    set_clauses = []
    params = []
    for k, v in updates.items():
        if v == "now()":
            set_clauses.append(f"{k} = now()")
        else:
            set_clauses.append(f"{k} = %s")
            params.append(v)

    params.extend([report_id, company])
    execute(f"UPDATE time_report SET {', '.join(set_clauses)} WHERE id=%s AND company_code=%s", tuple(params))

    # Reset approval if content changed (not just notes)
    content_fields = {"hours", "project_code", "work_date", "profession_code"}
    if content_fields & set(data.keys()) and existing.get("approved"):
        execute("UPDATE time_report SET approved=false, approved_by=NULL, approved_at=NULL WHERE id=%s", (report_id,))

    _audit_log(user.get("username",""), "update", "time_report", report_id, "", {"changes": list(data.keys())})
    return {"status": "updated"}


# ============================================================================
# DELETE /api/time-reports/{id}
# ============================================================================

@router.delete("/api/time-reports/{report_id}")
async def delete_time_report(request: Request, report_id: int):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.write")

    existing = query_dicts("SELECT user_id, locked, attested FROM time_report WHERE id=%s AND company_code=%s", (report_id, company))
    if not existing:
        raise HTTPException(404, "Tidrapport hittades inte")

    is_admin = _has_perm(user, "time.admin")
    if existing[0]["user_id"] != user["id"] and not is_admin:
        raise HTTPException(403, "Kan bara ta bort egna tidrapporter")
    if existing[0].get("locked") or existing[0].get("attested"):
        raise HTTPException(409, "Kan inte ta bort låst/attesterad tidrapport")

    execute("DELETE FROM time_report WHERE id=%s AND company_code=%s", (report_id, company))
    _audit_log(user.get("username",""), "delete", "time_report", report_id)
    return {"status": "deleted"}


# ============================================================================
# POST /api/time-reports/{id}/approve — PL godkänner
# ============================================================================

@router.post("/api/time-reports/{report_id}/approve")
async def approve_time_report(request: Request, report_id: int):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.approve")

    existing = query_dicts("SELECT approved FROM time_report WHERE id=%s AND company_code=%s", (report_id, company))
    if not existing:
        raise HTTPException(404, "Tidrapport hittades inte")
    if existing[0]["approved"]:
        return {"status": "already_approved"}

    execute(
        "UPDATE time_report SET approved=true, approved_by=%s, approved_at=now() WHERE id=%s AND company_code=%s",
        (user["id"], report_id, company)
    )
    _audit_log(user.get("username",""), "approve", "time_report", report_id)
    return {"status": "approved"}


# ============================================================================
# POST /api/time-reports/{id}/attest — ekonomi attesterar
# ============================================================================

@router.post("/api/time-reports/{report_id}/attest")
async def attest_time_report(request: Request, report_id: int):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.attest")

    existing = query_dicts("SELECT approved, attested FROM time_report WHERE id=%s AND company_code=%s", (report_id, company))
    if not existing:
        raise HTTPException(404, "Tidrapport hittades inte")
    if not existing[0]["approved"]:
        raise HTTPException(409, "Måste godkännas av PL innan attestering")
    if existing[0]["attested"]:
        return {"status": "already_attested"}

    execute(
        "UPDATE time_report SET attested=true, attested_by=%s, attested_at=now(), locked=true WHERE id=%s AND company_code=%s",
        (user["id"], report_id, company)
    )
    _audit_log(user.get("username",""), "attest", "time_report", report_id)
    return {"status": "attested"}


# ============================================================================
# POST /api/time-reports/approve-batch — godkänn flera
# ============================================================================

@router.post("/api/time-reports/approve-batch")
async def approve_batch(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.approve")

    body = await request.json()
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(400, "Inga id:n angivna")

    placeholders = ",".join(["%s"] * len(ids))
    execute(
        f"UPDATE time_report SET approved=true, approved_by=%s, approved_at=now() WHERE id IN ({placeholders}) AND company_code=%s AND approved=false",
        tuple([user["id"]] + ids + [company])
    )
    _audit_log(user.get("username",""), "approve_batch", "time_report", 0, "", {"ids": ids})
    return {"status": "approved", "count": len(ids)}


# ============================================================================
# POST /api/time-reports/attest-batch — attestera flera
# ============================================================================

@router.post("/api/time-reports/attest-batch")
async def attest_batch(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.attest")

    body = await request.json()
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(400, "Inga id:n angivna")

    placeholders = ",".join(["%s"] * len(ids))
    execute(
        f"UPDATE time_report SET attested=true, attested_by=%s, attested_at=now(), locked=true WHERE id IN ({placeholders}) AND company_code=%s AND approved=true AND attested=false",
        tuple([user["id"]] + ids + [company])
    )
    _audit_log(user.get("username",""), "attest_batch", "time_report", 0, "", {"ids": ids})
    return {"status": "attested", "count": len(ids)}


# ============================================================================
# GET /api/profession-rates — lista yrkesroller
# ============================================================================

@router.get("/api/profession-rates")
async def list_profession_rates(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
    rows = query_dicts(
        "SELECT * FROM profession_rate WHERE company_code=%s AND active=true ORDER BY profession_code",
        (company,)
    )
    return {"items": rows}


# ============================================================================
# POST /api/profession-rates — skapa yrkesroll (admin)
# ============================================================================

@router.post("/api/profession-rates")
async def create_profession_rate(request: Request, body: ProfessionRateCreate):
    user = await _get_current_user(request)
    company = _cc(request, user)
    _require_perm(user, "time.admin")

    new_id = execute("""
        INSERT INTO profession_rate (company_code, profession_code, display_name, cost_per_hour, price_per_hour)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (company_code, profession_code)
        DO UPDATE SET display_name=EXCLUDED.display_name, cost_per_hour=EXCLUDED.cost_per_hour, price_per_hour=EXCLUDED.price_per_hour
        RETURNING id
    """, (company, body.profession_code, body.display_name, body.cost_per_hour, body.price_per_hour), returning=True)

    return {"id": new_id, "status": "upserted"}


# ============================================================================
# SUPPLIER (UE) CRUD
# ============================================================================

class SupplierCreate(BaseModel):
    name: str
    org_number: Optional[str] = None
    trade_code: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    default_hourly_rate: Optional[float] = None
    notes: Optional[str] = None

class SupplierUpdate(BaseModel):
    name: Optional[str] = None
    org_number: Optional[str] = None
    trade_code: Optional[str] = None
    contact_person: Optional[str] = None
    contact_phone: Optional[str] = None
    contact_email: Optional[str] = None
    default_hourly_rate: Optional[float] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None

class WorkerCreate(BaseModel):
    supplier_id: int
    name: str
    phone: Optional[str] = None
    trade_code: Optional[str] = None
    hourly_rate: Optional[float] = None

class WorkerUpdate(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    trade_code: Optional[str] = None
    hourly_rate: Optional[float] = None
    is_active: Optional[bool] = None

class SupplierProjectCreate(BaseModel):
    supplier_id: int
    project_code: str
    trade_code: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@router.get("/api/trades")
async def list_trades(request: Request):
    """Lista alla yrkeskategorier."""
    await _get_current_user(request)
    rows = query_dicts("SELECT code, name, sort_order FROM trade ORDER BY sort_order", db="rm_central")
    return {"items": rows}


@router.get("/api/suppliers")
async def list_suppliers(request: Request, trade_code: Optional[str] = None, active_only: bool = True):
    """Lista UE-företag med workers och projektantal."""
    user = await _get_current_user(request)
    _require_perm(user, "time.view_subcontractor")
    company = _cc(request, user)

    conds = ["s.company_code = %s"]
    params = [company]
    if active_only:
        conds.append("s.is_active = true")
    if trade_code:
        conds.append("s.trade_code = %s")
        params.append(trade_code)
    where = " AND ".join(conds)

    rows = query_dicts(f"""
        SELECT s.*,
               (SELECT count(*) FROM supplier_worker sw WHERE sw.supplier_id = s.id AND sw.is_active = true) as worker_count,
               (SELECT count(*) FROM supplier_project sp WHERE sp.supplier_id = s.id AND sp.is_active = true) as project_count
        FROM supplier s
        WHERE {where}
        ORDER BY s.name
    """, tuple(params), db="rm_central")
    return {"items": rows}


@router.post("/api/suppliers", status_code=201)
async def create_supplier(request: Request, body: SupplierCreate):
    user = await _get_current_user(request)
    _require_perm(user, "time.manage_suppliers")
    company = _cc(request, user)

    new_id = execute("""
        INSERT INTO supplier (company_code, name, org_number, trade_code,
                              contact_person, contact_phone, contact_email,
                              default_hourly_rate, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (company, body.name, body.org_number, body.trade_code,
          body.contact_person, body.contact_phone, body.contact_email,
          body.default_hourly_rate, body.notes),
    db="rm_central", returning=True)

    if _audit_log:
        _audit_log(user.get("username",""), "create_supplier", "supplier", new_id, body.name, {})
    return {"id": new_id, "status": "created"}


@router.patch("/api/suppliers/{supplier_id}")
async def update_supplier(supplier_id: int, request: Request, body: SupplierUpdate):
    user = await _get_current_user(request)
    _require_perm(user, "time.manage_suppliers")

    sets, params = [], []
    for field in ["name", "org_number", "trade_code", "contact_person",
                  "contact_phone", "contact_email", "default_hourly_rate",
                  "is_active", "notes"]:
        val = getattr(body, field, None)
        if val is not None:
            sets.append(f"{field} = %s")
            params.append(val)
    if not sets:
        raise HTTPException(400, "Inga fält att uppdatera")
    sets.append("updated_at = now()")
    params.append(supplier_id)

    execute(f"UPDATE supplier SET {', '.join(sets)} WHERE id = %s", tuple(params), db="rm_central")
    if _audit_log:
        _audit_log(user.get("username",""), "update_supplier", "supplier", supplier_id, "", {})
    return {"status": "updated"}


# --- Workers ---

@router.get("/api/suppliers/{supplier_id}/workers")
async def list_workers(supplier_id: int, request: Request):
    user = await _get_current_user(request)
    _require_perm(user, "time.view_subcontractor")

    rows = query_dicts("""
        SELECT sw.*, t.name as trade_name
        FROM supplier_worker sw
        LEFT JOIN trade t ON t.code = sw.trade_code
        WHERE sw.supplier_id = %s AND sw.is_active = true
        ORDER BY sw.name
    """, (supplier_id,), db="rm_central")
    return {"items": rows}


@router.post("/api/suppliers/workers", status_code=201)
async def create_worker(request: Request, body: WorkerCreate):
    user = await _get_current_user(request)
    _require_perm(user, "time.manage_suppliers")

    new_id = execute("""
        INSERT INTO supplier_worker (supplier_id, name, phone, trade_code, hourly_rate)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """, (body.supplier_id, body.name, body.phone, body.trade_code, body.hourly_rate),
    db="rm_central", returning=True)

    if _audit_log:
        _audit_log(user.get("username",""), "create_worker", "supplier_worker", new_id, body.name, {})
    return {"id": new_id, "status": "created"}


@router.patch("/api/suppliers/workers/{worker_id}")
async def update_worker(worker_id: int, request: Request, body: WorkerUpdate):
    user = await _get_current_user(request)
    _require_perm(user, "time.manage_suppliers")

    sets, params = [], []
    for field in ["name", "phone", "trade_code", "hourly_rate", "is_active"]:
        val = getattr(body, field, None)
        if val is not None:
            sets.append(f"{field} = %s")
            params.append(val)
    if not sets:
        raise HTTPException(400, "Inga fält att uppdatera")
    params.append(worker_id)
    execute(f"UPDATE supplier_worker SET {', '.join(sets)} WHERE id = %s", tuple(params), db="rm_central")
    return {"status": "updated"}


# --- Supplier ↔ Project ---

@router.get("/api/suppliers/{supplier_id}/projects")
async def list_supplier_projects(supplier_id: int, request: Request):
    user = await _get_current_user(request)
    _require_perm(user, "time.view_subcontractor")

    rows = query_dicts("""
        SELECT sp.*, COALESCE(npe.project_name, sp.project_code) as project_name
        FROM supplier_project sp
        LEFT JOIN next_project_economy npe ON npe.project_no = sp.project_code
        WHERE sp.supplier_id = %s AND sp.is_active = true
        ORDER BY sp.project_code
    """, (supplier_id,), db="rm_central")
    return {"items": rows}


@router.post("/api/suppliers/projects", status_code=201)
async def assign_supplier_project(request: Request, body: SupplierProjectCreate):
    user = await _get_current_user(request)
    _require_perm(user, "time.manage_suppliers")

    new_id = execute("""
        INSERT INTO supplier_project (supplier_id, project_code, trade_code, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (supplier_id, project_code) WHERE is_active = true
        DO UPDATE SET trade_code = EXCLUDED.trade_code, start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date
        RETURNING id
    """, (body.supplier_id, body.project_code, body.trade_code, body.start_date, body.end_date),
    db="rm_central", returning=True)

    if _audit_log:
        _audit_log(user.get("username",""), "assign_supplier_project", "supplier_project", new_id, f"{body.supplier_id}→{body.project_code}", {})
    return {"id": new_id, "status": "assigned"}


@router.delete("/api/suppliers/projects/{assignment_id}")
async def remove_supplier_project(assignment_id: int, request: Request):
    user = await _get_current_user(request)
    _require_perm(user, "time.manage_suppliers")

    execute("UPDATE supplier_project SET is_active = false WHERE id = %s", (assignment_id,), db="rm_central")
    return {"status": "deactivated"}
