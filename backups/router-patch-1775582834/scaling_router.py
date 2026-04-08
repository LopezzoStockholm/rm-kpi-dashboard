"""
scaling_router.py — Scaling Up: Goals, Rocks, Scorecard, Key Activities

Strategilagret i Howwe/EOS-modellen. CRUD för årsmål, kvartalsmål (rocks),
scorecard-targets med entries och key activities kopplade till rocks.
Registreras via init_scaling_router() som injicerar auth-funktioner från kärnan.
"""

import logging
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from rm_data import query_dicts, execute

logger = logging.getLogger(__name__)

router = APIRouter(tags=["scaling"])

# --- Dependency injection from core -----------------------------------------
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None


def init_scaling_router(get_current_user, has_perm, require_perm, audit_log):
    """Called once from portal_api.py to inject shared auth functions."""
    global _get_current_user, _has_perm, _require_perm, _audit_log
    _get_current_user = get_current_user
    _has_perm = has_perm
    _require_perm = require_perm
    _audit_log = audit_log


# ============================================================================
# Pydantic models
# ============================================================================

class RockCreate(BaseModel):
    title: str
    owner: str
    role: str = "vd"
    quarter: Optional[str] = None
    due_date: Optional[str] = None
    parent_goal_id: Optional[int] = None
    notes: Optional[str] = None

class RockUpdate(BaseModel):
    status: Optional[str] = None
    progress: Optional[int] = None
    notes: Optional[str] = None
    title: Optional[str] = None
    owner: Optional[str] = None

class ScorecardEntryCreate(BaseModel):
    target_id: int
    period: str
    actual_value: float
    notes: Optional[str] = None
    entered_by: Optional[str] = None

class GoalUpdate(BaseModel):
    current_value: Optional[float] = None
    status: Optional[str] = None

class GoalCreate(BaseModel):
    year: int
    title: str
    category: str
    target_value: Optional[float] = None
    current_value: Optional[float] = None
    unit: Optional[str] = None
    owner: Optional[str] = None

class GoalUpdateFull(BaseModel):
    title: Optional[str] = None
    category: Optional[str] = None
    target_value: Optional[float] = None
    current_value: Optional[float] = None
    unit: Optional[str] = None
    owner: Optional[str] = None
    status: Optional[str] = None

class TargetCreate(BaseModel):
    metric_name: str
    role: str
    target_value: float
    unit: Optional[str] = ""
    frequency: str = "weekly"
    owner: str
    is_green_above: bool = True
    sort_order: int = 0
    key_activity_id: Optional[int] = None
    source_query: Optional[str] = None
    auto_populate: bool = False

class TargetUpdate(BaseModel):
    metric_name: Optional[str] = None
    role: Optional[str] = None
    target_value: Optional[float] = None
    unit: Optional[str] = None
    frequency: Optional[str] = None
    owner: Optional[str] = None
    is_green_above: Optional[bool] = None
    sort_order: Optional[int] = None
    active: Optional[bool] = None
    key_activity_id: Optional[int] = None
    source_query: Optional[str] = None
    auto_populate: Optional[bool] = None

class KeyActivityCreate(BaseModel):
    rock_id: int
    title: str
    owner: Optional[str] = None
    role: Optional[str] = None
    frequency: str = "weekly"
    target_per_period: int = 1
    unit: Optional[str] = None
    sort_order: int = 0

class KeyActivityUpdate(BaseModel):
    title: Optional[str] = None
    owner: Optional[str] = None
    role: Optional[str] = None
    frequency: Optional[str] = None
    target_per_period: Optional[int] = None
    unit: Optional[str] = None
    active: Optional[bool] = None
    sort_order: Optional[int] = None


# ============================================================================
# Rocks
# ============================================================================

@router.get("/api/rocks")
async def get_rocks(quarter: Optional[str] = None, role: Optional[str] = None):
    """Hämta alla rocks, filtrera på kvartal och/eller roll."""
    conds = ["r.company_code = %s"]
    params: list = ["RM"]
    if quarter:
        conds.append("r.quarter = %s")
        params.append(quarter)
    if role:
        conds.append("r.role = %s")
        params.append(role)
    rows = query_dicts(
        f"""SELECT r.*, g.title as goal_title
            FROM quarterly_rock r
            LEFT JOIN annual_goal g ON r.parent_goal_id = g.id
            WHERE {' AND '.join(conds)}
            ORDER BY r.status = 'done', r.due_date NULLS LAST, r.id""",
        tuple(params)
    )
    total = len(rows)
    done = sum(1 for r in rows if r.get("status") == "done")
    at_risk = sum(1 for r in rows if r.get("status") == "at_risk")
    avg_progress = int(sum(r.get("progress", 0) for r in rows) / total) if total else 0
    return {
        "rocks": rows,
        "summary": {"total": total, "done": done, "at_risk": at_risk, "avg_progress": avg_progress}
    }


@router.post("/api/rocks")
async def create_rock(rock: RockCreate):
    """Skapa en ny rock."""
    q = rock.quarter
    if not q:
        m = datetime.now().month
        qn = (m - 1) // 3 + 1
        q = f"{datetime.now().year}-Q{qn}"
    new_id = execute(
        """INSERT INTO quarterly_rock (company_code, quarter, title, owner, role, due_date, parent_goal_id, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        ("RM", q, rock.title, rock.owner, rock.role, rock.due_date, rock.parent_goal_id, rock.notes),
        returning=True
    )
    if new_id is None:
        raise HTTPException(status_code=500, detail="Kunde inte skapa rock")
    return {"id": new_id, "quarter": q}


@router.put("/api/rocks/{rock_id}")
async def update_rock(rock_id: int, update: RockUpdate):
    """Uppdatera status/progress på en rock."""
    sets = []
    params = []
    for field in ["status", "progress", "notes", "title", "owner"]:
        val = getattr(update, field, None)
        if val is not None:
            sets.append(f"{field} = %s")
            params.append(val)
    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")
    sets.append("updated_at = NOW()")
    params.append(rock_id)
    execute(f"UPDATE quarterly_rock SET {', '.join(sets)} WHERE id = %s", tuple(params))
    return {"ok": True, "id": rock_id}


@router.delete("/api/rocks/{rock_id}")
async def delete_rock(rock_id: int):
    """Radera rock."""
    execute("DELETE FROM quarterly_rock WHERE id = %s", (rock_id,))
    return {"ok": True, "id": rock_id}


# ============================================================================
# Scorecard
# ============================================================================

@router.get("/api/scorecard")
async def get_scorecard(role: Optional[str] = None, weeks: int = 13):
    """Hämta scorecard med targets och senaste entries."""
    conds = ["active = true", "company_code = %s"]
    params: list = ["RM"]
    if role:
        conds.append("role = %s")
        params.append(role)
    targets = query_dicts(
        f"SELECT * FROM scorecard_target WHERE {' AND '.join(conds)} ORDER BY role, sort_order",
        tuple(params)
    )
    for t in targets:
        entries = query_dicts(
            "SELECT period, actual_value, notes FROM scorecard_entry WHERE target_id = %s ORDER BY period DESC LIMIT %s",
            (t["id"], weeks)
        )
        t["entries"] = entries
        if entries:
            latest = entries[0]["actual_value"]
            t["latest_value"] = float(latest) if latest is not None else None
            is_green = (float(latest) >= float(t["target_value"])) if t.get("is_green_above") else (float(latest) <= float(t["target_value"]))
            t["status"] = "green" if is_green else "red"
        else:
            t["latest_value"] = None
            t["status"] = "grey"
    return {"targets": targets}


@router.post("/api/scorecard/entry")
async def create_scorecard_entry(entry: ScorecardEntryCreate):
    """Registrera ett scorecard-värde (upsert per target+period)."""
    new_id = execute(
        """INSERT INTO scorecard_entry (target_id, period, actual_value, notes, entered_by)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (target_id, period) DO UPDATE SET actual_value=EXCLUDED.actual_value, notes=EXCLUDED.notes
           RETURNING id""",
        (entry.target_id, entry.period, entry.actual_value, entry.notes, entry.entered_by),
        returning=True
    )
    return {"id": new_id}


@router.post("/api/scorecard/target")
async def create_target(target: TargetCreate, request: Request):
    """Skapa ny scorecard-target."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    new_id = execute(
        """INSERT INTO scorecard_target
           (company_code, metric_name, role, target_value, unit, frequency, owner,
            is_green_above, sort_order, active, key_activity_id, source_query, auto_populate)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, true, %s, %s, %s) RETURNING id""",
        ("RM", target.metric_name, target.role, target.target_value, target.unit,
         target.frequency, target.owner, target.is_green_above, target.sort_order,
         target.key_activity_id, target.source_query, target.auto_populate),
        returning=True
    )
    if new_id is None:
        raise HTTPException(status_code=500, detail="Kunde inte skapa target")
    return {"id": new_id}


@router.put("/api/scorecard/target/{target_id}")
async def update_target(target_id: int, update: TargetUpdate, request: Request):
    """Uppdatera scorecard-target."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    sets = []
    params = []
    sent = update.model_fields_set if hasattr(update, "model_fields_set") else set()
    for field in ["metric_name", "role", "target_value", "unit", "frequency", "owner",
                  "is_green_above", "sort_order", "active", "key_activity_id", "source_query", "auto_populate"]:
        if field in sent:
            sets.append(f"{field} = %s")
            params.append(getattr(update, field))
    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")
    params.append(target_id)
    execute(f"UPDATE scorecard_target SET {', '.join(sets)} WHERE id = %s", tuple(params))
    return {"ok": True, "id": target_id}


@router.delete("/api/scorecard/target/{target_id}")
async def delete_target(target_id: int):
    """Soft delete: sätt active=false."""
    execute("UPDATE scorecard_target SET active = false WHERE id = %s", (target_id,))
    return {"ok": True, "id": target_id}


# ============================================================================
# Annual Goals
# ============================================================================

@router.get("/api/goals")
async def get_goals(year: Optional[int] = None):
    """Hämta årsmål."""
    y = year or datetime.now().year
    goals = query_dicts(
        "SELECT * FROM annual_goal WHERE company_code = %s AND year = %s ORDER BY category, id",
        ("RM", y)
    )
    for g in goals:
        rocks = query_dicts(
            "SELECT id, title, status, progress, quarter FROM quarterly_rock WHERE parent_goal_id = %s ORDER BY quarter",
            (g["id"],)
        )
        g["rocks"] = rocks
        g["rock_count"] = len(rocks)
        g["rocks_done"] = sum(1 for r in rocks if r.get("status") == "done")
    return {"goals": goals, "year": y}


@router.put("/api/goals/{goal_id}")
async def update_goal(goal_id: int, update: GoalUpdate, request: Request):
    """Uppdatera current_value eller status på ett årsmål."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    sets = []
    params = []
    if update.current_value is not None:
        sets.append("current_value = %s")
        params.append(update.current_value)
    if update.status is not None:
        sets.append("status = %s")
        params.append(update.status)
    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")
    sets.append("updated_at = NOW()")
    params.append(goal_id)
    execute(f"UPDATE annual_goal SET {', '.join(sets)} WHERE id = %s", tuple(params))
    return {"ok": True, "id": goal_id}


@router.post("/api/goals")
async def create_goal(goal: GoalCreate, request: Request):
    """Skapa nytt årsmål."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    new_id = execute(
        """INSERT INTO annual_goal (company_code, year, title, category, target_value, current_value, unit, owner, status)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active') RETURNING id""",
        ("RM", goal.year, goal.title, goal.category, goal.target_value, goal.current_value, goal.unit, goal.owner),
        returning=True
    )
    if new_id is None:
        raise HTTPException(status_code=500, detail="Kunde inte skapa mål")
    return {"id": new_id}


@router.patch("/api/goals/{goal_id}")
async def update_goal_full(goal_id: int, update: GoalUpdateFull, request: Request):
    """Uppdatera alla fält på ett årsmål."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    sets = []
    params = []
    for field in ["title", "category", "target_value", "current_value", "unit", "owner", "status"]:
        val = getattr(update, field, None)
        if val is not None:
            sets.append(f"{field} = %s")
            params.append(val)
    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")
    sets.append("updated_at = NOW()")
    params.append(goal_id)
    execute(f"UPDATE annual_goal SET {', '.join(sets)} WHERE id = %s", tuple(params))
    return {"ok": True, "id": goal_id}


@router.delete("/api/goals/{goal_id}")
async def delete_goal(goal_id: int, request: Request):
    """Radera årsmål (koppla loss rocks först)."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    execute("UPDATE quarterly_rock SET parent_goal_id = NULL WHERE parent_goal_id = %s", (goal_id,))
    execute("DELETE FROM annual_goal WHERE id = %s", (goal_id,))
    return {"ok": True, "id": goal_id}


# ============================================================================
# Key Activities (Howwe-inspirerat)
# ============================================================================

@router.get("/api/key-activities")
async def get_key_activities(
    request: Request,
    rock_id: Optional[int] = None,
    owner: Optional[str] = None,
    active: bool = True
):
    """Hämta key activities. Icke-VD ser bara sina egna."""
    user = await _get_current_user(request)
    if not _has_perm(user, "admin.scaling_up") and not owner:
        owner = user.get("owner_alias")
    conds = []
    params = []
    if active:
        conds.append("ka.active = true")
    if rock_id is not None:
        conds.append("ka.rock_id = %s")
        params.append(rock_id)
    if owner:
        conds.append("ka.owner = %s")
        params.append(owner)
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    rows = query_dicts(
        f"""SELECT ka.*, r.title AS rock_title, r.quarter, r.status AS rock_status
            FROM key_activity ka
            JOIN quarterly_rock r ON r.id = ka.rock_id
            {where}
            ORDER BY ka.rock_id, ka.sort_order, ka.id""",
        tuple(params) if params else None
    )
    return {"activities": rows}


@router.post("/api/key-activities")
async def create_key_activity(ka: KeyActivityCreate, request: Request):
    """Skapa en ny key activity kopplad till en rock."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    new_id = execute(
        """INSERT INTO key_activity (rock_id, title, owner, role, frequency, target_per_period, unit, sort_order)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (ka.rock_id, ka.title, ka.owner, ka.role, ka.frequency, ka.target_per_period, ka.unit, ka.sort_order),
        returning=True
    )
    if new_id is None:
        raise HTTPException(status_code=500, detail="Kunde inte skapa key activity")
    return {"id": new_id}


@router.patch("/api/key-activities/{ka_id}")
async def update_key_activity(ka_id: int, update: KeyActivityUpdate, request: Request):
    """Uppdatera en key activity."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    sets = []
    params = []
    for field in ["title", "owner", "role", "frequency", "target_per_period", "unit", "active", "sort_order"]:
        val = getattr(update, field, None)
        if val is not None:
            sets.append(f"{field} = %s")
            params.append(val)
    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")
    sets.append("updated_at = NOW()")
    params.append(ka_id)
    execute(f"UPDATE key_activity SET {', '.join(sets)} WHERE id = %s", tuple(params))
    return {"ok": True, "id": ka_id}


@router.delete("/api/key-activities/{ka_id}")
async def delete_key_activity(ka_id: int, request: Request):
    """Soft-delete (active=false) en key activity."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.scaling_up")
    execute("UPDATE key_activity SET active = false, updated_at = NOW() WHERE id = %s", (ka_id,))
    return {"ok": True, "id": ka_id}
