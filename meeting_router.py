"""
meeting_router.py — Operativt execution-lager (Howwe-modellen)

Weekly Acceleration Meeting, Hit-rate-analys och Daily Huddle.
Registreras via init_meeting_router() som injicerar auth-funktioner från kärnan.
"""

import logging
from typing import Optional
from datetime import datetime
from collections import OrderedDict

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from rm_data import query_dicts, query_one, execute

logger = logging.getLogger(__name__)

router = APIRouter(tags=["meeting"])

# --- Dependency injection from core -----------------------------------------
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_meeting_router(get_current_user, has_perm, require_perm, audit_log, get_company_code_fn=None):
    """Called once from portal_api.py to inject shared auth functions."""
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user
    _has_perm = has_perm
    _require_perm = require_perm
    _audit_log = audit_log
    _get_company_code = get_company_code_fn



def _cc(request, user):
    """Resolve company_code from header via injected function."""
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"

# ============================================================================
# Pydantic models
# ============================================================================

class CommitmentUpdate(BaseModel):
    committed_count: Optional[int] = None
    actual_count: Optional[int] = None
    completed: Optional[bool] = None
    blocker: Optional[str] = None
    notes: Optional[str] = None


# ============================================================================
# Helpers
# ============================================================================

def _current_iso_week() -> str:
    y, w, _ = datetime.utcnow().isocalendar()
    return f"{y}-W{w:02d}"


# ============================================================================
# Weekly Acceleration Meeting
# ============================================================================

@router.get("/api/meetings/weekly")
async def get_weekly_commitments(
    request: Request,
    week: Optional[str] = None,
    owner: Optional[str] = None
):
    """Hämta veckans commitments. Icke-VD ser bara sina egna."""
    user = await _get_current_user(request)
    wk = week or _current_iso_week()
    if not _has_perm(user, "admin.scaling_up") and not owner:
        owner = user.get("owner_alias")
    conds = ["mc.iso_week = %s"]
    params: list = [wk]
    if owner:
        conds.append("mc.owner = %s")
        params.append(owner)
    rows = query_dicts(
        f"""SELECT mc.*,
                   ka.title AS activity_title,
                   ka.target_per_period,
                   ka.unit,
                   ka.frequency,
                   ka.role AS activity_role,
                   r.id AS rock_id,
                   r.title AS rock_title,
                   r.quarter
            FROM meeting_commitment mc
            JOIN key_activity ka ON ka.id = mc.key_activity_id
            JOIN quarterly_rock r ON r.id = ka.rock_id
            WHERE {' AND '.join(conds)}
            ORDER BY mc.owner, r.id, ka.sort_order, ka.id""",
        tuple(params)
    )
    summary: dict = {}
    for r in rows:
        o = r["owner"]
        if o not in summary:
            summary[o] = {"total": 0, "completed": 0, "committed": 0, "actual": 0, "blockers": 0}
        summary[o]["total"] += 1
        if r["completed"]:
            summary[o]["completed"] += 1
        summary[o]["committed"] += r["committed_count"] or 0
        summary[o]["actual"] += r["actual_count"] or 0
        if r["blocker"]:
            summary[o]["blockers"] += 1
    return {"week": wk, "commitments": rows, "summary": summary}


@router.post("/api/meetings/weekly/generate")
async def generate_weekly_commitments(week: Optional[str] = None):
    """Skapa tomma commitment-rader för veckan från alla aktiva key activities."""
    wk = week or _current_iso_week()
    activities = query_dicts(
        """SELECT ka.id, ka.owner, ka.target_per_period
           FROM key_activity ka
           JOIN quarterly_rock r ON r.id = ka.rock_id
           WHERE ka.active = true AND ka.owner IS NOT NULL AND ka.owner != ''
             AND r.status != 'done'"""
    )
    created = 0
    for a in activities:
        result = execute(
            """INSERT INTO meeting_commitment (iso_week, key_activity_id, owner, committed_count)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (iso_week, key_activity_id, owner) DO NOTHING
               RETURNING id""",
            (wk, a["id"], a["owner"], a["target_per_period"] or 1),
            returning=True
        )
        if result is not None:
            created += 1
    return {"ok": True, "week": wk, "created": created, "total_activities": len(activities)}


@router.patch("/api/meetings/commitment/{cid}")
async def update_commitment(cid: int, update: CommitmentUpdate):
    """Uppdatera en commitment (actual, committed, blocker, notes, completed)."""
    sets = []
    params = []
    sent_fields = update.model_fields_set if hasattr(update, "model_fields_set") else set()
    for field in ["committed_count", "actual_count", "completed", "blocker", "notes"]:
        if field in sent_fields:
            val = getattr(update, field, None)
            sets.append(f"{field} = %s")
            params.append(val)
    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")
    sets.append("updated_at = NOW()")
    params.append(cid)
    execute(f"UPDATE meeting_commitment SET {', '.join(sets)} WHERE id = %s", tuple(params))
    # Auto-completed om actual >= committed
    if update.completed is None and update.actual_count is not None:
        execute(
            "UPDATE meeting_commitment SET completed = (actual_count >= committed_count AND committed_count > 0) WHERE id = %s",
            (cid,)
        )
    return {"ok": True, "id": cid}


# ============================================================================
# Hit-rate
# ============================================================================

@router.get("/api/meetings/hitrate")
async def get_hitrate(
    request: Request,
    weeks: int = 8,
    owner: Optional[str] = None
):
    """Hit-rate rullande N veckor per key activity, med matris, streak och at_risk-flagga."""
    user = await _get_current_user(request)
    if not _has_perm(user, "admin.scaling_up") and not owner:
        owner = user.get("owner_alias")
    where_owner = ""
    params: list = [weeks]
    if owner:
        where_owner = "AND mc.owner = %s"
        params.append(owner)
    raw = query_dicts(
        f"""SELECT mc.owner,
                   ka.id AS key_activity_id,
                   ka.title AS activity_title,
                   r.title AS rock_title,
                   mc.iso_week,
                   mc.completed,
                   mc.committed_count,
                   mc.actual_count,
                   mc.blocker
            FROM meeting_commitment mc
            JOIN key_activity ka ON ka.id = mc.key_activity_id
            JOIN quarterly_rock r ON r.id = ka.rock_id
            WHERE mc.iso_week >= TO_CHAR(NOW() - (%s || ' weeks')::INTERVAL, 'IYYY-"W"IW')
              {where_owner}
            ORDER BY mc.iso_week DESC""",
        tuple(params)
    )
    # Gruppera per (owner, key_activity_id)
    groups: dict = OrderedDict()
    all_weeks: set = set()
    for r in raw:
        key = (r["owner"], r["key_activity_id"])
        all_weeks.add(r["iso_week"])
        if key not in groups:
            groups[key] = {
                "owner": r["owner"],
                "key_activity_id": r["key_activity_id"],
                "activity_title": r["activity_title"],
                "rock_title": r["rock_title"],
                "weeks_data": {},
            }
        groups[key]["weeks_data"][r["iso_week"]] = {
            "completed": r["completed"],
            "committed": r["committed_count"],
            "actual": r["actual_count"],
            "blocker": r["blocker"],
        }
    sorted_weeks = sorted(all_weeks, reverse=True)

    # Owner->role map
    owner_role_rows = query_dicts(
        "SELECT owner_alias, role FROM portal_user WHERE owner_alias IS NOT NULL AND active = TRUE"
    )
    owner_role_map = {r["owner_alias"]: r["role"] for r in owner_role_rows}

    # Threshold resolver
    _thr_cache: dict = {}
    def resolve_hitrate_threshold(ka_id, row_owner):
        role = owner_role_map.get(row_owner)
        cache_key = (ka_id, role)
        if cache_key in _thr_cache:
            return _thr_cache[cache_key]
        res = query_dicts("SELECT * FROM get_threshold(%s, %s, NULL, 'hitrate')", (ka_id, role))
        thr = res[0] if res else {
            "green_at": 80, "yellow_at": 50, "window_weeks": 4,
            "min_data_points": 2, "at_risk_consecutive": 2,
            "direction": "higher_better", "resolved_scope": "fallback"
        }
        _thr_cache[cache_key] = thr
        return thr

    rows = []
    for g in groups.values():
        wd = g["weeks_data"]
        total = len(wd)
        completed = sum(1 for w in wd.values() if w["completed"])
        hit_rate = round(100.0 * completed / total) if total > 0 else 0

        thr = resolve_hitrate_threshold(g["key_activity_id"], g["owner"])
        window = int(thr["window_weeks"])
        green_at = float(thr["green_at"])
        yellow_at = float(thr["yellow_at"])
        min_data = int(thr["min_data_points"])

        # Streak
        streak = 0
        for w in sorted_weeks:
            if w in wd:
                if wd[w]["completed"]:
                    streak += 1
                else:
                    break
            else:
                break

        # Rolling window
        last_n = [wd[w]["completed"] for w in sorted_weeks[:window] if w in wd]
        last_n_pct = round(100.0 * sum(1 for c in last_n if c) / len(last_n)) if last_n else 0
        at_risk = last_n_pct < yellow_at and len(last_n) >= min_data

        if last_n_pct >= green_at:
            status = "green"
        elif last_n_pct >= yellow_at:
            status = "yellow"
        else:
            status = "red"

        timeline = []
        for w in sorted_weeks[:weeks]:
            if w in wd:
                timeline.append({"week": w, "completed": wd[w]["completed"], "blocker": wd[w]["blocker"]})
            else:
                timeline.append({"week": w, "completed": None, "blocker": None})

        rows.append({
            "owner": g["owner"],
            "key_activity_id": g["key_activity_id"],
            "activity_title": g["activity_title"],
            "rock_title": g["rock_title"],
            "total_weeks": total,
            "completed_weeks": completed,
            "hit_rate_pct": hit_rate,
            "last_4w_pct": last_n_pct,
            "window_weeks": window,
            "streak": streak,
            "at_risk": at_risk,
            "status": status,
            "threshold_green": green_at,
            "threshold_yellow": yellow_at,
            "threshold_scope": thr.get("resolved_scope", "system:default"),
            "timeline": timeline,
        })
    rows.sort(key=lambda x: (not x["at_risk"], x["last_4w_pct"]))
    return {"weeks": weeks, "all_weeks": sorted_weeks[:weeks], "rows": rows}


# ============================================================================
# Daily Huddle
# ============================================================================

@router.get("/api/meeting/daily")
async def daily_huddle(request: Request):
    """Data för daily huddle: stuck items, today tasks, critical numbers."""
    company = request.headers.get("x-company-code", "RM")
    stuck_rocks = query_dicts(
        "SELECT id, title, owner, status, progress FROM quarterly_rock "
        "WHERE company_code = %s AND status IN ('behind','at_risk') AND status != 'done' "
        "ORDER BY status DESC, progress",
        (company,)
    )
    overdue_tasks = query_dicts(
        "SELECT id, title, assigned_to, due_date::text FROM planner_task "
        "WHERE status != 'completed' AND company_code = %s AND due_date < CURRENT_DATE "
        "ORDER BY due_date LIMIT 10",
        (company,)
    )
    red_metrics = query_dicts(
        """SELECT t.metric_name, t.target_value, t.unit, t.owner, e.actual_value, e.period
           FROM scorecard_target t
           JOIN LATERAL (
               SELECT actual_value, period FROM scorecard_entry WHERE target_id = t.id ORDER BY period DESC LIMIT 1
           ) e ON true
           WHERE t.active = true AND t.company_code = %s
             AND ((t.is_green_above AND e.actual_value < t.target_value)
                  OR (NOT t.is_green_above AND e.actual_value > t.target_value))""",
        (company,)
    )
    bank = query_one("SELECT balance FROM bank_balance WHERE company_code = %s ORDER BY date DESC LIMIT 1", (company,))
    return {
        "stuck_rocks": stuck_rocks,
        "overdue_tasks": overdue_tasks,
        "red_metrics": red_metrics,
        "bank_balance": float(bank) if bank else None,
        "generated_at": datetime.now().isoformat()
    }


@router.get("/api/hitrate/summary")
async def hitrate_summary(request: Request):
    """Lättviktig hit-rate summering för dashboard-widget.
    Returnerar team-snitt, antal at-risk, top 5 at-risk activities."""
    user = await _get_current_user(request)
    company = _cc(request, user)

    # Hämta alla aktiva key_activities med rullande 4v hit-rate
    raw = query_dicts("""
        WITH weekly AS (
            SELECT mc.owner,
                   ka.id AS ka_id,
                   ka.title AS activity_title,
                   r.title AS rock_title,
                   mc.iso_week,
                   mc.completed
            FROM meeting_commitment mc
            JOIN key_activity ka ON ka.id = mc.key_activity_id AND ka.active = TRUE
            JOIN quarterly_rock r ON r.id = ka.rock_id
            WHERE mc.iso_week >= to_char(now() - interval '4 weeks', 'IYYY"-W"IW')
        ),
        per_activity AS (
            SELECT owner, ka_id, activity_title, rock_title,
                   COUNT(*) AS total,
                   SUM(CASE WHEN completed THEN 1 ELSE 0 END) AS done,
                   ROUND(100.0 * SUM(CASE WHEN completed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)) AS pct
            FROM weekly
            GROUP BY owner, ka_id, activity_title, rock_title
        )
        SELECT owner, ka_id, activity_title, rock_title, total, done, pct
        FROM per_activity
        ORDER BY pct ASC, owner
    """)

    at_risk = [r for r in raw if (r["pct"] or 0) < 50]
    team_pcts = [r["pct"] for r in raw if r["pct"] is not None]
    team_avg = round(sum(team_pcts) / len(team_pcts)) if team_pcts else 0

    green = sum(1 for r in raw if (r["pct"] or 0) >= 80)
    yellow = sum(1 for r in raw if 50 <= (r["pct"] or 0) < 80)
    red = sum(1 for r in raw if (r["pct"] or 0) < 50)

    return {
        "team_avg_pct": team_avg,
        "total_activities": len(raw),
        "green": green,
        "yellow": yellow,
        "red": red,
        "at_risk_count": len(at_risk),
        "top_at_risk": [
            {
                "owner": r["owner"],
                "activity": r["activity_title"],
                "rock": r["rock_title"],
                "pct": int(r["pct"] or 0),
                "done": int(r["done"]),
                "total": int(r["total"]),
            }
            for r in at_risk[:5]
        ],
    }
