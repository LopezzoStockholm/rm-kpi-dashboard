"""
productivity_router.py — Produktivitetsmodulen (Modul 6: Ops)

Tasks (Twenty CRM), Planner-tasks, Action items, Recurring templates (Årshjul),
Daily checklist, checklist historik och statistik.
Registreras via init_productivity_router() som injicerar auth-funktioner från kärnan.
"""

import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime, date

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from rm_data import query_dicts, query_one, execute

logger = logging.getLogger(__name__)

router = APIRouter(tags=["productivity"])

# Twenty CRM schema
TWENTY_SCHEMA = "workspace_13e0qz9uia3v9w5dx0mk6etm5"

# --- Dependency injection from core -----------------------------------------
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_productivity_router(get_current_user, has_perm, require_perm, audit_log, get_company_code_fn=None):
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

class TaskCreate(BaseModel):
    title: str
    due: Optional[str] = None
    deal_id: Optional[str] = None

class TaskDone(BaseModel):
    task_id: str


# ============================================================================
# CRM Tasks (Twenty)
# ============================================================================

@router.get("/api/tasks")
async def get_tasks(request: Request):
    """Hämta tasks från Twenty CRM. VD ser alla, övriga ser bara sina egna."""
    user = await _get_current_user(request)
    try:
        show_all = request.query_params.get("all") == "1" and _has_perm(user, "tasks.read_all")
        member_id = user.get("twenty_member_id")

        assignee_filter = ""
        if not show_all and member_id:
            assignee_filter = f"""AND (t."assigneeId" = %s OR t."assigneeId" IS NULL)"""
        elif not show_all and not member_id:
            assignee_filter = """AND t."assigneeId" IS NULL"""

        params = []
        if not show_all and member_id:
            params.append(member_id)

        query = f"""
            SELECT t.id, t.title, t.status::text as status, t."dueAt",
                   t."assigneeId",
                   COALESCE(m."nameFirstName" || ' ' || m."nameLastName", '') as assignee_name,
                   COALESCE(string_agg(DISTINCT o.name, ', '), '') as deal_names,
                   COALESCE(string_agg(DISTINCT c.name, ', '), '') as company_names
            FROM {TWENTY_SCHEMA}.task t
            LEFT JOIN {TWENTY_SCHEMA}."workspaceMember" m ON m.id = t."assigneeId"
            LEFT JOIN {TWENTY_SCHEMA}."taskTarget" tt ON tt."taskId" = t.id
            LEFT JOIN {TWENTY_SCHEMA}.opportunity o ON tt."targetOpportunityId" = o.id
            LEFT JOIN {TWENTY_SCHEMA}.company c ON tt."targetCompanyId" = c.id
            WHERE t."deletedAt" IS NULL {assignee_filter}
            GROUP BY t.id, t.title, t.status, t."dueAt", t."assigneeId", m."nameFirstName", m."nameLastName"
            ORDER BY t."dueAt" NULLS LAST
        """
        tasks = query_dicts(query, tuple(params) if params else None, db="twenty")
        return {"count": len(tasks), "tasks": tasks}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/task/done")
async def mark_task_done(body: TaskDone, request: Request):
    """Markera en CRM-task som klar."""
    user = await _get_current_user(request)
    try:
        result = query_dicts(
            f"""UPDATE {TWENTY_SCHEMA}.task
                SET status = 'DONE', "updatedAt" = NOW()
                WHERE id = %s
                RETURNING id, title, status::text as status""",
            (body.task_id,),
            db="twenty"
        )
        if not result:
            raise HTTPException(status_code=404, detail="Task not found")
        return {"success": True, "task": result[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/task/create")
async def create_task(body: TaskCreate, request: Request):
    """Skapa en ny CRM-task i Twenty."""
    user = await _get_current_user(request)
    try:
        _require_perm(user, "tasks.write")
        member_id = user.get("twenty_member_id")

        result = query_dicts(
            f"""INSERT INTO {TWENTY_SCHEMA}.task
                (title, status, "dueAt", "assigneeId", "createdAt", "updatedAt")
                VALUES (%s, 'TODO', %s, %s, NOW(), NOW())
                RETURNING id, title, status::text as status, "dueAt" """,
            (body.title, body.due, member_id),
            db="twenty"
        )
        if not result:
            raise Exception("Failed to create task")

        task_id = result[0]["id"]

        if body.deal_id:
            query_dicts(
                f"""INSERT INTO {TWENTY_SCHEMA}."taskTarget"
                    ("taskId", "targetOpportunityId", "createdAt", "updatedAt")
                    VALUES (%s, %s, NOW(), NOW())""",
                (task_id, body.deal_id),
                db="twenty"
            )

        return {"success": True, "task": result[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Action items (aggregerat)
# ============================================================================

@router.get("/api/actions")
async def get_actions(request: Request):
    """Sammanställning av brådskande åtgärder: förfallna fakturor, förhandlingsdeals, negativa TB1."""
    user = await _get_current_user(request)
    try:
        overdue_inv = query_dicts("""
            SELECT id, fortnox_id, customer_name, due_date, balance, total
            FROM fortnox_invoice
            WHERE due_date < CURRENT_DATE AND balance > 0
            ORDER BY due_date ASC
        """)
        overdue_sup = query_dicts("""
            SELECT id, fortnox_id, supplier_name, due_date, balance, total
            FROM fortnox_supplier_invoice
            WHERE due_date < CURRENT_DATE AND balance > 0
            ORDER BY due_date ASC
        """)
        forhandling = query_dicts("""
            SELECT id, name, calculated_value, stage
            FROM pipeline_deal
            WHERE stage = 'FORHANDLING'
            ORDER BY calculated_value DESC
        """)
        neg_tb1 = query_dicts("""
            SELECT project_name, project_leader, net_revenue, supplier_costs, tb1
            FROM project_profitability
            WHERE tb1 < 0 AND company_code = %s
            ORDER BY tb1 ASC
        """, ("RM",))
        return {
            "overdue_invoices": overdue_inv,
            "overdue_supplier_invoices": overdue_sup,
            "deals_in_forhandling": forhandling,
            "negative_tb1_projects": neg_tb1,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Planner Tasks
# ============================================================================

@router.get("/api/planner/tasks")
async def planner_tasks(request: Request):
    """Active Planner tasks with urgency classification."""
    user = await _get_current_user(request)
    company = _cc(request, user)
    try:
        role = user.get("role", "")
        planner_email = user.get("planner_email", "")

        conds = ["percent_complete < 100", "company_code = %s"]
        params: list = [company]

        if role != "vd" and planner_email:
            conds.append("assignee_email ILIKE %s")
            params.append(f"%{planner_email}%")
        elif role != "vd" and not planner_email:
            conds.append("1=0")

        tasks = query_dicts(
            f"""SELECT title, assignee_name, bucket_name, plan_name,
                       percent_complete, due_date::text, priority,
                       CASE
                           WHEN due_date < CURRENT_DATE AND percent_complete < 100 THEN 'overdue'
                           WHEN due_date = CURRENT_DATE THEN 'today'
                           WHEN due_date <= CURRENT_DATE + 7 THEN 'this_week'
                           ELSE 'later'
                       END as urgency
                FROM planner_task
                WHERE {' AND '.join(conds)}
                ORDER BY
                    CASE WHEN due_date IS NULL THEN 1 ELSE 0 END,
                    due_date ASC, priority DESC""",
            tuple(params)
        )
        return {
            "count": len(tasks),
            "overdue": len([t for t in tasks if t.get("urgency") == "overdue"]),
            "today": len([t for t in tasks if t.get("urgency") == "today"]),
            "this_week": len([t for t in tasks if t.get("urgency") == "this_week"]),
            "tasks": tasks,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Recurring Templates (Årshjul)
# ============================================================================

@router.get("/api/hub/recurring")
async def get_recurring_templates(request: Request, active_only: bool = True):
    """Lista alla recurring templates. VD ser alla, övriga ser sina."""
    user = await _get_current_user(request)

    conds = ["company_code = %s"]
    params: list = ["RM"]

    if active_only:
        conds.append("active = true")

    role = user.get("role", "")
    email = user.get("email", "")
    is_vd = role == "vd" or _has_perm(user, "admin.config")

    if not is_vd and email:
        conds.append("(assignee_email = %s OR backup_email = %s)")
        params.extend([email, email])

    rows = query_dicts(
        f"""SELECT id, title, description, process_area, frequency, rrule,
                   deadline_rule, deadline_offset_days, deadline_anchor,
                   assignee_email, backup_email, priority, task_type, context,
                   planner_plan_id, lookahead_days, active,
                   next_generate_at::text, last_generated_at::text,
                   created_at::text, updated_at::text
            FROM recurring_template
            WHERE {' AND '.join(conds)}
            ORDER BY process_area, frequency, title""",
        tuple(params)
    )
    return {"ok": True, "templates": rows}


@router.post("/api/hub/recurring")
async def create_recurring_template(request: Request):
    """Skapa ny recurring template (VD-only)."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.config")
    body = await request.json()

    title = body.get("title")
    if not title:
        raise HTTPException(status_code=400, detail="title krävs")

    new_id = execute(
        """INSERT INTO recurring_template
           (company_code, title, description, process_area, frequency, rrule,
            deadline_rule, deadline_offset_days, deadline_anchor,
            assignee_email, backup_email, priority, task_type, context,
            planner_plan_id, lookahead_days, active)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true)
           RETURNING id""",
        (
            "RM", title,
            body.get("description"),
            body.get("process_area", "Ovrigt"),
            body.get("frequency", "monthly"),
            body.get("rrule", ""),
            body.get("deadline_rule"),
            body.get("deadline_offset_days", 0),
            body.get("deadline_anchor", "period_start"),
            body.get("assignee_email"),
            body.get("backup_email"),
            body.get("priority", 2),
            body.get("task_type", "action"),
            body.get("context"),
            body.get("planner_plan_id"),
            body.get("lookahead_days", 7),
        ),
        returning=True
    )
    if new_id is None:
        raise HTTPException(status_code=500, detail="Kunde inte skapa template")

    _audit_log(user["username"], "recurring.create", "recurring_template", new_id, title)
    return {"ok": True, "id": new_id}


@router.patch("/api/hub/recurring/{template_id}")
async def update_recurring_template(template_id: int, request: Request):
    """Uppdatera en recurring template (VD-only)."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.config")
    body = await request.json()

    allowed = [
        "title", "description", "process_area", "frequency", "rrule",
        "deadline_rule", "deadline_offset_days", "deadline_anchor",
        "assignee_email", "backup_email", "priority", "task_type",
        "context", "planner_plan_id", "lookahead_days", "active"
    ]
    sets = []
    params = []
    for field in allowed:
        if field in body:
            sets.append(f"{field} = %s")
            params.append(body[field])

    if not sets:
        raise HTTPException(status_code=400, detail="Inget att uppdatera")

    sets.append("updated_at = NOW()")
    params.append(template_id)

    execute(
        f"UPDATE recurring_template SET {', '.join(sets)} WHERE id = %s",
        tuple(params)
    )
    _audit_log(user["username"], "recurring.update", "recurring_template", template_id, str(body))
    return {"ok": True, "id": template_id}


@router.delete("/api/hub/recurring/{template_id}")
async def deactivate_recurring_template(template_id: int, request: Request):
    """Soft-delete / pausa en recurring template (VD-only)."""
    user = await _get_current_user(request)
    _require_perm(user, "admin.config")

    execute(
        "UPDATE recurring_template SET active = false, updated_at = NOW() WHERE id = %s",
        (template_id,)
    )
    _audit_log(user["username"], "recurring.deactivate", "recurring_template", template_id)
    return {"ok": True, "id": template_id, "active": False}


# ============================================================================
# Daily Checklist
# ============================================================================

@router.get("/api/hub/checklist/today")
async def get_today_checklist(request: Request):
    """Hämta dagens checklista. VD ser alla, övriga ser sina."""
    user = await _get_current_user(request)

    role = user.get("role", "")
    email = user.get("email", "")
    is_vd = role == "vd" or _has_perm(user, "admin.config")

    conds = ["cl.check_date = CURRENT_DATE"]
    params = []

    if not is_vd and email:
        conds.append("(rt.assignee_email = %s OR rt.backup_email = %s)")
        params.extend([email, email])

    rows = query_dicts(
        f"""SELECT cl.id, cl.template_id, cl.check_date::text, cl.status,
                   cl.checked_by, cl.checked_at::text, cl.comment,
                   cl.current_escalation_level, cl.escalated_at::text,
                   rt.title, rt.description, rt.process_area,
                   rt.assignee_email, rt.backup_email, rt.context
            FROM daily_checklist_log cl
            JOIN recurring_template rt ON rt.id = cl.template_id
            WHERE {' AND '.join(conds)}
            ORDER BY rt.process_area, rt.title""",
        tuple(params) if params else None
    )

    # Markera items som är eskalerade till inloggad användare (backup-vy)
    from datetime import datetime as _dt
    current_hour = _dt.now().hour
    for r in rows:
        r["is_backup_item"] = (
            r.get("backup_email") == email
            and r.get("assignee_email") != email
            and r.get("status") == "pending"
            and current_hour >= 11
        )
        r["is_manager_escalation"] = (
            r.get("current_escalation_level", 0) >= 2
        )

    total = len(rows)
    done = sum(1 for r in rows if r.get("status") == "done")
    escalated = sum(1 for r in rows if (r.get("current_escalation_level") or 0) > 0)
    return {
        "ok": True,
        "date": str(date.today()),
        "items": rows,
        "total": total,
        "done": done,
        "pending": total - done,
        "escalated": escalated,
    }


@router.patch("/api/hub/checklist/{item_id}")
async def update_checklist_item(item_id: int, request: Request):
    """Bocka av / ångra en checklist-post."""
    user = await _get_current_user(request)
    body = await request.json()
    new_status = body.get("status", "done")

    if new_status == "done":
        execute(
            """UPDATE daily_checklist_log
               SET status = 'done', checked_by = %s, checked_at = NOW()
               WHERE id = %s""",
            (user.get("email", user["username"]), item_id)
        )
    else:
        execute(
            """UPDATE daily_checklist_log
               SET status = 'pending', checked_by = NULL, checked_at = NULL
               WHERE id = %s""",
            (item_id,)
        )

    return {"ok": True, "id": item_id, "status": new_status}


@router.get("/api/hub/checklist/history")
async def get_checklist_history(request: Request, days: int = 14):
    """Historik över checklist-poster, default 14 dagar."""
    user = await _get_current_user(request)

    rows = query_dicts(
        """SELECT cl.id, cl.template_id, cl.check_date::text, cl.status,
                  cl.checked_by, cl.checked_at::text, cl.comment,
                  rt.title, rt.process_area, rt.assignee_email
           FROM daily_checklist_log cl
           JOIN recurring_template rt ON rt.id = cl.template_id
           WHERE cl.check_date >= CURRENT_DATE - %s
           ORDER BY cl.check_date DESC, rt.process_area, rt.title""",
        (days,)
    )
    return {"ok": True, "items": rows, "days": days}


@router.get("/api/hub/checklist/stats")
async def get_checklist_stats(request: Request):
    """Aggregerad statistik per person senaste 30 dagarna."""
    user = await _get_current_user(request)

    rows = query_dicts(
        """SELECT rt.assignee_email,
                  COUNT(*) FILTER (WHERE cl.status = 'done') AS done,
                  COUNT(*) FILTER (WHERE cl.status = 'pending') AS pending,
                  COUNT(*) FILTER (WHERE cl.status = 'missed') AS missed,
                  COUNT(*) AS total
           FROM daily_checklist_log cl
           JOIN recurring_template rt ON rt.id = cl.template_id
           WHERE cl.check_date >= CURRENT_DATE - 30
           GROUP BY rt.assignee_email
           ORDER BY rt.assignee_email"""
    )
    return {"ok": True, "stats": rows, "period_days": 30}


@router.get("/api/hub/checklist/escalations")
async def get_checklist_escalations(request: Request, days: int = 7):
    """Eskaleringsöversikt. VD ser alla, övriga ser sina."""
    user = await _get_current_user(request)
    email = user.get("email", "")
    is_vd = _has_perm(user, "admin.config")

    conds = ["ee.triggered_at >= CURRENT_DATE - %s"]
    params = [days]

    if not is_vd and email:
        conds.append("ee.recipient_email = %s")
        params.append(email)

    rows = query_dicts(
        f"""SELECT ee.id, ee.checklist_log_id, ee.escalation_level,
                   ee.recipient_email, ee.channel,
                   ee.triggered_at::text, ee.acknowledged_at::text,
                   rt.title, rt.assignee_email, rt.process_area,
                   cl.check_date::text, cl.status AS checklist_status
            FROM escalation_event ee
            JOIN daily_checklist_log cl ON cl.id = ee.checklist_log_id
            JOIN recurring_template rt ON rt.id = ee.template_id
            WHERE {' AND '.join(conds)}
            ORDER BY ee.triggered_at DESC""",
        tuple(params)
    )

    summary = {"level_1": 0, "level_2": 0, "level_3": 0, "acknowledged": 0}
    for r in rows:
        lvl = r.get("escalation_level", 0)
        if lvl == 1: summary["level_1"] += 1
        elif lvl == 2: summary["level_2"] += 1
        elif lvl == 3: summary["level_3"] += 1
        if r.get("acknowledged_at"):
            summary["acknowledged"] += 1

    return {"ok": True, "escalations": rows, "summary": summary, "days": days}


@router.patch("/api/hub/checklist/escalation/{esc_id}/acknowledge")
async def acknowledge_escalation(esc_id: int, request: Request):
    """Markera en eskalering som kvitterad."""
    user = await _get_current_user(request)
    execute(
        "UPDATE escalation_event SET acknowledged_at = NOW() WHERE id = %s AND acknowledged_at IS NULL",
        (esc_id,)
    )
    return {"ok": True, "id": esc_id}
