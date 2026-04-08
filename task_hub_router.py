"""
task_hub_router.py — Task Hub kanban: board, CRUD, blockers, config, ingest.
Återskapad 2026-04-08 efter att endpoints försvann vid modulrefaktorering.
"""

import logging
import re
from typing import Optional
from datetime import date, datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from rm_data import query_dicts, query_one, execute

logger = logging.getLogger(__name__)

router = APIRouter(tags=["task_hub"])

_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_task_hub_router(get_current_user, has_perm, require_perm, audit_log, get_company_code_fn=None):
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user
    _has_perm = has_perm
    _require_perm = require_perm
    _audit_log = audit_log
    _get_company_code = get_company_code_fn


def _cc(request, user):
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"


def _log_activity(task_id, action, field, old_val, new_val, actor, cc="RM"):
    try:
        execute("""
            INSERT INTO task_activity (task_id, action, field, old_value, new_value, actor, company_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (task_id, action, field, str(old_val) if old_val is not None else None,
              str(new_val) if new_val is not None else None, actor, cc))
    except Exception as e:
        logger.warning("task_activity log failed: %s", e)


# ============================================================================
# Pydantic models
# ============================================================================

class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    task_type: Optional[str] = "action"
    context: Optional[str] = None
    priority: Optional[int] = 2
    assignee_name: Optional[str] = None
    assignee_email: Optional[str] = None
    project_code: Optional[str] = None
    project_name: Optional[str] = None
    deal_id: Optional[str] = None
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    contact_name: Optional[str] = None
    board_column: Optional[str] = "inbox"
    due_date: Optional[str] = None
    source: Optional[str] = "manual"


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    task_type: Optional[str] = None
    context: Optional[str] = None
    priority: Optional[int] = None
    assignee_name: Optional[str] = None
    assignee_email: Optional[str] = None
    project_code: Optional[str] = None
    project_name: Optional[str] = None
    deal_id: Optional[str] = None
    company_id: Optional[str] = None
    company_name: Optional[str] = None
    contact_name: Optional[str] = None
    board_column: Optional[str] = None
    due_date: Optional[str] = None


class MoveTask(BaseModel):
    board_column: str
    sort_order: Optional[int] = None


class StatusChange(BaseModel):
    status: str


class IngestTask(BaseModel):
    title: str
    source: Optional[str] = "api"
    source_message_id: Optional[str] = None
    sender_phone: Optional[str] = None
    assignee_name: Optional[str] = None
    assignee_email: Optional[str] = None
    project_code: Optional[str] = None
    project_name: Optional[str] = None
    board_column: Optional[str] = "inbox"
    task_type: Optional[str] = "action"
    context: Optional[str] = None
    priority: Optional[int] = 2
    description: Optional[str] = None
    company_name: Optional[str] = None
    contact_name: Optional[str] = None


# ============================================================================
# TASK columns SELECT (reusable)
# ============================================================================

TASK_SELECT = """
    t.id, t.title, t.description, t.task_type, t.context, t.priority,
    t.assignee_name, t.assignee_email, t.created_by,
    t.project_code, t.project_name, t.deal_id::text,
    t.company_id::text, t.company_name, t.contact_name,
    t.twenty_task_id::text, t.planner_task_id, t.planner_plan_id,
    t.status, t.board_column, t.sort_order,
    t.due_date::text as due_date, t.created_at::text, t.completed_at::text,
    t.source, t.external_id,
    (SELECT COUNT(*) FROM rm_task sub WHERE sub.parent_task_id = t.id) as subtask_count,
    (SELECT COUNT(*) FROM rm_task sub WHERE sub.parent_task_id = t.id AND sub.status = 'done') as subtask_done,
    (SELECT COUNT(*) FROM task_blocker b WHERE b.task_id = t.id AND b.status = 'active') as active_blockers,
    (SELECT COUNT(*) FROM task_blocker b WHERE b.task_id = t.id AND b.status = 'active' AND b.escalated = true) as escalated_blockers
"""


# ============================================================================
# GET /api/hub/tasks/board
# ============================================================================

@router.get("/api/hub/tasks/board")
async def get_board(request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)

    columns = query_dicts(
        "SELECT column_key, display_name, icon, wip_limit, sort_order FROM board_column_config WHERE company_code = %s AND active = true ORDER BY sort_order",
        (cc,))

    assignee_filter = ""
    params = [cc]
    if not _has_perm(user, "tasks.read_all"):
        email = user.get("email", "")
        assignee_filter = "AND (t.assignee_email = %s OR t.assignee_email IS NULL OR t.created_by = %s)"
        params.extend([email, email])

    tasks = query_dicts(f"""
        SELECT {TASK_SELECT}
        FROM rm_task t
        WHERE t.company_code = %s AND t.status != 'wontdo' AND t.parent_task_id IS NULL
        {assignee_filter}
        ORDER BY t.sort_order, t.created_at DESC
    """, tuple(params))

    board = {}
    for col in columns:
        key = col["column_key"]
        col_tasks = [t for t in tasks if t["board_column"] == key]
        board[key] = {"column": col, "tasks": col_tasks}

    return {"ok": True, "columns": columns, "board": board}


# ============================================================================
# POST /api/hub/tasks — Create
# ============================================================================

@router.post("/api/hub/tasks")
async def create_task(body: TaskCreate, request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)
    _require_perm(user, "tasks.write")

    result = query_dicts("""
        INSERT INTO rm_task (company_code, title, description, task_type, context, priority,
            assignee_name, assignee_email, project_code, project_name, deal_id,
            company_id, company_name, contact_name, board_column, due_date, source, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id, title, board_column, status
    """, (cc, body.title, body.description, body.task_type, body.context, body.priority,
          body.assignee_name, body.assignee_email, body.project_code, body.project_name,
          body.deal_id, body.company_id, body.company_name, body.contact_name,
          body.board_column or "inbox", body.due_date, body.source or "manual",
          user.get("email", "")), returning=True)

    if result:
        _log_activity(result[0]["id"], "created", None, None, body.title, user.get("username", ""), cc)
        _audit_log(user.get("username", ""), "create_task", "rm_task", str(result[0]["id"]), body.title)
    return {"ok": True, "task": result[0] if result else None}


# ============================================================================
# PUT /api/hub/tasks/{task_id} — Update
# ============================================================================

@router.put("/api/hub/tasks/{task_id}")
async def update_task(task_id: str, body: TaskUpdate, request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)
    _require_perm(user, "tasks.write")

    existing = query_dicts("SELECT * FROM rm_task WHERE id = %s AND company_code = %s", (task_id, cc))
    if not existing:
        raise HTTPException(404, "Task not found")
    old = existing[0]

    sets, vals = [], []
    for field in ["title", "description", "task_type", "context", "priority",
                  "assignee_name", "assignee_email", "project_code", "project_name",
                  "deal_id", "company_id", "company_name", "contact_name", "board_column", "due_date"]:
        new_val = getattr(body, field, None)
        if new_val is not None and str(new_val) != str(old.get(field, "")):
            sets.append(f"{field} = %s")
            vals.append(new_val)
            _log_activity(task_id, "updated", field, old.get(field), new_val, user.get("username", ""), cc)

    if not sets:
        return {"ok": True, "changed": 0}

    sets.append("updated_at = NOW()")
    vals.extend([task_id, cc])
    execute(f"UPDATE rm_task SET {', '.join(sets)} WHERE id = %s AND company_code = %s", tuple(vals))
    return {"ok": True, "changed": len(sets) - 1}


# ============================================================================
# PATCH /api/hub/tasks/{task_id}/move
# ============================================================================

@router.patch("/api/hub/tasks/{task_id}/move")
async def move_task(task_id: str, body: MoveTask, request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)

    old = query_dicts("SELECT board_column FROM rm_task WHERE id = %s AND company_code = %s", (task_id, cc))
    if not old:
        raise HTTPException(404, "Task not found")

    sort = body.sort_order if body.sort_order is not None else 0
    execute("UPDATE rm_task SET board_column = %s, sort_order = %s, updated_at = NOW() WHERE id = %s AND company_code = %s",
            (body.board_column, sort, task_id, cc))

    if body.board_column == "done":
        execute("UPDATE rm_task SET status = 'done', completed_at = NOW() WHERE id = %s", (task_id,))

    _log_activity(task_id, "moved", "board_column", old[0]["board_column"], body.board_column, user.get("username", ""), cc)
    return {"ok": True}


# ============================================================================
# PATCH /api/hub/tasks/{task_id}/status
# ============================================================================

@router.patch("/api/hub/tasks/{task_id}/status")
async def change_status(task_id: str, body: StatusChange, request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)

    old = query_dicts("SELECT status, board_column FROM rm_task WHERE id = %s AND company_code = %s", (task_id, cc))
    if not old:
        raise HTTPException(404, "Task not found")

    extra = ""
    if body.status == "done":
        extra = ", completed_at = NOW(), board_column = 'done'"
    elif body.status == "open" and old[0]["board_column"] == "done":
        extra = ", completed_at = NULL, board_column = 'inbox'"

    execute(f"UPDATE rm_task SET status = %s, updated_at = NOW() {extra} WHERE id = %s AND company_code = %s",
            (body.status, task_id, cc))
    _log_activity(task_id, "status_changed", "status", old[0]["status"], body.status, user.get("username", ""), cc)
    return {"ok": True}


# ============================================================================
# GET /api/hub/tasks/waiting — Active blockers
# ============================================================================

@router.get("/api/hub/tasks/waiting")
async def get_waiting(request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)

    items = query_dicts("""
        SELECT b.id as blocker_id, b.task_id, b.blocker_type,
               b.waiting_for_name, b.waiting_for_email, b.waiting_for_org,
               b.follow_up_date::text, b.follow_up_count, b.escalated, b.note,
               b.waiting_since::text,
               EXTRACT(DAY FROM NOW() - b.waiting_since)::int as days_waiting,
               t.title as task_title, t.project_code, t.project_name
        FROM task_blocker b
        JOIN rm_task t ON t.id = b.task_id
        WHERE b.status = 'active' AND t.company_code = %s
        ORDER BY b.follow_up_date NULLS LAST, b.waiting_since
    """, (cc,))
    return {"ok": True, "items": items}


# ============================================================================
# GET /api/hub/blockers/summary
# ============================================================================

@router.get("/api/hub/blockers/summary")
async def blockers_summary(request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)

    row = query_one("""
        SELECT
            COUNT(*) FILTER (WHERE b.status = 'active') as active_count,
            COUNT(*) FILTER (WHERE b.status = 'active' AND b.follow_up_date < CURRENT_DATE) as overdue_count,
            COUNT(*) FILTER (WHERE b.status = 'active' AND b.escalated = true) as escalated_count,
            COUNT(*) FILTER (WHERE b.status = 'active' AND b.follow_up_date BETWEEN CURRENT_DATE AND CURRENT_DATE + 3) as due_soon_count,
            COUNT(*) FILTER (WHERE b.status = 'resolved' AND b.resolved_at >= date_trunc('week', CURRENT_DATE)) as resolved_week
        FROM task_blocker b
        JOIN rm_task t ON t.id = b.task_id
        WHERE t.company_code = %s
    """, (cc,))

    return {"ok": True, "summary": dict(row) if row else {
        "active_count": 0, "overdue_count": 0, "escalated_count": 0,
        "due_soon_count": 0, "resolved_week": 0
    }}


# ============================================================================
# PATCH /api/hub/blockers/{blocker_id}/resolve
# ============================================================================

@router.patch("/api/hub/blockers/{blocker_id}/resolve")
async def resolve_blocker(blocker_id: str, request: Request):
    user = await _get_current_user(request)
    execute("UPDATE task_blocker SET status = 'resolved', resolved_at = NOW() WHERE id = %s", (blocker_id,))
    return {"ok": True}


# ============================================================================
# PATCH /api/hub/blockers/{blocker_id}/follow-up
# ============================================================================

@router.patch("/api/hub/blockers/{blocker_id}/follow-up")
async def follow_up_blocker(blocker_id: str, request: Request):
    user = await _get_current_user(request)
    execute("""
        UPDATE task_blocker
        SET follow_up_count = follow_up_count + 1, last_follow_up_at = NOW()
        WHERE id = %s
    """, (blocker_id,))
    return {"ok": True}


# ============================================================================
# Config endpoints
# ============================================================================

@router.get("/api/hub/config/task-types")
async def get_task_types(request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)
    types = query_dicts(
        "SELECT type_key, display_name, icon, default_context FROM task_type_config WHERE company_code = %s AND active = true ORDER BY sort_order",
        (cc,))
    return {"ok": True, "types": types}


@router.get("/api/hub/config/contexts")
async def get_contexts(request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)
    ctxs = query_dicts(
        "SELECT context_key, display_name, icon FROM context_config WHERE company_code = %s AND active = true ORDER BY sort_order",
        (cc,))
    return {"ok": True, "contexts": ctxs}


@router.get("/api/hub/config/projects")
async def get_projects(request: Request):
    user = await _get_current_user(request)
    cc = _cc(request, user)
    projs = query_dicts(
        "SELECT DISTINCT project_number as project_code, project_name FROM project_profitability WHERE company_code = %s ORDER BY project_name",
        (cc,))
    return {"ok": True, "projects": projs}


@router.get("/api/hub/config/team")
async def get_team(request: Request):
    user = await _get_current_user(request)
    members = query_dicts("""
        SELECT u.display_name, u.email,
               COALESCE(r.system_name, 'unknown') as role
        FROM portal_user u
        LEFT JOIN rm_role r ON r.id = u.role_id
        WHERE u.active = true
        ORDER BY u.display_name
    """)
    return {"ok": True, "members": members}


# ============================================================================
# POST /api/hub/tasks/ingest — External ingest (WhatsApp, Teams, email, n8n)
# ============================================================================

def _normalize_phone(phone: str) -> str:
    digits = re.sub(r'\D', '', phone)
    if digits.startswith('46') and len(digits) >= 11:
        return '0' + digits[2:]
    if digits.startswith('0') and len(digits) == 10:
        return digits
    return digits


@router.post("/api/hub/tasks/ingest")
async def ingest_task(body: IngestTask, request: Request):
    # Auth: accept both Bearer internal token and portal token
    auth = request.headers.get("authorization", "")
    portal_token = request.headers.get("x-portal-token", "")

    if not (auth == "Bearer rm-internal-2026" or portal_token):
        raise HTTPException(401, "Missing auth")

    cc = "RM"

    # Dedup
    if body.source_message_id and body.source:
        existing = query_dicts(
            "SELECT id FROM rm_task WHERE external_system = %s AND external_id = %s",
            (body.source, body.source_message_id))
        if existing:
            return {"ok": True, "dedup": True, "task_id": str(existing[0]["id"])}

    # Resolve sender phone to user
    created_by = body.assignee_email or ""
    assignee_name = body.assignee_name
    assignee_email = body.assignee_email

    if body.sender_phone and not assignee_email:
        norm = _normalize_phone(body.sender_phone)
        user_match = query_dicts("""
            SELECT display_name, email FROM portal_user
            WHERE active = true AND (
                phone LIKE %s OR phone LIKE %s OR phone LIKE %s
            ) LIMIT 1
        """, (f"%{norm[-8:]}%", f"%{norm}%", f"%{body.sender_phone}%"))
        if user_match:
            assignee_name = user_match[0]["display_name"]
            assignee_email = user_match[0]["email"]
            created_by = assignee_email

    # Determine board column
    board_col = body.board_column or "inbox"
    if assignee_email and body.sender_phone:
        sender_norm = _normalize_phone(body.sender_phone)
        sender_user = query_dicts("SELECT email FROM portal_user WHERE phone LIKE %s LIMIT 1", (f"%{sender_norm[-8:]}%",))
        if sender_user and sender_user[0]["email"] != assignee_email:
            board_col = "waiting"

    result = query_dicts("""
        INSERT INTO rm_task (company_code, title, description, task_type, context, priority,
            assignee_name, assignee_email, project_code, project_name,
            company_name, contact_name, board_column, source,
            external_system, external_id, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id, title, board_column
    """, (cc, body.title, body.description, body.task_type, body.context, body.priority,
          assignee_name, assignee_email, body.project_code, body.project_name,
          body.company_name, body.contact_name, board_col, body.source,
          body.source, body.source_message_id, created_by), returning=True)

    task_id = str(result[0]["id"]) if result else None
    if task_id:
        _log_activity(task_id, "created", "source", None, body.source, created_by, cc)

    return {"ok": True, "task_id": task_id, "title": body.title, "board_column": board_col, "assignee": assignee_name}
