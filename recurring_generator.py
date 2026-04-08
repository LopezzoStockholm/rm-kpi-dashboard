#!/usr/bin/env python3
"""Recurring task generator — skapar rm_task-instanser från recurring_template.

Dagliga templates → daglig checklista (daily_checklist_log), INTE rm_task.
Övriga → rm_task-instanser med deadline, flödar vidare via befintliga synkmotorer.

Cron: 0 6 * * * python3 /opt/rm-infra/recurring_generator.py >> /var/log/recurring_generator.log 2>&1
"""
import subprocess, sys, uuid, hashlib
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrulestr

try:
    from dateutil.rrule import rrulestr
    from dateutil.relativedelta import relativedelta
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-dateutil", "-q", "--break-system-packages"])
    from dateutil.rrule import rrulestr
    from dateutil.relativedelta import relativedelta

DB = "rm_central"
USER = "rmadmin"
DRY_RUN = "--dry-run" in sys.argv
LOG_PREFIX = "recurring_gen"

def psql(query, db=DB):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", USER, "-d", db, "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    if r.returncode != 0 and r.stderr.strip():
        print(f"  DB error: {r.stderr.strip()[:200]}")
    return r.stdout.strip()

def psql_rows(query, db=DB):
    raw = psql(query, db)
    if not raw:
        return []
    return [line.split("|") for line in raw.split("\n") if line.strip()]

def esc(s):
    if s is None: return "NULL"
    return "'" + str(s).replace("'", "''") + "'"

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {LOG_PREFIX}: {msg}")


def calculate_due_date(template, period_start):
    """Beräkna due_date baserat på template deadline-regler."""
    offset = int(template.get("deadline_offset_days") or 0)
    anchor = template.get("deadline_anchor", "period_start")
    freq = template.get("frequency", "")

    if anchor == "period_end":
        # Beräkna period-slut
        if freq == "monthly":
            period_end = period_start + relativedelta(months=1) - timedelta(days=1)
        elif freq == "quarterly":
            period_end = period_start + relativedelta(months=3) - timedelta(days=1)
        elif freq == "halfyear":
            period_end = period_start + relativedelta(months=6) - timedelta(days=1)
        elif freq == "yearly":
            period_end = period_start + relativedelta(years=1) - timedelta(days=1)
        else:
            period_end = period_start + timedelta(days=6)  # weekly
        return period_end + timedelta(days=offset)
    else:
        # period_start + offset
        if offset > 0:
            return period_start + timedelta(days=offset - 1)  # "den 12:e" = dag 12
        return period_start + timedelta(days=6) if freq == "weekly" else period_start


def calculate_next_generate(template, current_next):
    """Beräkna nästa genereringsdatum baserat på RRULE."""
    freq = template.get("frequency", "")
    today = date.today()

    if freq == "daily":
        return today + timedelta(days=1)
    elif freq == "weekly":
        return today + timedelta(days=7)
    elif freq == "monthly":
        return today + relativedelta(months=1)
    elif freq == "quarterly":
        return today + relativedelta(months=3)
    elif freq == "halfyear":
        return today + relativedelta(months=6)
    elif freq == "yearly":
        return today + relativedelta(years=1)
    return today + timedelta(days=30)


def generate_daily_checklists():
    """Skapa dagens checklist-poster för dagliga templates."""
    today = date.today()
    weekday = today.weekday()  # 0=mån, 6=sön

    # Skippa helger
    if weekday >= 5:
        log("Helg — skippar daglig checklista")
        return 0

    rows = psql_rows("""
        SELECT id, title, assignee_email
        FROM recurring_template
        WHERE active = true AND frequency = 'daily' AND company_code = 'RM'
        ORDER BY id
    """)

    created = 0
    for r in rows:
        template_id = r[0]
        title = r[1]

        # Kolla om redan finns för idag
        existing = psql(f"""
            SELECT id FROM daily_checklist_log
            WHERE template_id = {template_id} AND check_date = '{today}'
        """)
        if existing:
            continue

        if DRY_RUN:
            log(f"  DRY-RUN: Would create checklist for '{title[:50]}'")
            created += 1
            continue

        psql(f"""
            INSERT INTO daily_checklist_log (template_id, check_date, status)
            VALUES ({template_id}, '{today}', 'pending')
            ON CONFLICT (template_id, check_date) DO NOTHING
        """)
        created += 1

    return created


def generate_task_instances():
    """Skapa rm_task-instanser för icke-dagliga templates."""
    today = date.today()

    rows = psql_rows("""
        SELECT id, title, description, process_area, frequency,
               deadline_offset_days, deadline_anchor,
               assignee_email, backup_email, priority,
               context, planner_plan_id, lookahead_days,
               next_generate_at::text, deadline_rule
        FROM recurring_template
        WHERE active = true
          AND frequency != 'daily'
          AND company_code = 'RM'
          AND (next_generate_at IS NULL OR next_generate_at <= CURRENT_DATE + lookahead_days)
        ORDER BY id
    """)

    keys = ["id", "title", "description", "process_area", "frequency",
            "deadline_offset_days", "deadline_anchor",
            "assignee_email", "backup_email", "priority",
            "context", "planner_plan_id", "lookahead_days",
            "next_generate_at", "deadline_rule"]

    templates = [dict(zip(keys, [c if c else None for c in r])) for r in rows]

    created = 0
    skipped = 0
    for t in templates:
        template_id = t["id"]
        next_gen = t.get("next_generate_at")

        # Beräkna period_start (= next_generate_at eller idag)
        if next_gen:
            try:
                period_start = date.fromisoformat(next_gen)
            except (ValueError, TypeError):
                period_start = today
        else:
            period_start = today

        # Dedup: finns redan en öppen task med denna template som källa?
        existing = psql(f"""
            SELECT id FROM rm_task
            WHERE external_system = 'recurring'
              AND external_id = '{template_id}'
              AND status NOT IN ('done', 'wontdo')
            LIMIT 1
        """)
        if existing:
            skipped += 1
            continue

        # Beräkna due_date
        due_date = calculate_due_date(t, period_start)

        # Skapa rm_task-instans
        task_id = str(uuid.uuid4())
        title = t["title"]
        prio = int(t.get("priority") or 2)
        context = t.get("context")
        assignee = t.get("assignee_email")
        process = t.get("process_area")

        if DRY_RUN:
            log(f"  DRY-RUN: Would create task '{title[:50]}' due={due_date} for={assignee}")
            created += 1
            # Uppdatera next_generate_at i dry-run mode
            continue

        psql(f"""
            INSERT INTO rm_task
              (id, company_code, title, description, status, board_column,
               assignee_email, priority, context, due_date,
               source, external_system, external_id, created_by,
               project_name, task_type)
            VALUES
              ('{task_id}', 'RM', {esc(title)}, {esc(t.get('description'))},
               'open', 'inbox',
               {esc(assignee)}, {prio}, {esc(context)}, '{due_date}',
               'recurring', 'recurring', '{template_id}', 'recurring-generator',
               {esc(process)}, 'action')
        """)

        # Uppdatera next_generate_at
        next_date = calculate_next_generate(t, period_start)
        psql(f"""
            UPDATE recurring_template
            SET next_generate_at = '{next_date}', last_generated_at = '{today}', updated_at = NOW()
            WHERE id = {template_id}
        """)

        log(f"  CREATED: {title[:50]} due={due_date} → {assignee or 'unassigned'}")
        created += 1

    return created, skipped


def escalate_missed_daily():
    """Flagga missade dagliga checklist-poster från gårdagen."""
    yesterday = date.today() - timedelta(days=1)
    weekday = yesterday.weekday()

    # Skippa lördag/söndag
    if weekday >= 5:
        return 0

    # Hitta ogjorda poster från gårdagen
    rows = psql_rows(f"""
        SELECT cl.id, rt.title, rt.assignee_email, rt.backup_email
        FROM daily_checklist_log cl
        JOIN recurring_template rt ON rt.id = cl.template_id
        WHERE cl.check_date = '{yesterday}' AND cl.status = 'pending'
    """)

    escalated = 0
    for r in rows:
        cl_id = r[0]
        title = r[1]
        assignee = r[2]
        backup = r[3]

        if not DRY_RUN:
            psql(f"UPDATE daily_checklist_log SET status = 'missed' WHERE id = {cl_id}")

            # Skapa notifiering via befintlig notification-tabell
            psql(f"""
                INSERT INTO dashboard_notification
                  (user_email, type, title, body, severity, source, created_at)
                VALUES
                  ({esc(assignee)}, 'recurring_missed',
                   {esc(f'Missad rutin: {title[:60]}')},
                   {esc(f'Daglig rutin ej utförd igår ({yesterday}). Backup: {backup or "ingen"}.')},
                   'warning', 'recurring_generator', NOW())
                ON CONFLICT DO NOTHING
            """)

        log(f"  MISSED: {title[:50]} ({assignee}) — notifiering skickad")
        escalated += 1

    return escalated


def main():
    log("═══ START ═══")
    if DRY_RUN:
        log("DRY-RUN MODE")

    # 1. Daglig checklista
    daily = generate_daily_checklists()
    log(f"Daglig checklista: {daily} poster skapade")

    # 2. Task-instanser (vecko/månad/kvartal/halvår/år)
    created, skipped = generate_task_instances()
    log(f"Task-instanser: {created} skapade, {skipped} redan öppna")

    # 3. Eskalera missade dagliga
    escalated = escalate_missed_daily()
    if escalated:
        log(f"Eskalering: {escalated} missade dagliga rutiner")

    log("═══ KLART ═══")


if __name__ == "__main__":
    main()
