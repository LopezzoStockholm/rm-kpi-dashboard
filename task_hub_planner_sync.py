#!/usr/bin/env python3
"""Task Hub ↔ Microsoft Planner sync engine.

Gör rm_task till master och synkar bidirektionellt mot Planner via Graph API.

Flöde:
  1. INBOUND:  planner_task (redan synkad av planner_sync.py) → rm_task
  2. OUTBOUND: rm_task utan planner_task_id → skapa Planner-task via Graph API
  3. STATUS:   board_column ↔ percentComplete (bidirektionellt)

Routing (outbound):
  - keyword_plan_mapping:  title/context matchas mot nyckelord → plan
  - user_plan_mapping:     assignee user_id → plan
  - default_plan:          fallback

Cron: */5 * * * * python3 /opt/rm-infra/task_hub_planner_sync.py >> /var/log/task_hub_planner_sync.log 2>&1
"""
import subprocess, json, sys, os, uuid, hashlib
from datetime import datetime

try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "-q", "--break-system-packages"])
    import requests

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
CONFIG_FILE = "/opt/rm-infra/planner-config.json"
CENTRAL_DB = "rm_central"
DB_USER = "rmadmin"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
LOG_PREFIX = "planner_hub"
DRY_RUN = "--dry-run" in sys.argv

# board_column → percentComplete
COLUMN_TO_PERCENT = {
    "inbox": 0,
    "backlog": 0,
    "today": 50,
    "this_week": 50,
    "waiting": 50,
    "done": 100,
}

# percentComplete → board_column (inbound)
PERCENT_TO_COLUMN = {
    0: None,      # behåll befintlig
    50: None,     # behåll befintlig
    100: "done",
}


# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────
def psql(query, db=CENTRAL_DB):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", DB_USER, "-d", db, "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    if r.returncode != 0 and r.stderr.strip():
        print(f"  DB error ({db}): {r.stderr.strip()[:200]}")
    return r.stdout.strip()


def psql_rows(query, db=CENTRAL_DB):
    raw = psql(query, db)
    if not raw:
        return []
    rows = []
    for line in raw.split("\n"):
        if line.strip():
            rows.append(line.split("|"))
    return rows


def escape(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def checksum(*parts):
    return hashlib.md5("|".join(str(p) for p in parts).encode()).hexdigest()[:16]


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {LOG_PREFIX}: {msg}")


# ─────────────────────────────────────────────────────────────
# Graph API
# ─────────────────────────────────────────────────────────────
def get_access_token(config):
    url = f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def graph_get(token, path, params=None):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{GRAPH_BASE}{path}", headers=headers, params=params, timeout=15)
    if r.status_code != 200:
        log(f"  Graph GET {path} failed ({r.status_code}): {r.text[:200]}")
        return None
    return r.json()


def graph_post(token, path, body):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    r = requests.post(f"{GRAPH_BASE}{path}", headers=headers, json=body, timeout=15)
    if r.status_code not in (200, 201):
        log(f"  Graph POST {path} failed ({r.status_code}): {r.text[:300]}")
        return None
    return r.json()


def graph_patch(token, path, body, etag=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    if etag:
        headers["If-Match"] = etag
    r = requests.patch(f"{GRAPH_BASE}{path}", headers=headers, json=body, timeout=15)
    if r.status_code not in (200, 204):
        log(f"  Graph PATCH {path} failed ({r.status_code}): {r.text[:300]}")
        return False
    return True


# ─────────────────────────────────────────────────────────────
# User resolution: email → Graph user ID
# ─────────────────────────────────────────────────────────────
_user_cache = {}

def resolve_user_id(token, email):
    """Resolve email → Azure AD user ID."""
    if not email:
        return None
    if email in _user_cache:
        return _user_cache[email]

    data = graph_get(token, "/users", {"$filter": f"mail eq '{email}' or userPrincipalName eq '{email}'", "$select": "id,mail"})
    if data and data.get("value"):
        uid = data["value"][0]["id"]
        _user_cache[email] = uid
        return uid

    _user_cache[email] = None
    return None


# ─────────────────────────────────────────────────────────────
# Bucket resolution: plan → first bucket (or "To Do")
# ─────────────────────────────────────────────────────────────
_bucket_cache = {}

def get_default_bucket(token, plan_id):
    """Hämta default bucket (första, eller 'To Do' om den finns)."""
    if plan_id in _bucket_cache:
        return _bucket_cache[plan_id]

    data = graph_get(token, f"/planner/plans/{plan_id}/buckets")
    if not data or not data.get("value"):
        _bucket_cache[plan_id] = None
        return None

    buckets = data["value"]
    # Föredra "To Do" eller "Att göra"
    for b in buckets:
        if b["name"].lower() in ("to do", "att göra", "todo"):
            _bucket_cache[plan_id] = b["id"]
            return b["id"]

    # Fallback: första bucket
    _bucket_cache[plan_id] = buckets[0]["id"]
    return buckets[0]["id"]


# ─────────────────────────────────────────────────────────────
# Plan routing: task → vilken Planner-plan?
# ─────────────────────────────────────────────────────────────
def route_to_plan(task, config, token):
    """Bestäm vilken plan en task ska hamna i baserat på nyckelord/assignee."""
    keyword_map = config.get("keyword_plan_mapping", {})
    user_map = config.get("user_plan_mapping", {})
    default = config.get("default_plan", "")

    title = (task.get("title") or "").lower()
    context = (task.get("context") or "").lower()
    project = (task.get("project_name") or "").lower()
    search_text = f"{title} {context} {project}"

    # 1. Keyword-matchning (längsta nyckelord först för precision)
    sorted_keywords = sorted(keyword_map.keys(), key=len, reverse=True)
    for keyword in sorted_keywords:
        if keyword.lower() in search_text:
            plan_id = keyword_map[keyword]
            plan_name = config.get("plans", {}).get(plan_id, plan_id[:12])
            return plan_id, f"keyword:{keyword}→{plan_name}"

    # 2. User-baserad routing
    if task.get("assignee_email"):
        user_id = resolve_user_id(token, task["assignee_email"])
        if user_id and user_id in user_map:
            plan_id = user_map[user_id]
            plan_name = config.get("plans", {}).get(plan_id, plan_id[:12])
            return plan_id, f"user:{task['assignee_email']}→{plan_name}"

    # 3. Default
    plan_name = config.get("plans", {}).get(default, default[:12])
    return default, f"default→{plan_name}"


# ─────────────────────────────────────────────────────────────
# INBOUND: planner_task → rm_task
# ─────────────────────────────────────────────────────────────
def sync_inbound():
    """Importera Planner-tasks som saknar rm_task-koppling."""

    # Hämta alla planner_task_id:n som redan finns i rm_task
    existing = set()
    rows = psql_rows("SELECT planner_task_id FROM rm_task WHERE planner_task_id IS NOT NULL")
    for r in rows:
        existing.add(r[0])

    # Hämta befintliga titlar för dedup (title+assignee)
    existing_titles = set()
    title_rows = psql_rows("SELECT lower(title), COALESCE(assignee_email,'') FROM rm_task")
    for r in title_rows:
        existing_titles.add((r[0], r[1]))

    # Template-tasks att skippa
    SKIP_TITLES = {"create a plan", "assign tasks", "add new tasks", "add additional information to tasks",
                   "customize  buckets", "customize buckets"}

    # Hämta alla planner_task
    planner_rows = psql_rows("""
        SELECT task_id, plan_name, bucket_name, title,
               assignee_name, assignee_email, due_date::text,
               percent_complete, priority, plan_id
        FROM planner_task
        WHERE company_code = 'RM'
    """)

    imported = 0
    skipped = 0
    for row in planner_rows:
        task_id = row[0]
        if task_id in existing:
            skipped += 1
            continue

        title_raw = (row[3] or "").strip()
        assignee_email_raw = (row[5] or "").strip()

        # Skippa template-tasks
        if title_raw.lower().strip() in SKIP_TITLES:
            skipped += 1
            continue

        # Dedup: skippa om samma title+assignee redan finns
        dedup_key = (title_raw.lower(), assignee_email_raw)
        if dedup_key in existing_titles:
            skipped += 1
            continue
        existing_titles.add(dedup_key)

        plan_name = row[1]
        bucket_name = row[2]
        title = row[3] or "(ingen titel)"
        assignee_name = row[4] if row[4] else None
        assignee_email = row[5] if row[5] else None
        due_date = row[6] if row[6] else None
        percent_complete = int(row[7]) if row[7] else 0
        priority = int(row[8]) if row[8] else 2
        plan_id = row[9] if row[9] else None

        # Skippa klara tasks (percent_complete = 100)
        # Importera alla oavsett status — vi vill ha full bild
        # Men markera klara som done
        if percent_complete == 100:
            board_column = "done"
            rm_status = "done"
        elif percent_complete >= 50:
            board_column = "today"
            rm_status = "open"
        else:
            board_column = "inbox"
            rm_status = "open"

        # Prioritet: Planner 0=urgent 1=important 2=medium 3=low → rm_task 0=urgent 1=high 2=normal 3=low
        rm_priority = min(priority, 3)

        # Hantera multi-assignee (kommaseparerat)
        if assignee_email and "," in assignee_email:
            assignee_email = assignee_email.split(",")[0].strip()
        if assignee_name and "," in assignee_name:
            assignee_name = assignee_name.split(",")[0].strip()

        new_id = str(uuid.uuid4())
        hash_val = checksum(title, board_column, due_date)

        if DRY_RUN:
            log(f"  DRY-RUN: Would import '{title[:50]}' from {plan_name}")
            imported += 1
            continue

        # Due date SQL
        due_sql = f"'{due_date}'" if due_date else "NULL"

        psql(f"""
            INSERT INTO rm_task
              (id, company_code, title, status, board_column,
               assignee_email, assignee_name, priority,
               planner_task_id, planner_plan_id,
               sync_hash, last_synced_at,
               due_date, source, external_system, external_id,
               created_by, project_name)
            VALUES
              ('{new_id}', 'RM', {escape(title)}, '{rm_status}', '{board_column}',
               {escape(assignee_email)}, {escape(assignee_name)}, {rm_priority},
               {escape(task_id)}, {escape(plan_id)},
               {escape(hash_val)}, NOW(),
               {due_sql}, 'planner', 'planner', {escape(task_id)},
               'planner-sync', {escape(plan_name)})
        """)
        log(f"  IMPORTED: {title[:50]} ({plan_name} / {bucket_name})")
        imported += 1

    return imported, skipped


# ─────────────────────────────────────────────────────────────
# OUTBOUND: rm_task → Planner
# ─────────────────────────────────────────────────────────────
def sync_outbound(config, token):
    """Skapa Planner-tasks för rm_task som saknar planner_task_id."""

    # Hämta tasks utan planner-koppling
    rows = psql_rows("""
        SELECT id::text, title, description, board_column, status,
               assignee_email, assignee_name,
               context, project_name, due_date::text, task_type
        FROM rm_task
        WHERE planner_task_id IS NULL
          AND status NOT IN ('wontdo', 'done')
        ORDER BY created_at
    """)

    # Ladda planner_task-titlar för att undvika dubbletter
    planner_titles = set()
    pt_rows = psql_rows("SELECT lower(left(title, 30)) FROM planner_task WHERE company_code = 'RM'")
    for r in pt_rows:
        planner_titles.add(r[0].strip())

    if not rows:
        return 0

    keys = ["id", "title", "description", "board_column", "status",
            "assignee_email", "assignee_name",
            "context", "project_name", "due_date", "task_type"]
    tasks = [dict(zip(keys, [c if c else None for c in r])) for r in rows]

    created = 0
    for task in tasks:
        # Kontrollera om liknande task redan finns i Planner (via planner_task)
        task_prefix = (task.get("title") or "")[:30].lower().strip()
        if task_prefix and task_prefix in planner_titles:
            # Hitta planner_task_id och koppla
            pt_match = psql_rows(f"""
                SELECT task_id, plan_id FROM planner_task
                WHERE company_code = 'RM' AND lower(left(title, 30)) = {escape(task_prefix)}
                LIMIT 1
            """)
            if pt_match:
                pt_id = pt_match[0][0]
                pt_plan = pt_match[0][1] if len(pt_match[0]) > 1 else None
                if not DRY_RUN:
                    psql(f"UPDATE rm_task SET planner_task_id = {escape(pt_id)}, planner_plan_id = {escape(pt_plan)}, last_synced_at = NOW() WHERE id = '{task['id']}'")
                log(f"  LINKED: {task['title'][:50]} → planner={pt_id[:12]} (redan i Planner)")
                continue

        # Route till rätt plan
        plan_id, route_reason = route_to_plan(task, config, token)
        if not plan_id:
            log(f"  SKIP (no plan): {task['title'][:50]}")
            continue

        # Hämta bucket
        bucket_id = get_default_bucket(token, plan_id)
        if not bucket_id:
            log(f"  SKIP (no bucket): {task['title'][:50]} → plan {plan_id[:12]}")
            continue

        # Bygg Planner task body
        percent = COLUMN_TO_PERCENT.get(task.get("board_column", "inbox"), 0)
        body = {
            "planId": plan_id,
            "bucketId": bucket_id,
            "title": task["title"],
            "percentComplete": percent,
        }

        # Due date
        if task.get("due_date"):
            body["dueDateTime"] = f"{task['due_date']}T23:59:59Z"

        # Assignee
        if task.get("assignee_email"):
            user_id = resolve_user_id(token, task["assignee_email"])
            if user_id:
                body["assignments"] = {
                    user_id: {
                        "@odata.type": "#microsoft.graph.plannerAssignment",
                        "orderHint": " !"
                    }
                }

        if DRY_RUN:
            log(f"  DRY-RUN: Would create Planner task '{task['title'][:50]}' → {route_reason}")
            created += 1
            continue

        # POST till Graph API
        result = graph_post(token, "/planner/tasks", body)
        if not result:
            log(f"  FAILED: {task['title'][:50]} → {route_reason}")
            continue

        planner_id = result["id"]
        hash_val = checksum(task["title"], task.get("board_column"), task.get("due_date"))

        # Uppdatera rm_task
        psql(f"""
            UPDATE rm_task SET
                planner_task_id = {escape(planner_id)},
                planner_plan_id = {escape(plan_id)},
                sync_hash = {escape(hash_val)},
                last_synced_at = NOW()
            WHERE id = '{task['id']}'
        """)

        log(f"  CREATED: {task['title'][:50]} → planner={planner_id[:12]} ({route_reason})")
        created += 1

    return created


# ─────────────────────────────────────────────────────────────
# STATUS SYNC: bidirektionell
# ─────────────────────────────────────────────────────────────
def sync_status(config, token):
    """Synka statusändringar rm_task ↔ Planner."""

    # Hämta alla rm_task med planner_task_id
    rows = psql_rows("""
        SELECT id::text, title, board_column, status,
               planner_task_id, sync_hash, due_date::text
        FROM rm_task
        WHERE planner_task_id IS NOT NULL
          AND status != 'wontdo'
    """)
    if not rows:
        return 0

    keys = ["id", "title", "board_column", "status",
            "planner_task_id", "sync_hash", "due_date"]
    tasks = [dict(zip(keys, [c if c else None for c in r])) for r in rows]

    updated = 0
    for task in tasks:
        planner_id = task["planner_task_id"]

        # Hämta aktuell Planner-status
        planner_data = graph_get(token, f"/planner/tasks/{planner_id}")
        if not planner_data:
            continue

        planner_percent = planner_data.get("percentComplete", 0)
        etag = planner_data.get("@odata.etag")
        expected_percent = COLUMN_TO_PERCENT.get(task.get("board_column", "inbox"), 0)

        # Beräkna hash
        current_hash = checksum(task["title"], task.get("board_column"), task.get("due_date"))

        # Fall 1: rm_task ändrats → pusha till Planner
        if task.get("sync_hash") and current_hash != task["sync_hash"]:
            if planner_percent != expected_percent:
                if not DRY_RUN and etag:
                    success = graph_patch(token, f"/planner/tasks/{planner_id}",
                                         {"percentComplete": expected_percent}, etag)
                    if success:
                        log(f"  STATUS→PLANNER: {task['title'][:40]} {planner_percent}%→{expected_percent}%")
                        updated += 1
                else:
                    log(f"  DRY-RUN: Would update Planner {planner_id[:12]} → {expected_percent}%")

            if not DRY_RUN:
                psql(f"UPDATE rm_task SET sync_hash = {escape(current_hash)}, last_synced_at = NOW() WHERE id = '{task['id']}'")

        # Fall 2: Planner ändrats till 100% → markera done i rm_task
        elif planner_percent == 100 and task.get("board_column") != "done" and task.get("status") != "done":
            if not DRY_RUN:
                psql(f"""UPDATE rm_task SET board_column = 'done', status = 'done',
                         completed_at = NOW(), sync_hash = {escape(current_hash)}, last_synced_at = NOW()
                         WHERE id = '{task['id']}'""")
            log(f"  STATUS←PLANNER: {task['title'][:40]} → done (100% i Planner)")
            updated += 1

        # Fall 3: Planner öppnats igen (0/50%) → reopena rm_task
        elif planner_percent < 100 and task.get("status") == "done":
            new_column = "today" if planner_percent == 50 else "inbox"
            if not DRY_RUN:
                psql(f"""UPDATE rm_task SET board_column = '{new_column}', status = 'open',
                         completed_at = NULL, sync_hash = {escape(current_hash)}, last_synced_at = NOW()
                         WHERE id = '{task['id']}'""")
            log(f"  STATUS←PLANNER: {task['title'][:40]} → {new_column} (reopen {planner_percent}%)")
            updated += 1

        # Synka Planner-ändringar inbound (percent ändrad men ej done)
        elif planner_percent != expected_percent and task.get("sync_hash") == current_hash:
            # Planner-sidan har ändrats — bara logga, behåll rm_task-status
            # (undvik flapping, rm_task är master)
            pass

    return updated


# ─────────────────────────────────────────────────────────────
# PLANNER_TASK refresh: uppdatera planner_task_id på rm_task
# för tasks som skapats via planner_to_twenty.py-flödet
# ─────────────────────────────────────────────────────────────
def backfill_existing_matches():
    """Matcha befintliga planner_task med rm_task via title/assignee."""
    # Hämta rm_task utan planner_task_id men med twenty_task_id
    # Dessa kan ha skapats via dashboard/whatsapp och sedan synkats till Twenty
    # men saknar Planner-koppling
    # Vi matchar INTE dessa — de ska gå outbound till Planner
    # Denna funktion matchar planner_task → rm_task via title-match

    # Hämta alla planner_task som inte redan finns i rm_task
    rows = psql_rows("""
        SELECT pt.task_id, pt.title, pt.assignee_email
        FROM planner_task pt
        WHERE pt.company_code = 'RM'
          AND NOT EXISTS (
              SELECT 1 FROM rm_task rt WHERE rt.planner_task_id = pt.task_id
          )
    """)

    matched = 0
    for row in rows:
        ptask_id = row[0]
        ptitle = row[1]
        pemail = row[2]

        # Title-match mot rm_task utan planner_task_id
        # Exakt match ELLER substring (första 30 tecken) för WhatsApp-tasks med trunkerade titlar
        title_lower = ptitle.lower().strip() if ptitle else ""
        title_prefix = title_lower[:30] if len(title_lower) > 30 else title_lower

        match_rows = psql_rows(f"""
            SELECT id::text FROM rm_task
            WHERE planner_task_id IS NULL
              AND (
                  lower(title) = lower({escape(ptitle)})
                  OR (length(title) >= 20 AND lower(left(title, 30)) = lower({escape(title_prefix)}))
              )
              AND (assignee_email = {escape(pemail)} OR {escape(pemail)} IS NULL OR assignee_email IS NULL)
            LIMIT 1
        """)

        if match_rows:
            rm_id = match_rows[0][0]
            if not DRY_RUN:
                psql(f"UPDATE rm_task SET planner_task_id = {escape(ptask_id)} WHERE id = '{rm_id}'")
            log(f"  BACKFILL: {ptitle[:40]} → planner={ptask_id[:12]}")
            matched += 1

    return matched


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    log("═══ START ═══")
    if DRY_RUN:
        log("DRY-RUN MODE — inga ändringar skrivs")

    # Ladda config
    if not os.path.exists(CONFIG_FILE):
        log(f"Config saknas: {CONFIG_FILE}")
        sys.exit(1)

    with open(CONFIG_FILE) as f:
        config = json.load(f)

    # Token
    token = get_access_token(config)
    log("Token OK")

    # 1. Backfill: matcha befintliga planner_task mot rm_task
    matched = backfill_existing_matches()
    if matched:
        log(f"Backfill: {matched} matchade")

    # 2. Inbound: planner_task → rm_task
    imported, skipped = sync_inbound()
    log(f"Inbound: {imported} importerade, {skipped} redan kopplade")

    # 3. Outbound: rm_task → Planner (bara tasks som inte har planner_task_id)
    created = sync_outbound(config, token)
    log(f"Outbound: {created} skapade i Planner")

    # 4. Status-synk (bidirektionell)
    updated = sync_status(config, token)
    log(f"Status-synk: {updated} uppdaterade")

    log("═══ KLART ═══")


if __name__ == "__main__":
    main()
