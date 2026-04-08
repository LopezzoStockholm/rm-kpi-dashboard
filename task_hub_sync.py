#!/usr/bin/env python3
"""Task Hub ↔ Twenty CRM sync engine.

Synkar rm_task → Twenty CRM tasks med korrekt koppling till:
  - company (targetCompanyId)
  - deal/opportunity (targetOpportunityId)
  - person (targetPersonId)

Matchningslogik:
  1. Om company_id redan satt på rm_task → direkt
  2. Om company_name/project_name → fuzzy match mot Twenty companies
  3. Om deal_id satt → direkt
  4. Om company match → sök aktiv deal på det företaget
  5. Statussynk: board_column ↔ Twenty task status

Cron: */5 * * * * python3 /opt/rm-infra/task_hub_sync.py >> /var/log/task_hub_sync.log 2>&1
"""
import subprocess, json, hashlib, uuid, sys, os, re
from datetime import datetime

try:
    from thefuzz import fuzz
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "thefuzz", "python-Levenshtein", "-q", "--break-system-packages"])
    from thefuzz import fuzz

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
CENTRAL_DB = "rm_central"
TWENTY_DB = "twenty"
DB_USER = "rmadmin"
WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"
MATCH_THRESHOLD = 70  # fuzzy score minimum
LOG_PREFIX = "task_hub_sync"
DRY_RUN = "--dry-run" in sys.argv

# Board column → Twenty status
COLUMN_TO_TWENTY_STATUS = {
    "inbox": "TODO",
    "backlog": "TODO",
    "today": "IN_PROGRESS",
    "this_week": "IN_PROGRESS",
    "waiting": "IN_PROGRESS",
    "done": "DONE",
}

# Twenty status → board_column (inbound)
TWENTY_STATUS_TO_COLUMN = {
    "TODO": None,         # behåll befintlig kolumn
    "IN_PROGRESS": None,  # behåll befintlig kolumn
    "DONE": "done",
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
# Load referensdata
# ─────────────────────────────────────────────────────────────
def load_twenty_companies():
    """Alla aktiva Twenty-företag: {id, name}"""
    rows = psql_rows(f"""
        SELECT id::text, name
        FROM {WS}.company
        WHERE "deletedAt" IS NULL AND name IS NOT NULL AND name != ''
    """, TWENTY_DB)
    return [{"id": r[0], "name": r[1]} for r in rows]


def load_twenty_deals():
    """Alla aktiva deals med company-koppling."""
    rows = psql_rows(f"""
        SELECT o.id::text, o.name, o.stage, o."companyId"::text,
               COALESCE(c.name, '') as company_name
        FROM {WS}.opportunity o
        LEFT JOIN {WS}.company c ON c.id = o."companyId"
        WHERE o."deletedAt" IS NULL
    """, TWENTY_DB)
    return [{
        "id": r[0], "name": r[1], "stage": r[2],
        "company_id": r[3] if r[3] else None,
        "company_name": r[4]
    } for r in rows]


def load_portal_users():
    """email → twenty_member_id mappning."""
    rows = psql_rows("""
        SELECT email, twenty_member_id::text FROM portal_user
        WHERE twenty_member_id IS NOT NULL
    """, CENTRAL_DB)
    return {r[0]: r[1] for r in rows}


def load_unsynced_tasks():
    """rm_task utan twenty_task_id (behöver synkas ut)."""
    rows = psql_rows("""
        SELECT id::text, title, description, board_column, status,
               assignee_email, assignee_name,
               project_code, project_name,
               company_id::text, company_name, contact_name,
               deal_id::text, twenty_task_id::text,
               due_date::text, created_by, source
        FROM rm_task
        WHERE status != 'wontdo'
          AND twenty_task_id IS NULL
        ORDER BY created_at
    """, CENTRAL_DB)
    keys = ["id", "title", "description", "board_column", "status",
            "assignee_email", "assignee_name",
            "project_code", "project_name",
            "company_id", "company_name", "contact_name",
            "deal_id", "twenty_task_id",
            "due_date", "created_by", "source"]
    return [dict(zip(keys, [c if c else None for c in r])) for r in rows]


def load_synced_tasks():
    """rm_task med twenty_task_id (statussynk)."""
    rows = psql_rows("""
        SELECT id::text, title, board_column, status,
               twenty_task_id::text, sync_hash,
               assignee_email, due_date::text
        FROM rm_task
        WHERE twenty_task_id IS NOT NULL AND status != 'wontdo'
    """, CENTRAL_DB)
    keys = ["id", "title", "board_column", "status",
            "twenty_task_id", "sync_hash",
            "assignee_email", "due_date"]
    return [dict(zip(keys, [c if c else None for c in r])) for r in rows]


# ─────────────────────────────────────────────────────────────
# Matchning: rm_task → Twenty company/deal
# ─────────────────────────────────────────────────────────────
def match_company(task, companies):
    """Fuzzy-matcha task mot Twenty-företag. Returnerar (company_id, score, source)."""
    # 1. Om company_id redan satt
    if task.get("company_id"):
        return task["company_id"], 100, "explicit"

    # Samla kandidattexter att matcha mot
    candidates = []
    if task.get("company_name"):
        candidates.append(("company_name", task["company_name"]))
    if task.get("project_name"):
        candidates.append(("project_name", task["project_name"]))
    # Även title kan innehålla företagsnamn
    if task.get("title"):
        candidates.append(("title", task["title"]))

    if not candidates:
        return None, 0, None

    best_match = None
    best_score = 0
    best_source = None

    for source_field, text in candidates:
        for company in companies:
            cname = company["name"]
            # Exact substring match (hög konfidence)
            if cname.lower() in text.lower() and len(cname) >= 3:
                score = 95
                if score > best_score:
                    best_score = score
                    best_match = company["id"]
                    best_source = f"{source_field}_substring"
                continue

            # Fuzzy match
            score = fuzz.token_set_ratio(text.lower(), cname.lower())
            # Vikta ner title-matchning (bredare, mer noise)
            if source_field == "title":
                score = int(score * 0.8)
            if score > best_score:
                best_score = score
                best_match = company["id"]
                best_source = f"{source_field}_fuzzy"

    if best_score >= MATCH_THRESHOLD:
        return best_match, best_score, best_source
    return None, best_score, None


def match_deal(task, deals, matched_company_id):
    """Matcha task mot deal. Returnerar (deal_id, source)."""
    # 1. Om deal_id redan satt
    if task.get("deal_id"):
        return task["deal_id"], "explicit"

    # 2. Sök deal via matched company
    if matched_company_id:
        company_deals = [d for d in deals if d["company_id"] == matched_company_id]
        if len(company_deals) == 1:
            # Bara en deal → auto-koppling
            return company_deals[0]["id"], "single_company_deal"

        # Flera deals → fuzzy mot title/project_name
        if company_deals:
            best_deal = None
            best_score = 0
            search_text = " ".join(filter(None, [task.get("title"), task.get("project_name")]))
            for deal in company_deals:
                score = fuzz.token_set_ratio(search_text.lower(), deal["name"].lower())
                if score > best_score:
                    best_score = score
                    best_deal = deal["id"]
            if best_score >= 60:
                return best_deal, f"fuzzy_deal_{best_score}"

    # 3. Fuzzy match mot deal-namn direkt
    # Kräv att deal-namn har minst 3 ord ELLER att search_text innehåller
    # ett substantivt ord från deal-namnet (inte generisk text)
    search_text = " ".join(filter(None, [task.get("title"), task.get("project_name")]))
    if search_text and len(search_text.split()) >= 3:
        best_deal = None
        best_score = 0
        for deal in deals:
            dname = deal["name"]
            # Skippa matchning om deal-namn är för kort (< 5 tecken)
            if len(dname) < 5:
                continue
            score = fuzz.token_set_ratio(search_text.lower(), dname.lower())
            # Extra krav: minst ett ord (>3 bokstäver) från deal-namnet
            # måste finnas som substring i search_text
            deal_words = [w for w in dname.lower().split() if len(w) > 3]
            has_overlap = any(w in search_text.lower() for w in deal_words)
            if not has_overlap:
                score = int(score * 0.4)  # kraftig nedvikt utan ordöverlapp
            if score > best_score:
                best_score = score
                best_deal = deal
        if best_score >= MATCH_THRESHOLD and best_deal:
            return best_deal["id"], f"deal_name_fuzzy_{best_score}"

    return None, None


# ─────────────────────────────────────────────────────────────
# Twenty task CRUD
# ─────────────────────────────────────────────────────────────
def create_twenty_task(task, company_id, deal_id, users):
    """Skapa task + taskTarget i Twenty. Returnerar twenty_task_id."""
    task_uuid = str(uuid.uuid4())
    target_uuid = str(uuid.uuid4())

    title_esc = escape(task["title"])
    status = COLUMN_TO_TWENTY_STATUS.get(task.get("board_column", "inbox"), "TODO")

    # Due date
    due_sql = "NULL"
    if task.get("due_date"):
        due_sql = f"'{task['due_date']}T00:00:00+00:00'"

    # Assignee → Twenty workspace member
    assignee_sql = "NULL"
    if task.get("assignee_email") and task["assignee_email"] in users:
        assignee_sql = f"'{users[task['assignee_email']]}'"

    # Uppgiftstyp baserat på task_type/context
    typ_sql = "NULL"

    if DRY_RUN:
        log(f"  DRY-RUN: Would create Twenty task '{task['title']}' → company={company_id}, deal={deal_id}")
        return task_uuid

    # Skapa task
    psql(f"""
        INSERT INTO {WS}.task
          (id, title, status, "dueAt", "assigneeId", uppgiftstyp,
           "createdAt", "updatedAt", position)
        VALUES
          ('{task_uuid}', {title_esc}, '{status}', {due_sql}, {assignee_sql}, {typ_sql},
           NOW(), NOW(), 0)
    """, TWENTY_DB)

    # Skapa taskTarget (koppling till company/deal)
    if company_id or deal_id:
        company_sql = f"'{company_id}'" if company_id else "NULL"
        deal_sql = f"'{deal_id}'" if deal_id else "NULL"
        psql(f"""
            INSERT INTO {WS}."taskTarget"
              (id, "taskId", "targetCompanyId", "targetOpportunityId",
               "createdAt", "updatedAt", position)
            VALUES
              ('{target_uuid}', '{task_uuid}', {company_sql}, {deal_sql},
               NOW(), NOW(), 0)
        """, TWENTY_DB)

    return task_uuid


def update_twenty_task_status(twenty_task_id, new_status):
    """Uppdatera status på Twenty-task."""
    if DRY_RUN:
        log(f"  DRY-RUN: Would update Twenty task {twenty_task_id} → {new_status}")
        return
    psql(f"""
        UPDATE {WS}.task SET status = '{new_status}', "updatedAt" = NOW()
        WHERE id = '{twenty_task_id}'
    """, TWENTY_DB)


# ─────────────────────────────────────────────────────────────
# Synk: rm_task → Twenty (outbound)
# ─────────────────────────────────────────────────────────────
def sync_outbound(tasks, companies, deals, users):
    """Skapa Twenty-tasks för rm_task som saknar twenty_task_id."""
    created = 0
    matched = 0
    for task in tasks:
        # Matcha company
        company_id, score, match_source = match_company(task, companies)
        if company_id:
            matched += 1

        # Matcha deal
        deal_id, deal_source = match_deal(task, deals, company_id)

        # Skapa i Twenty
        twenty_id = create_twenty_task(task, company_id, deal_id, users)

        # Uppdatera rm_task med twenty_task_id + company_id/name
        update_parts = [f"twenty_task_id = '{twenty_id}'"]
        update_parts.append(f"sync_hash = {escape(checksum(task['title'], task.get('board_column'), task.get('due_date')))}")
        update_parts.append(f"last_synced_at = NOW()")

        if company_id and not task.get("company_id"):
            update_parts.append(f"company_id = '{company_id}'")
            # Hämta company name
            for c in companies:
                if c["id"] == company_id:
                    update_parts.append(f"company_name = {escape(c['name'])}")
                    break

        if deal_id and not task.get("deal_id"):
            update_parts.append(f"deal_id = '{deal_id}'")

        if not DRY_RUN:
            psql(f"UPDATE rm_task SET {', '.join(update_parts)} WHERE id = '{task['id']}'", CENTRAL_DB)

        source_info = f"company={match_source}({score})" if company_id else "no_match"
        deal_info = f"deal={deal_source}" if deal_id else "no_deal"
        log(f"  CREATED: {task['title'][:50]} → twenty={twenty_id[:8]} {source_info} {deal_info}")
        created += 1

    return created, matched


# ─────────────────────────────────────────────────────────────
# Synk: status-ändringar (bidirektionell)
# ─────────────────────────────────────────────────────────────
def sync_status(synced_tasks):
    """Synka statusändringar mellan rm_task och Twenty."""
    updated = 0
    for task in synced_tasks:
        if not task.get("twenty_task_id"):
            continue

        # Beräkna nuvarande hash
        current_hash = checksum(task["title"], task.get("board_column"), task.get("due_date"))

        # Kolla Twenty-status
        rows = psql_rows(f"""
            SELECT status FROM {WS}.task WHERE id = '{task['twenty_task_id']}' AND "deletedAt" IS NULL
        """, TWENTY_DB)
        if not rows:
            continue

        twenty_status = rows[0][0]
        expected_twenty_status = COLUMN_TO_TWENTY_STATUS.get(task.get("board_column", "inbox"), "TODO")

        # Om rm_task ändrats → uppdatera Twenty
        if task.get("sync_hash") and current_hash != task["sync_hash"]:
            if twenty_status != expected_twenty_status:
                update_twenty_task_status(task["twenty_task_id"], expected_twenty_status)
                log(f"  STATUS→TWENTY: {task['title'][:40]} {twenty_status}→{expected_twenty_status}")
                updated += 1

            if not DRY_RUN:
                psql(f"""UPDATE rm_task SET sync_hash = {escape(current_hash)}, last_synced_at = NOW()
                         WHERE id = '{task['id']}'""", CENTRAL_DB)

        # Om Twenty ändrats till DONE → uppdatera rm_task
        elif twenty_status == "DONE" and task.get("board_column") != "done" and task.get("status") != "done":
            if not DRY_RUN:
                psql(f"""UPDATE rm_task SET board_column = 'done', status = 'done',
                         completed_at = NOW(), sync_hash = {escape(current_hash)}, last_synced_at = NOW()
                         WHERE id = '{task['id']}'""", CENTRAL_DB)
            log(f"  STATUS←TWENTY: {task['title'][:40]} → done (stängd i CRM)")
            updated += 1

    return updated


# ─────────────────────────────────────────────────────────────
# Import: befintliga Twenty tasks → rm_task (inbound)
# ─────────────────────────────────────────────────────────────
def import_twenty_orphans():
    """Importera Twenty-tasks som saknar rm_task-koppling."""
    # Hämta alla Twenty task-id:n som redan finns i rm_task
    existing = set()
    rows = psql_rows("SELECT twenty_task_id::text FROM rm_task WHERE twenty_task_id IS NOT NULL", CENTRAL_DB)
    for r in rows:
        existing.add(r[0])

    # Hämta Twenty-tasks
    twenty_tasks = psql_rows(f"""
        SELECT t.id::text, t.title, t.status, t."dueAt"::text,
               t."assigneeId"::text,
               COALESCE(tt."targetCompanyId"::text, '') as company_id,
               COALESCE(tt."targetOpportunityId"::text, '') as deal_id
        FROM {WS}.task t
        LEFT JOIN {WS}."taskTarget" tt ON tt."taskId" = t.id AND tt."deletedAt" IS NULL
        WHERE t."deletedAt" IS NULL
    """, TWENTY_DB)

    # Hämta member → email reverse mapping
    member_rows = psql_rows("SELECT twenty_member_id::text, email, display_name FROM portal_user WHERE twenty_member_id IS NOT NULL", CENTRAL_DB)
    member_to_email = {r[0]: r[1] for r in member_rows}
    member_to_name = {r[0]: r[2] for r in member_rows}

    imported = 0
    for row in twenty_tasks:
        twenty_id = row[0]
        if twenty_id in existing:
            continue

        title = row[1] or "(ingen titel)"
        status = row[2] or "TODO"
        due_at = row[3] if row[3] else None
        assignee_member = row[4] if row[4] else None
        company_id = row[5] if row[5] else None
        deal_id = row[6] if row[6] else None

        # Map status
        board_column = "inbox"
        rm_status = "open"
        if status == "DONE":
            board_column = "done"
            rm_status = "done"
        elif status == "IN_PROGRESS":
            board_column = "today"

        # Map assignee
        assignee_email = member_to_email.get(assignee_member) if assignee_member else None
        assignee_name = member_to_name.get(assignee_member) if assignee_member else None

        # Due date
        due_sql = "NULL"
        if due_at and due_at != "":
            due_sql = f"'{due_at[:10]}'"

        # Company name
        company_name_sql = "NULL"
        if company_id:
            cn_rows = psql_rows(f"SELECT name FROM {WS}.company WHERE id = '{company_id}'", TWENTY_DB)
            if cn_rows:
                company_name_sql = escape(cn_rows[0][0])

        new_id = str(uuid.uuid4())
        hash_val = checksum(title, board_column, due_at)

        if DRY_RUN:
            log(f"  DRY-RUN: Would import Twenty task '{title[:50]}' company={company_id}")
            imported += 1
            continue

        psql(f"""
            INSERT INTO rm_task
              (id, company_code, title, status, board_column,
               assignee_email, assignee_name,
               company_id, company_name, deal_id,
               twenty_task_id, sync_hash, last_synced_at,
               due_date, source, external_system, external_id, created_by)
            VALUES
              ('{new_id}', 'RM', {escape(title)}, '{rm_status}', '{board_column}',
               {escape(assignee_email)}, {escape(assignee_name)},
               {f"'{company_id}'" if company_id else 'NULL'},
               {company_name_sql},
               {f"'{deal_id}'" if deal_id else 'NULL'},
               '{twenty_id}', {escape(hash_val)}, NOW(),
               {due_sql}, 'twenty', 'twenty', '{twenty_id}', 'system')
        """, CENTRAL_DB)
        log(f"  IMPORTED: {title[:50]} (twenty={twenty_id[:8]})")
        imported += 1

    return imported


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    log("═══ START ═══")
    if DRY_RUN:
        log("DRY-RUN MODE — inga ändringar skrivs")

    # Ladda referensdata
    companies = load_twenty_companies()
    deals = load_twenty_deals()
    users = load_portal_users()
    log(f"Loaded: {len(companies)} companies, {len(deals)} deals, {len(users)} users")

    # 1. Outbound: rm_task → Twenty
    unsynced = load_unsynced_tasks()
    if unsynced:
        created, matched = sync_outbound(unsynced, companies, deals, users)
        log(f"Outbound: {created} skapade, {matched} company-matchade")
    else:
        log("Outbound: inga nya tasks")

    # 2. Status-synk (bidirektionell)
    synced = load_synced_tasks()
    if synced:
        updated = sync_status(synced)
        log(f"Status-synk: {updated} uppdaterade")
    else:
        log("Status-synk: inga kopplade tasks")

    # 3. Inbound: Twenty → rm_task (importera orphans)
    if "--import" in sys.argv:
        imported = import_twenty_orphans()
        log(f"Import: {imported} Twenty-tasks importerade")

    log("═══ KLART ═══")


if __name__ == "__main__":
    main()
