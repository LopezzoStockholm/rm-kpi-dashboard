#!/usr/bin/env python3
"""Sync Planner tasks to Twenty CRM — deal-matched tasks.

Runs after planner_sync.py. Reads planner_task table, fuzzy-matches
task titles against Twenty company names, and creates/updates tasks
in Twenty CRM linked to the matching deal.

Config: uses same DB connections as planner_sync.py
Cron: */5 * * * * (chained after planner_sync)
"""
import subprocess, json, hashlib, uuid, re, sys, os
from datetime import datetime

try:
    from thefuzz import fuzz, process as fuzz_process
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "thefuzz", "python-Levenshtein", "-q", "--break-system-packages"])
    from thefuzz import fuzz, process as fuzz_process

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MATCH_THRESHOLD = 75          # Minimum fuzzy score for auto-match
TWENTY_DB = "twenty"
TWENTY_USER = "rmadmin"
CENTRAL_DB = "rm_central"
WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"
LOG_PREFIX = "planner_to_twenty"

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def psql(query, db=CENTRAL_DB):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", TWENTY_USER, "-d", db, "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    if r.returncode != 0 and r.stderr.strip():
        print(f"  DB error ({db}): {r.stderr.strip()[:200]}")
    return r.stdout.strip()

def psql_rows(query, db=CENTRAL_DB):
    raw = psql(query, db)
    if not raw:
        return []
    return [row.split("|") for row in raw.split("\n") if row.strip()]

def escape(s):
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"

def checksum(title, pct, due):
    return hashlib.md5(f"{title}|{pct}|{due}".encode()).hexdigest()[:16]

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
def load_planner_tasks():
    """Open Planner tasks (not completed)."""
    rows = psql_rows("""
        SELECT task_id, title, plan_name, bucket_name, assignee_name,
               percent_complete, due_date::text, assignee_email
        FROM planner_task
        WHERE company_code='RM' AND percent_complete < 100
        ORDER BY plan_name, title
    """)
    tasks = []
    for r in rows:
        if len(r) >= 8:
            tasks.append({
                "task_id": r[0], "title": r[1], "plan_name": r[2],
                "bucket_name": r[3], "assignee_name": r[4],
                "pct": int(r[5]) if r[5] else 0,
                "due": r[6] if r[6] else None,
                "email": r[7] if r[7] else ""
            })
    return tasks

def load_completed_planner():
    """Completed Planner tasks that were previously synced."""
    rows = psql_rows("""
        SELECT pt.task_id, tsm.twenty_task_id::text
        FROM planner_task pt
        JOIN task_sync_map tsm ON tsm.planner_task_id = pt.task_id
        WHERE pt.company_code='RM' AND pt.percent_complete >= 100
          AND tsm.twenty_task_id IS NOT NULL
    """)
    return [(r[0], r[1]) for r in rows if len(r) >= 2]

def load_twenty_companies():
    rows = psql_rows(f"""
        SELECT id::text, name FROM {WS}.company
        WHERE "deletedAt" IS NULL AND name IS NOT NULL AND name != ''
        ORDER BY name
    """, TWENTY_DB)
    return {r[0]: r[1] for r in rows if len(r) >= 2}

def load_twenty_deals():
    """Active deals with their company link."""
    rows = psql_rows(f"""
        SELECT o.id::text, o.name, o."companyId"::text, o.stage
        FROM {WS}.opportunity o
        WHERE o."deletedAt" IS NULL
          AND o."lostReason" IS NULL
          AND o.stage != 'FORLORAD'
        ORDER BY o.name
    """, TWENTY_DB)
    deals = []
    for r in rows:
        if len(r) >= 4:
            deals.append({
                "id": r[0], "name": r[1],
                "company_id": r[2] if r[2] else None,
                "stage": r[3]
            })
    return deals

def load_sync_map():
    rows = psql_rows("""
        SELECT planner_task_id, twenty_task_id::text, planner_checksum
        FROM task_sync_map
    """)
    return {r[0]: {"twenty_id": r[1], "checksum": r[2]} for r in rows if len(r) >= 3}

# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------
def match_task_to_company(task, companies):
    """Try to match a Planner task to a Twenty company.
    
    Strategy:
    1. Check task title for company name (fuzzy)
    2. Check bucket name for company name (fuzzy)
    3. Check deal name overlap in title
    """
    company_names = list(companies.values())
    company_ids = list(companies.keys())
    
    if not company_names:
        return None, None, 0
    
    title = task["title"]
    bucket = task["bucket_name"] or ""
    
    # Strategy 1: Title match
    result = fuzz_process.extractOne(title, company_names, scorer=fuzz.token_set_ratio)
    if result and result[1] >= MATCH_THRESHOLD:
        idx = company_names.index(result[0])
        return company_ids[idx], result[0], result[1]
    
    # Strategy 2: Bucket match (often bucket = customer name)
    if bucket and bucket not in ("Att göra", "To do", "Pågår", "Klart", "Done", "Backlog"):
        result = fuzz_process.extractOne(bucket, company_names, scorer=fuzz.token_set_ratio)
        if result and result[1] >= MATCH_THRESHOLD:
            idx = company_names.index(result[0])
            return company_ids[idx], result[0], result[1]
    
    return None, None, 0

def find_deal_for_company(company_id, deals):
    """Find best open deal for a company."""
    company_deals = [d for d in deals if d["company_id"] == company_id]
    if not company_deals:
        return None
    # Prefer deals in active stages
    stage_priority = {"KALKYL": 1, "ANBUD": 2, "FORHANDLING": 3, "AVTAL": 4, "PRODUKTION": 5, "FAKTURERAT": 6}
    company_deals.sort(key=lambda d: stage_priority.get(d["stage"], 99))
    return company_deals[0]

# ---------------------------------------------------------------------------
# Twenty CRM writes
# ---------------------------------------------------------------------------
def create_twenty_task(task, company_id, opportunity_id):
    """Create a task in Twenty CRM and link via taskTarget."""
    task_uuid = str(uuid.uuid4())
    target_uuid = str(uuid.uuid4())
    
    title_esc = escape(task["title"])
    due_sql = f"'{task['due']}'" if task["due"] else "NULL"
    
    # Create task
    psql(f"""
        INSERT INTO {WS}.task (id, title, status, "dueAt", "createdAt", "updatedAt", position)
        VALUES ('{task_uuid}', {title_esc}, 'TODO', {due_sql}, NOW(), NOW(), 0)
    """, TWENTY_DB)
    
    # Create taskTarget linking to company and/or deal
    company_sql = f"'{company_id}'" if company_id else "NULL"
    opp_sql = f"'{opportunity_id}'" if opportunity_id else "NULL"
    
    psql(f"""
        INSERT INTO {WS}."taskTarget" (id, "taskId", "targetCompanyId", "targetOpportunityId", "createdAt", "updatedAt", position)
        VALUES ('{target_uuid}', '{task_uuid}', {company_sql}, {opp_sql}, NOW(), NOW(), 0)
    """, TWENTY_DB)
    
    return task_uuid

def update_twenty_task_done(twenty_task_id):
    """Mark a Twenty task as DONE."""
    psql(f"""
        UPDATE {WS}.task SET status = 'DONE', "updatedAt" = NOW()
        WHERE id = '{twenty_task_id}' AND status != 'DONE'
    """, TWENTY_DB)

def update_twenty_task(twenty_task_id, task):
    """Update title and due date if changed."""
    title_esc = escape(task["title"])
    due_sql = f"'{task['due']}'" if task["due"] else "NULL"
    psql(f"""
        UPDATE {WS}.task SET title = {title_esc}, "dueAt" = {due_sql}, "updatedAt" = NOW()
        WHERE id = '{twenty_task_id}'
    """, TWENTY_DB)

# ---------------------------------------------------------------------------
# Sync map writes
# ---------------------------------------------------------------------------
def upsert_sync_map(planner_id, twenty_id, company_id, opp_id, source, score, chk):
    company_sql = f"'{company_id}'" if company_id else "NULL"
    opp_sql = f"'{opp_id}'" if opp_id else "NULL"
    twenty_sql = f"'{twenty_id}'" if twenty_id else "NULL"
    psql(f"""
        INSERT INTO task_sync_map (planner_task_id, twenty_task_id, twenty_company_id, twenty_opportunity_id, match_source, match_score, planner_checksum)
        VALUES ({escape(planner_id)}, {twenty_sql}, {company_sql}, {opp_sql}, {escape(source)}, {score}, {escape(chk)})
        ON CONFLICT (planner_task_id) DO UPDATE SET
            twenty_task_id = EXCLUDED.twenty_task_id,
            twenty_company_id = EXCLUDED.twenty_company_id,
            twenty_opportunity_id = EXCLUDED.twenty_opportunity_id,
            match_source = EXCLUDED.match_source,
            match_score = EXCLUDED.match_score,
            planner_checksum = EXCLUDED.planner_checksum,
            last_synced_at = NOW()
    """)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def sync():
    print(f"{datetime.now()}: {LOG_PREFIX} starting...")
    
    planner_tasks = load_planner_tasks()
    companies = load_twenty_companies()
    deals = load_twenty_deals()
    sync_map = load_sync_map()
    
    print(f"  {len(planner_tasks)} open Planner tasks, {len(companies)} companies, {len(deals)} deals")
    
    created = 0
    updated = 0
    matched = 0
    
    for task in planner_tasks:
        pid = task["task_id"]
        chk = checksum(task["title"], task["pct"], task["due"])
        
        # Already synced?
        existing = sync_map.get(pid)
        
        if existing and existing["twenty_id"]:
            # Check if task changed
            if existing["checksum"] != chk:
                update_twenty_task(existing["twenty_id"], task)
                upsert_sync_map(pid, existing["twenty_id"], None, None, "update", 0, chk)
                updated += 1
                print(f"    Updated: {task['title'][:40]}")
            continue
        
        # Try to match to a company
        company_id, company_name, score = match_task_to_company(task, companies)
        
        if company_id:
            matched += 1
            deal = find_deal_for_company(company_id, deals)
            opp_id = deal["id"] if deal else None
            
            print(f"    Match: '{task['title'][:35]}' → {company_name} (score {score})" +
                  (f" → deal: {deal['name'][:30]}" if deal else " (no deal)"))
            
            twenty_id = create_twenty_task(task, company_id, opp_id)
            upsert_sync_map(pid, twenty_id, company_id, opp_id, "titel" if score > 0 else "bucket", score, chk)
            created += 1
        else:
            # No match — record in sync map without Twenty task (so we don't retry every cycle)
            upsert_sync_map(pid, None, None, None, "no_match", 0, chk)
    
    # Handle completed tasks (Planner done → Twenty done)
    completed_pairs = load_completed_planner()
    closed = 0
    for planner_id, twenty_id in completed_pairs:
        update_twenty_task_done(twenty_id)
        closed += 1
    
    print(f"{datetime.now()}: {LOG_PREFIX} done — {matched} matched, {created} created, {updated} updated, {closed} closed")

if __name__ == "__main__":
    sync()
