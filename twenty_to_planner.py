#!/usr/bin/env python3
"""Reverse sync: Twenty CRM task status -> Planner.

When a deal-linked task is marked DONE in Twenty,
update the corresponding Planner task to 100% complete.
When reopened in Twenty, set Planner to 50% (in progress).

Runs after planner_to_twenty.py in the cron chain.
"""
import subprocess, json, sys, os
from datetime import datetime

try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "-q", "--break-system-packages"])
    import requests

CONFIG_FILE = "/opt/rm-infra/planner-config.json"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
CENTRAL_DB = "rm_central"
TWENTY_DB = "twenty"
DB_USER = "rmadmin"
WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"
LOG_PREFIX = "twenty_to_planner"

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
    return [row.split("|") for row in raw.split("\n") if row.strip()]

def get_access_token(config):
    url = f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/v2.0/token"
    r = requests.post(url, data={
        "grant_type": "client_credentials",
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
        "scope": "https://graph.microsoft.com/.default"
    })
    r.raise_for_status()
    return r.json()["access_token"]

def get_planner_task(token, task_id):
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{GRAPH_BASE}/planner/tasks/{task_id}", headers=headers)
    if r.status_code != 200:
        return None, None
    data = r.json()
    return data, data.get("@odata.etag", "")

def update_planner_pct(token, task_id, etag, pct):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "If-Match": etag
    }
    r = requests.patch(f"{GRAPH_BASE}/planner/tasks/{task_id}", headers=headers, json={"percentComplete": pct})
    return r.status_code in (200, 204)

def sync():
    print(f"{datetime.now()}: {LOG_PREFIX} starting...")

    if not os.path.exists(CONFIG_FILE):
        print(f"  Config not found: {CONFIG_FILE}")
        return

    with open(CONFIG_FILE) as f:
        config = json.load(f)

    # Step 1: Get all synced pairs from rm_central
    sync_entries = psql_rows("""
        SELECT planner_task_id, twenty_task_id::text
        FROM task_sync_map
        WHERE twenty_task_id IS NOT NULL
    """)

    if not sync_entries:
        print(f"{datetime.now()}: {LOG_PREFIX} done — no synced tasks")
        return

    token = get_access_token(config)
    completed = 0
    reopened = 0

    for planner_id, twenty_id in sync_entries:
        # Step 2: Get Twenty task status
        twenty_status = psql(f"""
            SELECT status FROM {WS}.task WHERE id = '{twenty_id}' AND "deletedAt" IS NULL
        """, TWENTY_DB).strip()

        if not twenty_status:
            continue

        # Step 3: Get Planner task current state
        planner_row = psql(f"""
            SELECT percent_complete, title FROM planner_task
            WHERE task_id = '{planner_id}' AND company_code = 'RM'
        """)
        if not planner_row:
            continue

        parts = planner_row.split("|")
        planner_pct = int(parts[0]) if parts[0].strip().isdigit() else 0
        planner_title = parts[1].strip() if len(parts) > 1 else "?"

        # Step 4: Sync status
        if twenty_status == "DONE" and planner_pct < 100:
            task_data, etag = get_planner_task(token, planner_id)
            if task_data and etag:
                if update_planner_pct(token, planner_id, etag, 100):
                    completed += 1
                    print(f"    Planner -> 100%: {planner_title[:40]}")
                else:
                    print(f"    FAIL complete: {planner_title[:40]}")

        elif twenty_status in ("TODO", "IN_PROGRESS") and planner_pct >= 100:
            task_data, etag = get_planner_task(token, planner_id)
            if task_data and etag:
                if update_planner_pct(token, planner_id, etag, 50):
                    reopened += 1
                    print(f"    Planner -> 50%: {planner_title[:40]}")
                else:
                    print(f"    FAIL reopen: {planner_title[:40]}")

    print(f"{datetime.now()}: {LOG_PREFIX} done — {completed} completed, {reopened} reopened")

if __name__ == "__main__":
    sync()
