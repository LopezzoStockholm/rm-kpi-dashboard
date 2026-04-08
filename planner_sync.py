#!/usr/bin/env python3
"""Sync Microsoft Planner tasks to PostgreSQL for RM dashboard.

Requires Azure AD app registration with:
  - Application permission: Tasks.Read.All, Group.Read.All
  - Or delegated permission: Tasks.Read, Group.Read.All

Config file: /opt/rm-infra/planner-config.json
{
    "tenant_id": "1508fbab-5db4-45c6-8c16-9af15443febc",
    "client_id": "<from Azure AD app registration>",
    "client_secret": "<from Azure AD app registration>",
    "plans": {
        "eQsKfDDyjkG-b8dSB-dpGJgAANY_": "RM Drift & Projekt",
        "YD99zEGdpkK8No7ihLpEUJgAEcD-": "RM Ekonomi & Styrning",
        "_R91WOHYEkGm1yvJlzX4nZgAHTTt": "Fastighetsutveckling",
        "0bbH2r3GQUabli75RNyHXJgABupG": "Bolagsstyrning & Ekonomi",
        "Nhr4yBhcbkGD9uqTP296VZgAHqmG": "Sälj & Affärsutveckling",
        "lPbsJZjTI0-b3z3qwEC7pJgABJgp": "Actionlista / All Company"
    }
}
"""
import json, sys, subprocess, os
from datetime import datetime

try:
    import requests
except ImportError:
    print("Installing requests...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "-q"])
    import requests

CONFIG_FILE = "/opt/rm-infra/planner-config.json"
GRAPH_BASE = "https://graph.microsoft.com/v1.0"

def psql(query, db="rm_central"):
    """Execute PostgreSQL query via docker."""
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"  DB error: {r.stderr.strip()}")
    return r.stdout.strip()

def get_access_token(config):
    """Get OAuth2 token via client credentials flow."""
    url = f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
        "scope": "https://graph.microsoft.com/.default"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

def get_plan_buckets(token, plan_id):
    """Fetch bucket names for a plan."""
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{GRAPH_BASE}/planner/plans/{plan_id}/buckets", headers=headers)
    if r.status_code != 200:
        print(f"  Buckets fetch failed ({r.status_code}): {r.text[:200]}")
        return {}
    buckets = r.json().get("value", [])
    return {b["id"]: b["name"] for b in buckets}

def get_plan_tasks(token, plan_id):
    """Fetch all tasks for a plan."""
    headers = {"Authorization": f"Bearer {token}"}
    tasks = []
    url = f"{GRAPH_BASE}/planner/plans/{plan_id}/tasks"
    while url:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            print(f"  Tasks fetch failed ({r.status_code}): {r.text[:200]}")
            return tasks
        data = r.json()
        tasks.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return tasks

def get_user_name(token, user_id, user_cache):
    """Resolve user ID to display name."""
    if user_id in user_cache:
        return user_cache[user_id]
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{GRAPH_BASE}/users/{user_id}", headers=headers)
    if r.status_code == 200:
        name = r.json().get("displayName", "Okänd")
        email = r.json().get("mail", "")
        user_cache[user_id] = (name, email)
        return (name, email)
    user_cache[user_id] = ("Okänd", "")
    return ("Okänd", "")

def escape_sql(s):
    """Escape string for SQL."""
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"

def sync():
    """Main sync function."""
    if not os.path.exists(CONFIG_FILE):
        print(f"Config file not found: {CONFIG_FILE}")
        print("Create it with tenant_id, client_id, client_secret, and plans.")
        print("See script header for format.")
        sys.exit(1)

    with open(CONFIG_FILE) as f:
        config = json.load(f)

    print(f"{datetime.now()}: Planner sync starting...")

    token = get_access_token(config)
    plans = config.get("plans", {})
    user_cache = {}
    total_tasks = 0

    # Clear old data
    psql("DELETE FROM planner_task WHERE company_code='RM'")

    for plan_id, plan_name in plans.items():
        print(f"  Syncing plan: {plan_name} ({plan_id[:12]}...)")

        buckets = get_plan_buckets(token, plan_id)
        tasks = get_plan_tasks(token, plan_id)

        if not tasks:
            print(f"    No tasks found")
            continue

        for task in tasks:
            task_id = task["id"]
            bucket_id = task.get("bucketId", "")
            bucket_name = buckets.get(bucket_id, "Okänd")
            title = task.get("title", "")
            percent = task.get("percentComplete", 0)
            priority = task.get("priority", 1)

            # Parse dates
            due = task.get("dueDateTime")
            due_sql = escape_sql(due[:10]) if due else "NULL"

            created = task.get("createdDateTime")
            created_sql = escape_sql(created) if created else "NULL"

            completed = task.get("completedDateTime")
            completed_sql = escape_sql(completed) if completed else "NULL"

            # Get assignees
            assignments = task.get("assignments", {})
            assignee_names = []
            assignee_emails = []
            for uid in assignments:
                name, email = get_user_name(token, uid, user_cache)
                assignee_names.append(name)
                assignee_emails.append(email)

            assignee_name = ", ".join(assignee_names) if assignee_names else ""
            assignee_email = ", ".join(assignee_emails) if assignee_emails else ""

            sql = f"""
                INSERT INTO planner_task (task_id, plan_id, plan_name, bucket_name, title,
                    assignee_name, assignee_email, due_date, percent_complete, priority,
                    created_at, completed_at, company_code)
                VALUES ({escape_sql(task_id)}, {escape_sql(plan_id)}, {escape_sql(plan_name)},
                    {escape_sql(bucket_name)}, {escape_sql(title)},
                    {escape_sql(assignee_name)}, {escape_sql(assignee_email)},
                    {due_sql}, {percent}, {priority},
                    {created_sql}, {completed_sql}, 'RM')
                ON CONFLICT (task_id) DO UPDATE SET
                    bucket_name=EXCLUDED.bucket_name, title=EXCLUDED.title,
                    assignee_name=EXCLUDED.assignee_name, assignee_email=EXCLUDED.assignee_email,
                    due_date=EXCLUDED.due_date, percent_complete=EXCLUDED.percent_complete,
                    priority=EXCLUDED.priority, completed_at=EXCLUDED.completed_at,
                    updated_at=NOW()
            """
            psql(sql)
            total_tasks += 1

        print(f"    {len(tasks)} tasks synced")

    print(f"{datetime.now()}: Planner sync complete — {total_tasks} tasks across {len(plans)} plans")

if __name__ == "__main__":
    sync()
