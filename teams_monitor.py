#!/usr/bin/env python3
"""
teams_monitor.py — Polls Teams channels for new messages and creates Planner tasks.

Runs every 5 minutes via cron. Reads messages from configured channels,
passes them through message_parser.py, and creates Planner tasks for
messages that look like task assignments.

Uses docker exec for DB access (consistent with planner_sync.py).
"""

import json
import subprocess
import sys
import re
import requests
from datetime import datetime, timezone
from pathlib import Path

# Ensure thefuzz is available
try:
    from thefuzz import fuzz
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "thefuzz", "python-Levenshtein", "-q", "--break-system-packages"])

from message_parser import parse_message, create_planner_task, load_companies_via_psql

# ── Config ──
CONFIG_DIR = Path("/opt/rm-infra")
PLANNER_CONFIG = CONFIG_DIR / "planner-config.json"
TEAMS_CONFIG = CONFIG_DIR / "teams-config.json"
STATE_FILE = CONFIG_DIR / "teams_monitor_state.json"

EXCLUDED_CHANNELS = set()  # Monitor ALL channels including General
MIN_CONFIDENCE = 0.50

CENTRAL_DB = "rm_central"
TWENTY_DB = "twenty"
DB_USER = "rmadmin"


# ── DB helper (docker exec) ──
def psql(query, db=CENTRAL_DB):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", DB_USER, "-d", db, "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    return r.stdout.strip()


def psql_exec(query, db=CENTRAL_DB):
    subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", DB_USER, "-d", db, "-c", query],
        capture_output=True, text=True
    )


def get_token():
    config = json.loads(PLANNER_CONFIG.read_text())
    r = requests.post(
        f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/v2.0/token",
        data={
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials"
        }
    )
    return r.json()["access_token"]


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_monitored_channels(token):
    """Dynamically fetch ALL teams and channels from Graph API.
    No static config needed — new teams/channels are automatically included.
    """
    headers = {"Authorization": f"Bearer {token}"}
    channels = []

    # Get all M365 groups that have Teams provisioned
    groups_url = "https://graph.microsoft.com/v1.0/groups?$filter=resourceProvisioningOptions/Any(x:x eq 'Team')&$select=id,displayName"
    groups_resp = requests.get(groups_url, headers=headers)
    if groups_resp.status_code != 200:
        print(f"  WARN: Could not fetch teams: {groups_resp.status_code}")
        # Fallback to static config
        teams_data = json.loads(TEAMS_CONFIG.read_text()) if TEAMS_CONFIG.exists() else {}
        for team_id, team_info in teams_data.get("teams", {}).items():
            for ch in team_info.get("channels", []):
                channels.append({
                    "team_id": team_id,
                    "team_name": team_info["name"],
                    "channel_id": ch["id"],
                    "channel_name": ch["name"]
                })
        return channels

    for group in groups_resp.json().get("value", []):
        team_id = group["id"]
        team_name = group["displayName"]

        # Get all channels for this team
        ch_resp = requests.get(
            f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels?$select=id,displayName",
            headers=headers
        )
        if ch_resp.status_code != 200:
            continue

        for ch in ch_resp.json().get("value", []):
            channels.append({
                "team_id": team_id,
                "team_name": team_name,
                "channel_id": ch["id"],
                "channel_name": ch["displayName"]
            })

    return channels


def fetch_new_messages(token, team_id, channel_id, since_ts=None):
    """Fetch top-level messages AND their replies from a Teams channel."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages"
    params = {"$top": 20}
    resp = requests.get(url, headers=headers, params=params)

    if resp.status_code != 200:
        print(f"  WARN: Could not fetch channel messages: {resp.status_code} {resp.text[:200]}")
        return []

    top_messages = resp.json().get("value", [])

    # Filter by timestamp
    if since_ts:
        # Keep top-level messages that are new OR might have new replies
        # (a message can be old but have new replies)
        top_messages = [m for m in top_messages
                        if m.get("lastModifiedDateTime", m.get("createdDateTime", "")) > since_ts
                        or m.get("createdDateTime", "") > since_ts]

    all_messages = []

    for m in top_messages:
        if m.get("messageType") == "message":
            all_messages.append(m)

        # Fetch replies for this message
        replies_url = f"{url}/{m['id']}/replies"
        replies_resp = requests.get(replies_url, headers=headers, params={"$top": 50})
        if replies_resp.status_code == 200:
            replies = replies_resp.json().get("value", [])
            if since_ts:
                replies = [r for r in replies if r.get("createdDateTime", "") > since_ts]
            for r in replies:
                if r.get("messageType") == "message":
                    # Tag reply with parent context for better parsing
                    r["_isReply"] = True
                    r["_parentMessageId"] = m["id"]
                    all_messages.append(r)

    return all_messages


def extract_text(message):
    body = message.get("body", {})
    content = body.get("content", "")
    if body.get("contentType") == "html":
        content = re.sub(r'<[^>]+>', ' ', content)
        content = content.replace("&nbsp;", " ").replace("&amp;", "&")
        content = content.replace("&lt;", "<").replace("&gt;", ">")
    return content.strip()


def message_already_processed(msg_id):
    safe_id = msg_id.replace("'", "''")
    result = psql(f"SELECT id FROM message_task_log WHERE source_message_id = '{safe_id}'")
    return bool(result)


def log_message(source, channel, msg_id, raw_text, sender_name, sender_id,
                parsed=None, planner_task_id=None, skipped=False, skip_reason=None):
    def esc(v):
        if v is None:
            return "NULL"
        return "'" + str(v).replace("'", "''") + "'"

    p = parsed or {}
    sql = f"""INSERT INTO message_task_log
        (source, source_channel, source_message_id, raw_text, sender_name, sender_id,
         parsed_assignee, parsed_assignee_id, parsed_title, parsed_project,
         parsed_bucket_id, parsed_due_date, planner_task_id, confidence, skipped, skip_reason)
    VALUES (
        {esc(source)}, {esc(channel)}, {esc(msg_id)}, {esc(raw_text[:500])}, {esc(sender_name)}, {esc(sender_id)},
        {esc(p.get('assignee_name'))}, {esc(p.get('assignee_id'))}, {esc(p.get('title'))}, {esc(p.get('project'))},
        {esc(p.get('bucket_id'))}, {esc(str(p['due_date']) if p.get('due_date') else None)},
        {esc(planner_task_id)}, {p.get('confidence', 'NULL')}, {str(skipped).lower()}, {esc(skip_reason)}
    )"""
    psql_exec(sql)


def run():
    ts = datetime.now(timezone.utc)
    print(f"{ts.isoformat()}: teams_monitor starting...")

    token = get_token()
    state = load_state()
    channels = get_monitored_channels(token)

    # Load companies for project matching
    companies = load_companies_via_psql()

    total_messages = 0
    total_tasks = 0
    total_skipped = 0

    for ch in channels:
        ch_key = ch["channel_id"]
        since = state.get(ch_key)

        messages = fetch_new_messages(token, ch["team_id"], ch["channel_id"], since)

        if not messages:
            continue

        print(f"  Channel: {ch['team_name']} / {ch['channel_name']} — {len(messages)} new messages")

        newest_ts = since
        for msg in messages:
            msg_id = msg["id"]
            msg_ts = msg["createdDateTime"]
            total_messages += 1

            if newest_ts is None or msg_ts > newest_ts:
                newest_ts = msg_ts

            if message_already_processed(msg_id):
                continue

            text = extract_text(msg)
            sender = msg.get("from", {}).get("user", {}) or {}
            sender_name = sender.get("displayName", "Unknown")
            sender_id = sender.get("id")

            if not text or len(text) < 5:
                log_message("teams", ch["channel_name"], msg_id, text or "",
                           sender_name, sender_id, skipped=True, skip_reason="too_short")
                total_skipped += 1
                continue

            # Skip automated/bot messages (CRM Audit, etc.)
            if any(kw in text.lower() for kw in ["crm audit", "dashboard:", "issues ("]):
                log_message("teams", ch["channel_name"], msg_id, text,
                           sender_name, sender_id, skipped=True, skip_reason="automated_message")
                total_skipped += 1
                continue

            # Parse message (pass companies directly)
            parsed = parse_message(text, sender_name, sender_id, source="teams",
                                   companies=companies)

            if not parsed:
                log_message("teams", ch["channel_name"], msg_id, text,
                           sender_name, sender_id, skipped=True, skip_reason="not_a_task")
                total_skipped += 1
                continue

            if parsed["confidence"] < MIN_CONFIDENCE:
                log_message("teams", ch["channel_name"], msg_id, text,
                           sender_name, sender_id, parsed=parsed,
                           skipped=True, skip_reason=f"low_confidence_{parsed['confidence']:.0%}")
                total_skipped += 1
                continue

            # Create Planner task
            planner_task_id = create_planner_task(parsed)

            if planner_task_id:
                print(f"    TASK: '{parsed['title']}' → {parsed.get('assignee_name', 'unassigned')} "
                      f"(confidence: {parsed['confidence']:.0%})")
                total_tasks += 1

            log_message("teams", ch["channel_name"], msg_id, text,
                       sender_name, sender_id, parsed=parsed, planner_task_id=planner_task_id)

        if newest_ts:
            state[ch_key] = newest_ts

    save_state(state)

    print(f"{datetime.now(timezone.utc).isoformat()}: teams_monitor done — "
          f"{total_messages} messages, {total_tasks} tasks created, {total_skipped} skipped")


if __name__ == "__main__":
    run()
