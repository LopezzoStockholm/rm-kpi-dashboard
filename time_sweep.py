#!/usr/bin/env python3
"""
WhatsApp Time Report — End-of-day Sweep (Fas F, steg 3)

Sends interactive list messages to field personnel at 16:30 weekdays,
asking them to report hours for their active projects.

Flow:
  1. Cron triggers sweep → sends list message per user
  2. User taps project → webhook receives interactive.list_reply
  3. Bot replies "Hur många timmar på {projekt}?"
  4. User sends hours → saved as time_report
  5. Bot confirms

Tables:
  whatsapp_time_pending  — conversation state (who is being asked, which project)

Usage:
  python3 time_sweep.py              # Send sweep to all eligible users
  python3 time_sweep.py --test 46707830063   # Test: send only to this number
  python3 time_sweep.py --check      # Check who hasn't reported today
"""

import json
import sys
import os
import re
import requests
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CONFIG_DIR = Path("/opt/rm-infra/config")
META_CONFIG = CONFIG_DIR / "whatsapp-meta-config.json"
CENTRAL_DB = "rm_central"

# We import rm_data helpers that are already on the server
sys.path.insert(0, "/opt/rm-infra")
from rm_data import execute, query_dicts

PHONE_NUMBER_ID = None
META_ACCESS_TOKEN = None

def _load_meta():
    global PHONE_NUMBER_ID, META_ACCESS_TOKEN
    if META_CONFIG.exists():
        cfg = json.loads(META_CONFIG.read_text())
        PHONE_NUMBER_ID = cfg.get("phone_number_id")
        META_ACCESS_TOKEN = cfg.get("access_token")

_load_meta()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _ensure_pending_table():
    """Create whatsapp_time_pending if not exists."""
    execute("""
        CREATE TABLE IF NOT EXISTS whatsapp_time_pending (
            id SERIAL PRIMARY KEY,
            wa_id TEXT NOT NULL,
            user_id INT,
            person TEXT,
            project_code TEXT,
            project_name TEXT,
            prompt_type TEXT DEFAULT 'hours',
            created_at TIMESTAMPTZ DEFAULT now(),
            expires_at TIMESTAMPTZ DEFAULT (now() + interval '4 hours'),
            resolved BOOLEAN DEFAULT false,
            resolved_at TIMESTAMPTZ
        )
    """, db=CENTRAL_DB)
    execute("""
        CREATE INDEX IF NOT EXISTS idx_time_pending_wa
        ON whatsapp_time_pending (wa_id, resolved, expires_at)
    """, db=CENTRAL_DB)


def get_pending(wa_id: str):
    """Get active (unresolved, non-expired) pending prompt for a user."""
    rows = query_dicts("""
        SELECT id, project_code, project_name, prompt_type
        FROM whatsapp_time_pending
        WHERE wa_id = %s AND resolved = false AND expires_at > now()
        ORDER BY created_at DESC LIMIT 1
    """, (wa_id,), db=CENTRAL_DB)
    return rows[0] if rows else None


def set_pending(wa_id: str, user_id: int, person: str, project_code: str, project_name: str):
    """Set a pending hours-prompt for a user (clears any previous)."""
    # Resolve old
    execute("""
        UPDATE whatsapp_time_pending SET resolved = true, resolved_at = now()
        WHERE wa_id = %s AND resolved = false
    """, (wa_id,), db=CENTRAL_DB)
    # Insert new
    execute("""
        INSERT INTO whatsapp_time_pending (wa_id, user_id, person, project_code, project_name)
        VALUES (%s, %s, %s, %s, %s)
    """, (wa_id, user_id, person, project_code, project_name), db=CENTRAL_DB)


def resolve_pending(wa_id: str):
    """Mark pending as resolved."""
    execute("""
        UPDATE whatsapp_time_pending SET resolved = true, resolved_at = now()
        WHERE wa_id = %s AND resolved = false
    """, (wa_id,), db=CENTRAL_DB)


# ---------------------------------------------------------------------------
# Get eligible users and their projects
# ---------------------------------------------------------------------------
def get_sweep_users():
    """Get users who should receive end-of-day sweep.
    Returns [{id, username, display_name, role, phone, wa_id}]"""
    rows = query_dicts("""
        SELECT pu.id, pu.username, pu.display_name, pu.role, pu.phone
        FROM portal_user pu
        WHERE pu.role IN ('projektledare', 'vd')
          AND pu.phone IS NOT NULL
          AND pu.phone != ''
    """, db=CENTRAL_DB)
    
    result = []
    for r in rows:
        phone = r["phone"].lstrip("0")
        wa_id = f"46{phone}"
        result.append({**r, "wa_id": wa_id})
    return result


def get_user_projects(user_id: int, wa_id: str):
    """Get active projects for a user.
    Strategy: recent time_reports + projects they're assigned to.
    Returns [{project_code, project_name}] max 10."""
    
    # Recent projects from time_report (last 30 days)
    recent = query_dicts("""
        SELECT project_code,
               COALESCE(
                 (SELECT project_name FROM next_project_economy WHERE project_no = sub.project_code LIMIT 1),
                 sub.project_code
               ) as project_name
        FROM (
            SELECT t.project_code, MAX(t.work_date) as last_date
            FROM time_report t
            WHERE (t.user_id = %s OR t.person = (SELECT display_name FROM portal_user WHERE id = %s))
              AND t.work_date >= current_date - interval '30 days'
              AND t.project_code IS NOT NULL AND t.project_code != ''
            GROUP BY t.project_code
            ORDER BY last_date DESC
            LIMIT 5
        ) sub
    """, (user_id, user_id), db=CENTRAL_DB)
    
    seen = {r["project_code"] for r in recent}
    
    # Also get active "real" projects (4-digit, not overhead)
    active = query_dicts("""
        SELECT project_no as project_code, project_name
        FROM next_project_economy
        WHERE length(project_no) = 4
          AND project_no NOT IN ('0', '00', '10', '11', '12', '20', '100', '101')
          AND project_no NOT LIKE '100%'
          AND project_no != '99999'
        ORDER BY project_no DESC
        LIMIT 9
    """, db=CENTRAL_DB)
    
    for a in active:
        if a["project_code"] not in seen:
            recent.append(a)
            seen.add(a["project_code"])
        if len(recent) >= 9:
            break
    
    return recent[:9]  # Max 9 projects + 1 skip = 10 rows (Meta limit)


def get_users_without_report_today():
    """Check which eligible users haven't reported hours today."""
    users = get_sweep_users()
    today = date.today().isoformat()
    
    reported = query_dicts("""
        SELECT DISTINCT user_id FROM time_report WHERE work_date = %s
    """, (today,), db=CENTRAL_DB)
    reported_ids = {r["user_id"] for r in reported}
    
    return [u for u in users if u["id"] not in reported_ids]


# ---------------------------------------------------------------------------
# WhatsApp interactive messages
# ---------------------------------------------------------------------------
def send_list_message(to_phone: str, body_text: str, button_text: str, sections: list):
    """Send WhatsApp interactive list message.
    sections: [{title: str, rows: [{id, title, description}]}]
    """
    payload = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "interactive",
        "interactive": {
            "type": "list",
            "body": {"text": body_text},
            "action": {
                "button": button_text,
                "sections": sections,
            }
        }
    }
    
    resp = requests.post(
        f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages",
        headers={
            "Authorization": f"Bearer {META_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=15,
    )
    
    if resp.status_code in (200, 201):
        print(f"  List message sent to {to_phone}")
        return True
    else:
        print(f"  List send FAILED: {resp.status_code} {resp.text[:200]}")
        return False


def send_text(to_phone: str, text: str):
    """Send plain text WhatsApp message."""
    resp = requests.post(
        f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages",
        headers={
            "Authorization": f"Bearer {META_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        },
        json={
            "messaging_product": "whatsapp",
            "to": to_phone,
            "type": "text",
            "text": {"body": text}
        },
        timeout=15,
    )
    return resp.status_code in (200, 201)


# ---------------------------------------------------------------------------
# Sweep: send project list to users
# ---------------------------------------------------------------------------
def run_sweep(target_wa_id=None):
    """Send end-of-day sweep to eligible users."""
    _ensure_pending_table()
    
    if target_wa_id:
        # Test mode: send to specific user
        rows = query_dicts("""
            SELECT pu.id, pu.username, pu.display_name, pu.role, pu.phone
            FROM portal_user pu
            WHERE REPLACE(pu.phone, '0', '') = REPLACE(%s, '46', '')
               OR '46' || LTRIM(pu.phone, '0') = %s
            LIMIT 1
        """, (target_wa_id, target_wa_id), db=CENTRAL_DB)
        if not rows:
            print(f"No user found for {target_wa_id}")
            return
        users = [{"wa_id": target_wa_id, **rows[0]}]
    else:
        users = get_users_without_report_today()
    
    if not users:
        print("All users have reported today. No sweep needed.")
        return
    
    print(f"Sweep: {len(users)} users to prompt")
    
    for user in users:
        projects = get_user_projects(user["id"], user["wa_id"])
        
        if not projects:
            print(f"  {user['display_name']}: no active projects, skipping")
            continue
        
        # Build list sections
        rows = []
        for p in projects:
            rows.append({
                "id": f"time_{p['project_code']}",
                "title": p["project_name"][:24],
                "description": f"Kod: {p['project_code']}",
            })
        
        # Add "Ingen tid idag" option
        rows.append({
            "id": "time_SKIP",
            "title": "Ingen tid idag",
            "description": "Hoppa över",
        })
        
        sections = [{
            "title": "Projekt",
            "rows": rows,
        }]
        
        body = f"Hej {user['display_name'].split()[0]}! Har du rapporterat tid idag? Välj projekt nedan."
        
        ok = send_list_message(
            user["wa_id"],
            body,
            "Välj projekt",
            sections,
        )
        
        if ok:
            print(f"  Sent to {user['display_name']} ({user['wa_id']})")
        else:
            print(f"  FAILED for {user['display_name']}")


# ---------------------------------------------------------------------------
# Handle interactive reply (called from whatsapp_webhook.py)
# ---------------------------------------------------------------------------
def handle_time_list_reply(wa_id: str, sender_name: str, row_id: str):
    """Handle when user selects a project from the sweep list.
    Returns True if handled, False if not a time sweep reply."""
    
    if not row_id.startswith("time_"):
        return False
    
    project_code = row_id.replace("time_", "")
    
    if project_code == "SKIP":
        send_text(wa_id, "OK, ingen tid registrerad idag.")
        return True
    
    # Look up project name
    rows = query_dicts(
        "SELECT project_name FROM next_project_economy WHERE project_no = %s LIMIT 1",
        (project_code,), db=CENTRAL_DB
    )
    project_name = rows[0]["project_name"] if rows else project_code
    
    # Look up user
    phone_suffix = wa_id.replace("46", "0")  # 46707... → 0707...
    user_rows = query_dicts(
        "SELECT id, display_name FROM portal_user WHERE phone = %s OR phone = %s LIMIT 1",
        (phone_suffix, wa_id), db=CENTRAL_DB
    )
    user_id = user_rows[0]["id"] if user_rows else None
    person = user_rows[0]["display_name"] if user_rows else sender_name
    
    # Set pending state
    set_pending(wa_id, user_id, person, project_code, project_name)
    
    # Ask for hours
    send_text(wa_id, f"Hur många timmar på {project_name} idag?")
    
    return True


def handle_pending_hours(wa_id: str, sender_name: str, text: str, msg_id: str):
    """Check if user has a pending time prompt, and if the text is hours.
    Returns True if handled, False if not."""
    
    pending = get_pending(wa_id)
    if not pending:
        return False
    
    # Try to parse hours from text
    hours = _parse_hours(text)
    if hours is None:
        return False
    
    if hours <= 0 or hours > 24:
        send_text(wa_id, "Ange timmar mellan 0.5 och 24.")
        return True
    
    # Save time report
    from ata_handler import _phone_to_user
    
    user_info = _phone_to_user(wa_id)
    user_id = user_info.get("user_id") if user_info else pending.get("user_id")
    person = pending.get("person") or sender_name
    profession_code = user_info.get("profession_code", "MONTÖR") if user_info else "MONTÖR"
    
    # Look up cost/price
    rate_rows = query_dicts(
        "SELECT cost_per_hour, price_per_hour FROM profession_rate WHERE profession_code = %s LIMIT 1",
        (profession_code,), db=CENTRAL_DB
    )
    cost_unit = float(rate_rows[0]["cost_per_hour"]) if rate_rows else 0
    price_unit = float(rate_rows[0]["price_per_hour"]) if rate_rows else 0
    
    new_id = execute("""
        INSERT INTO time_report (
            company_code, person, user_id, project_code, work_date, hours,
            category, notes, source, profession_code,
            cost_unit, price_unit, total_cost, total_revenue,
            whatsapp_message_id
        ) VALUES (
            'RM', %s, %s, %s, current_date, %s,
            'arbete', %s, 'whatsapp', %s,
            %s, %s, %s, %s,
            %s
        )
    """, (
        person, user_id, pending["project_code"], hours,
        f"Sweep: {pending['project_name']}", profession_code,
        cost_unit, price_unit, round(hours * cost_unit, 2), round(hours * price_unit, 2),
        msg_id,
    ), db=CENTRAL_DB, returning=True)
    
    resolve_pending(wa_id)
    
    # Confirm
    send_text(wa_id, f"Tid registrerad\nProjekt: {pending['project_name']}\nTimmar: {hours}\nDatum: idag")
    
    print(f"  Sweep time report: {hours}h on {pending['project_name']} by {person} (id={new_id})")
    return True


def _parse_hours(text: str):
    """Parse hours from a text reply. Supports: 8, 8.5, 8,5, heldag, halvdag."""
    text = text.strip().lower()
    
    if text in ("heldag", "hel dag"):
        return 8.0
    if text in ("halvdag", "halv dag"):
        return 4.0
    
    # Try numeric: "8", "8.5", "8,5", "7.5h", "8 timmar"
    m = re.match(r'^(\d+[.,]?\d*)\s*(?:h|tim|timmar)?$', text)
    if m:
        return float(m.group(1).replace(',', '.'))
    
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if "--check" in sys.argv:
        users = get_users_without_report_today()
        print(f"Internal users without report today: {len(users)}")
        for u in users:
            print(f"  {u['display_name']} ({u['role']}) — {u['wa_id']}")
        # UE check
        try:
            from supplier_time_handler import get_ue_workers_without_report_today
            ue = get_ue_workers_without_report_today()
            print(f"UE workers without report today: {len(ue)}")
            for w in ue:
                print(f"  {w['worker_name']} ({w['supplier_name']}) — {w['phone']}")
        except Exception as e:
            print(f"UE check error: {e}")
    elif "--test" in sys.argv:
        idx = sys.argv.index("--test")
        target = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        if target:
            run_sweep(target_wa_id=target)
        else:
            print("Usage: time_sweep.py --test 46707830063")
    else:
        # Run both internal and UE sweep
        run_sweep()
        try:
            from supplier_time_handler import run_ue_sweep
            run_ue_sweep()
        except Exception as e:
            print(f"UE sweep error: {e}")
