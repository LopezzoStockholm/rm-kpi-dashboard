"""
ata_handler.py — Handles ÄTA, beslut, avvikelse and dagbok entries.

Receives classified messages from whatsapp_webhook.py and:
1. Inserts into ata_register or project_log
2. Sends WhatsApp confirmation back to sender (ÄTA number)
3. Creates Planner follow-up task if needed
4. Sends Teams notification to relevant channel
5. Downloads and stores photos if present

All database operations go through rm_data module with parameterized queries.
"""

import json
import re
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rm_data import query_one, query_dicts, execute
from message_parser import (
    load_companies_via_psql, load_users, load_buckets,
    match_project, match_assignee, _get_token
)

# ── Config ──
CENTRAL_DB = "rm_central"
PLANNER_CONFIG = Path("/opt/rm-infra/planner-config.json")
NOTIFICATION_CONFIG = Path("/opt/rm-infra/notification-config.json")
META_CONFIG = Path("/opt/rm-infra/whatsapp-meta-config.json")
PHONE_NUMBER_ID = "1087867614408215"  # RM WhatsApp sender


# ══════════════════════════════════════════════════════════════
# Project matching
# ══════════════════════════════════════════════════════════════

# ── Media buffer correlation (temporal photo ↔ text matching) ──
_MEDIA_WINDOW_SECONDS = 120

def attach_buffered_media(sender_phone: str, target_table: str, target_id: int) -> int:
    """Attach unconsumed photos from media_buffer to a just-created target row.

    Finds media_buffer rows from same sender within last 120s, appends their
    photo_url to target_table.photo_urls, marks them consumed. Returns count attached.
    """
    if not sender_phone or not target_id:
        return 0
    try:
        # Normalize sender variants
        norm = sender_phone.lstrip('+').lstrip('0')
        rows = query_dicts(
            """SELECT id, photo_url FROM media_buffer
               WHERE consumed_by_id IS NULL
                 AND created_at >= now() - interval '%s seconds'
                 AND (sender_phone = %%s
                      OR regexp_replace(sender_phone, '^[+0]+','') = %%s)
               ORDER BY created_at ASC""" % _MEDIA_WINDOW_SECONDS,
            (sender_phone, norm),
            db=CENTRAL_DB
        )
        if not rows:
            return 0
        urls = [r['photo_url'] for r in rows]
        buffer_ids = [r['id'] for r in rows]
        # Append to target row's photo_urls (array column)
        if target_table == 'ata_register':
            execute(
                "UPDATE ata_register SET photo_urls = COALESCE(photo_urls,'{}') || %s::text[] WHERE id = %s",
                (urls, target_id), db=CENTRAL_DB
            )
        elif target_table == 'project_log':
            execute(
                "UPDATE project_log SET photo_urls = COALESCE(photo_urls,'{}') || %s::text[] WHERE id = %s",
                (urls, target_id), db=CENTRAL_DB
            )
        else:
            return 0
        # Mark consumed
        execute(
            "UPDATE media_buffer SET consumed_by_table=%s, consumed_by_id=%s, consumed_at=now() WHERE id = ANY(%s)",
            (target_table, target_id, buffer_ids), db=CENTRAL_DB
        )
        print(f"  attached {len(urls)} buffered photo(s) to {target_table}.{target_id}")
        return len(urls)
    except Exception as e:
        print(f"  WARNING: attach_buffered_media failed: {e}")
        return 0


def find_recent_open_ata(sender_phone: str) -> Optional[dict]:
    """Find recent ata_register row from same sender within 120s without photos.
    Returns {'id': x, 'ata_number': 'ÄTA-...'} or None."""
    if not sender_phone:
        return None
    norm = sender_phone.lstrip('+').lstrip('0')
    try:
        # Widened to 30 min window; allow re-attach to ÄTA that already has photos.
        # User flow: report ÄTA by text, then send photo(s) shortly after to same ÄTA.
        rows = query_dicts(
            """SELECT id, ata_number FROM ata_register
               WHERE created_at >= now() - interval '1800 seconds'
                 AND status IN ('reported','pending_project','approved_internal')
                 AND (reported_by_phone = %s
                      OR regexp_replace(reported_by_phone, '^[+0]+','') = %s)
               ORDER BY created_at DESC LIMIT 1""",
            (sender_phone, norm),
            db=CENTRAL_DB
        )
        return rows[0] if rows else None
    except Exception as e:
        print(f"  WARNING: find_recent_open_ata failed: {e}")
        return None


def attach_photo_to_ata(ata_id: int, photo_url: str) -> bool:
    """Append single photo to an existing ata_register row."""
    try:
        execute(
            "UPDATE ata_register SET photo_urls = COALESCE(photo_urls,'{}') || ARRAY[%s]::text[] WHERE id = %s",
            (photo_url, ata_id), db=CENTRAL_DB
        )
        return True
    except Exception as e:
        print(f"  WARNING: attach_photo_to_ata failed: {e}")
        return False



def match_project_for_ata(text: str) -> dict:
    """Match text to project. Returns {project_code, project_name} or empty."""
    companies = load_companies_via_psql()
    buckets = load_buckets()
    result = match_project(text, companies, buckets)
    if result:
        # Try to find project_id from next_project_economy
        project_name = result[1]
        pid = query_one(
            "SELECT project_no FROM next_project_economy WHERE project_name ILIKE %s LIMIT 1",
            (f"%{project_name}%",),
            db=CENTRAL_DB
        )
        return {
            'project_code': pid if pid else None,
            'project_name': project_name,
        }

    # Fallback: fuzzy match against next_project_economy.project_name
    # Catches typos ("grimmvägen" -> "Grimvägen") and projects not in Twenty/Planner
    try:
        from thefuzz import fuzz
        from rm_data import query_dicts
        rows = query_dicts(
            "SELECT DISTINCT project_no, project_name FROM next_project_economy "
            "WHERE project_name IS NOT NULL",
            db=CENTRAL_DB
        )
        text_lower = text.lower()
        best = None  # (score, project_id, project_name)
        for r in rows:
            pname = (r.get('project_name') or '').strip()
            if not pname:
                continue
            # partial_ratio handles "grimmvägen" in longer text, token_set for word order
            score = max(
                fuzz.partial_ratio(pname.lower(), text_lower),
                fuzz.token_set_ratio(pname.lower(), text_lower),
            )
            if score >= 80 and (best is None or score > best[0]):
                best = (score, r.get('project_no'), pname)
        if best:
            return {'project_code': best[1], 'project_name': best[2]}
    except Exception as e:
        print(f"  WARNING: project fallback match failed: {e}")

    return {'project_code': None, 'project_name': None}


# ══════════════════════════════════════════════════════════════
# ÄTA handling
# ══════════════════════════════════════════════════════════════

def handle_ata(text: str, sender_name: str, sender_phone: str,
               msg_id: str, estimated_amount: Optional[float] = None,
               photo_urls: Optional[list] = None) -> dict:
    """Register a new ÄTA from a WhatsApp message.

    Returns: {ata_number, ata_id, project_name, estimated_amount}
    """
    proj = match_project_for_ata(text)

    # Generate ÄTA number
    ata_number = query_one("SELECT next_ata_number()", db=CENTRAL_DB)
    if not ata_number:
        ata_number = f"ÄTA-{datetime.now().year}-XXX"
        print(f"  WARNING: Could not generate ÄTA number, using fallback")

    # Clean description: full original text
    description = text.strip()

    sql = """INSERT INTO ata_register
        (ata_number, company_code, project_code, project_name, description,
         estimated_amount, status, reported_by, reported_by_phone,
         photo_urls, whatsapp_message_id)
    VALUES (
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s
    ) RETURNING id"""

    params = (
        ata_number,
        'RM',
        proj['project_code'],
        proj['project_name'],
        description,
        estimated_amount,
        'reported',
        sender_name,
        sender_phone,
        photo_urls,  # psycopg2 converts Python list to TEXT[] automatically
        msg_id
    )

    ata_id = execute(sql, params, db=CENTRAL_DB, returning=True)

    # Attach any recently buffered photos from same sender
    attach_buffered_media(sender_phone, 'ata_register', ata_id)

    result = {
        'ata_number': ata_number,
        'ata_id': ata_id,
        'project_code': proj['project_code'],
        'project_name': proj['project_name'],
        'estimated_amount': estimated_amount,
        'description': description,
    }

    print(f"  ÄTA registered: {ata_number} | project={proj['project_name']} | amount={estimated_amount}")

    # Send WhatsApp confirmation
    _send_whatsapp_confirmation_ata(sender_phone, ata_number, proj.get('project_code'), proj['project_name'], estimated_amount)

    # Teams notification
    _notify_teams_ata(result, sender_name, sender_phone)

    return result


# ══════════════════════════════════════════════════════════════
# Beslut / Avvikelse / Dagbok handling
# ══════════════════════════════════════════════════════════════

def handle_project_log(text: str, log_type: str, sender_name: str, sender_phone: str,
                       msg_id: str, severity: str = 'normal',
                       photo_urls: Optional[list] = None,
                       related_ata_id: Optional[int] = None) -> dict:
    """Insert a project log entry (beslut, avvikelse, dagbok).

    Returns: {log_id, log_type, project_name}
    """
    proj = match_project_for_ata(text)

    # Generate short title from first ~80 chars
    title = text.strip()[:80]
    if len(text.strip()) > 80:
        title = title.rsplit(' ', 1)[0] + '...'

    sql = """INSERT INTO project_log
        (company_code, project_code, project_name, log_type, title, description,
         severity, reported_by, reported_by_phone, photo_urls,
         whatsapp_message_id, related_ata_id)
    VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s
    ) RETURNING id"""

    params = (
        'RM',
        proj['project_code'],
        proj['project_name'],
        log_type,
        title,
        text.strip(),
        severity,
        sender_name,
        sender_phone,
        photo_urls,  # psycopg2 converts Python list to TEXT[] automatically
        msg_id,
        related_ata_id
    )

    log_id = execute(sql, params, db=CENTRAL_DB, returning=True)

    # Attach buffered photos from same sender
    attach_buffered_media(sender_phone, 'project_log', log_id)

    result = {
        'log_id': log_id,
        'log_type': log_type,
        'title': title,
        'project_code': proj['project_code'],
        'project_name': proj['project_name'],
        'severity': severity,
    }

    print(f"  Project log: [{log_type}] {title} | project={proj['project_name']} | severity={severity}")

    # Teams notification for avvikelse (important/critical) and beslut
    if log_type in ('avvikelse', 'beslut') or severity in ('important', 'critical'):
        _notify_teams_log(result, sender_name, sender_phone, text)

    # WhatsApp confirmation for avvikelse
    if log_type == 'avvikelse':
        _send_whatsapp_confirmation_log(sender_phone, log_type, title, proj.get('project_code'), proj['project_name'])

    return result


# ══════════════════════════════════════════════════════════════
# Time report handling (WhatsApp → time_report table)
# ══════════════════════════════════════════════════════════════

def _phone_to_user(sender_phone: str) -> Optional[dict]:
    """Look up portal_user by phone number. Returns {id, display_name, profession_code} or None."""
    from rm_data import query_dicts
    # Normalize: strip leading + and 00, try Swedish format
    digits = sender_phone.lstrip('+').lstrip('0')
    rows = query_dicts(
        """SELECT pu.id, pu.display_name, pu.role
           FROM portal_user pu
           WHERE pu.active = true
             AND (pu.phone = %s OR pu.phone = %s OR pu.phone = %s OR pu.phone = %s)
           LIMIT 1""",
        (sender_phone, digits, '0' + digits[-9:] if len(digits) >= 9 else digits,
         '+46' + digits[-9:] if len(digits) >= 9 else digits),
        db=CENTRAL_DB
    )
    if rows:
        r = rows[0]
        # Map role to default profession_code
        role_map = {'vd': 'VD', 'projektledare': 'PL', 'ekonomi': 'ADMIN'}
        return {
            'id': r['id'],
            'display_name': r['display_name'],
            'profession_code': role_map.get(r['role'], 'MONTÖR'),
        }
    return None


def handle_time_report(text: str, sender_name: str, sender_phone: str,
                       msg_id: str, hours: Optional[float] = None) -> dict:
    """Insert a time report from WhatsApp message.

    Parses: project (fuzzy match), hours, optional notes.
    Returns: {report_id, hours, project_name, project_code}
    """
    from rm_data import query_dicts
    proj = match_project_for_ata(text)

    # Look up user
    user = _phone_to_user(sender_phone)
    user_id = user['id'] if user else None
    person_name = user['display_name'] if user else sender_name
    profession = user['profession_code'] if user else None

    # Get profession rate for auto cost calculation
    cost_unit = None
    price_unit = None
    total_cost = None
    total_revenue = None
    if profession:
        rate_rows = query_dicts(
            "SELECT cost_per_hour, price_per_hour FROM profession_rate WHERE company_code='RM' AND profession_code=%s AND active=true",
            (profession,),
            db=CENTRAL_DB
        )
        if rate_rows:
            cost_unit = float(rate_rows[0]['cost_per_hour'])
            price_unit = float(rate_rows[0]['price_per_hour'])
            if hours:
                total_cost = round(cost_unit * hours, 2)
                total_revenue = round(price_unit * hours, 2)

    # Extract notes (text minus the hours/project part — keep full text as notes)
    notes = text.strip()

    import datetime
    work_date = datetime.date.today().isoformat()

    sql = """INSERT INTO time_report
        (company_code, user_id, person, project_code, next_project_no, work_date, hours,
         category, notes, profession_code, cost_unit, price_unit, total_cost, total_revenue,
         source, whatsapp_message_id, updated_at)
    VALUES (
        'RM', %s, %s, %s, %s, %s, %s,
        'arbete', %s, %s, %s, %s, %s, %s,
        'whatsapp', %s, now()
    ) RETURNING id"""

    params = (
        user_id, person_name,
        proj['project_code'], proj['project_code'],
        work_date, hours or 0,
        notes, profession,
        cost_unit, price_unit, total_cost, total_revenue,
        msg_id,
    )

    report_id = execute(sql, params, db=CENTRAL_DB, returning=True)

    result = {
        'report_id': report_id,
        'hours': hours,
        'project_code': proj['project_code'],
        'project_name': proj['project_name'],
        'person': person_name,
    }

    print(f"  Time report: {hours}h on {proj['project_name']} by {person_name} (id={report_id})")

    # WhatsApp confirmation
    _send_whatsapp_confirmation_time(sender_phone, hours, proj['project_code'], proj['project_name'])

    return result


def _send_whatsapp_confirmation_time(phone: str, hours: Optional[float],
                                      project_code: Optional[str], project_name: str):
    """Send confirmation of time report registration."""
    lines = [
        f"Tid registrerad",
        f"Projekt: {project_name}" + (f" ({project_code})" if project_code else ""),
        f"Timmar: {hours}" if hours else "Timmar: (ej angivet)",
        f"Datum: idag",
    ]
    _send_whatsapp_text(phone, "\n".join(lines))


# ══════════════════════════════════════════════════════════════
# Photo handling
# ══════════════════════════════════════════════════════════════

def download_whatsapp_image(media_id: str) -> Optional[str]:
    """Download image from WhatsApp and return the media URL (for storage reference).

    We store the Meta CDN URL. For permanent storage, a future version
    could upload to Azure Blob / S3.
    """
    try:
        config = json.loads(META_CONFIG.read_text())
        token = config.get("access_token", "")
    except:
        return None

    # Get media URL
    resp = requests.get(
        f"https://graph.facebook.com/v21.0/{media_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10
    )
    if resp.status_code != 200:
        print(f"  Image download failed: {resp.status_code}")
        return None

    media_url = resp.json().get("url")
    if media_url:
        print(f"  Image URL retrieved: {media_id}")
    return media_url


# ══════════════════════════════════════════════════════════════
# WhatsApp confirmations
# ══════════════════════════════════════════════════════════════

def _send_whatsapp_text(to_phone: str, message: str):
    """Send a plain text WhatsApp message via Cloud API."""
    try:
        config = json.loads(META_CONFIG.read_text())
        token = config.get("access_token", "")
    except:
        print("  WARNING: Could not load Meta config for WhatsApp reply")
        return False

    # Remove + prefix for API
    phone = to_phone.lstrip('+')

    resp = requests.post(
        f"https://graph.facebook.com/v21.0/{PHONE_NUMBER_ID}/messages",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={
            "messaging_product": "whatsapp",
            "to": phone,
            "type": "text",
            "text": {"body": message}
        },
        timeout=15
    )

    if resp.status_code in (200, 201):
        print(f"  WhatsApp confirmation sent to {to_phone}")
        return True
    else:
        print(f"  WhatsApp send FAILED: {resp.status_code} {resp.text[:200]}")
        return False


def _send_whatsapp_confirmation_ata(phone: str, ata_number: str,
                                     project_code: Optional[str],
                                     project_name: Optional[str],
                                     amount: Optional[float]):
    """Send ÄTA registration confirmation back to sender."""
    lines = [f"ÄTA registrerad: {ata_number}"]
    if project_code and project_name:
        lines.append(f"Projekt: {project_code} {project_name}")
    elif project_name:
        lines.append(f"Projekt: {project_name} (projektnummer saknas — svara med projektnummer)")
    else:
        lines.append("OBS: projekt saknas — svara med projektnummer eller namn")
    if amount:
        lines.append(f"Uppskattat belopp: {amount:,.0f} kr")
    lines.append("Status: Rapporterad (ej godkänd)")
    lines.append("")
    lines.append("Skicka foto som svar pa detta meddelande for att koppla till denna ÄTA.")

    _send_whatsapp_text(phone, "\n".join(lines))


def _send_whatsapp_confirmation_log(phone: str, log_type: str,
                                     title: str,
                                     project_code: Optional[str],
                                     project_name: Optional[str]):
    """Send avvikelse/beslut confirmation."""
    type_sv = {'avvikelse': 'Avvikelse', 'beslut': 'Beslut', 'dagbok': 'Dagbok'}.get(log_type, log_type)
    lines = [f"{type_sv} registrerad"]
    if project_code and project_name:
        lines.append(f"Projekt: {project_code} {project_name}")
    elif project_name:
        lines.append(f"Projekt: {project_name}")
    lines.append(f"Titel: {title}")

    _send_whatsapp_text(phone, "\n".join(lines))


# ══════════════════════════════════════════════════════════════
# Teams notifications
# ══════════════════════════════════════════════════════════════

def _load_webhook_url(target_channel: str = "Aktiva Projekt") -> Optional[str]:
    """Load Teams webhook URL from notification config."""
    try:
        config = json.loads(NOTIFICATION_CONFIG.read_text())
        # Search plan_targets for matching channel
        for plan_id, info in config.get("plan_targets", {}).items():
            for target in info.get("targets", []):
                if target.get("channel_name") == target_channel:
                    return target.get("url")
        # Fallback: default target
        default = config.get("default_target", {})
        return default.get("url")
    except:
        return None


def _notify_teams_ata(ata_result: dict, sender_name: str, sender_phone: str):
    """Send Teams card for new ÄTA."""
    webhook_url = _load_webhook_url("Aktiva Projekt")
    if not webhook_url:
        print("  No Teams webhook for ÄTA notification")
        return

    facts = [
        {"title": "ÄTA-nummer", "value": ata_result['ata_number']},
        {"title": "Rapporterad av", "value": f"{sender_name} ({sender_phone})"},
    ]
    if ata_result.get('project_name'):
        facts.append({"title": "Projekt", "value": ata_result['project_name']})
    if ata_result.get('estimated_amount'):
        facts.append({"title": "Uppskattat belopp", "value": f"{ata_result['estimated_amount']:,.0f} kr"})

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"Ny ÄTA: {ata_result['ata_number']}",
                        "weight": "bolder",
                        "size": "medium",
                        "color": "warning",
                        "wrap": True
                    },
                    {"type": "FactSet", "facts": facts},
                    {
                        "type": "TextBlock",
                        "text": f"_{ata_result['description'][:300]}_",
                        "wrap": True,
                        "isSubtle": True,
                        "size": "small"
                    }
                ]
            }
        }]
    }

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        if resp.status_code == 202:
            print(f"  Teams ÄTA notification sent")
        else:
            print(f"  Teams ÄTA notification FAILED: {resp.status_code}")
    except Exception as e:
        print(f"  Teams ÄTA notification error: {e}")


def _notify_teams_log(log_result: dict, sender_name: str, sender_phone: str, full_text: str):
    """Send Teams card for avvikelse/beslut."""
    webhook_url = _load_webhook_url("Aktiva Projekt")
    if not webhook_url:
        return

    type_sv = {'avvikelse': 'Avvikelse', 'beslut': 'Beslut'}.get(log_result['log_type'], log_result['log_type'])
    color = 'attention' if log_result.get('severity') in ('critical', 'important') else 'default'

    facts = [
        {"title": "Typ", "value": type_sv},
        {"title": "Rapporterad av", "value": f"{sender_name} ({sender_phone})"},
    ]
    if log_result.get('project_name'):
        facts.append({"title": "Projekt", "value": log_result['project_name']})
    if log_result.get('severity') and log_result['severity'] != 'normal':
        facts.append({"title": "Allvarlighetsgrad", "value": log_result['severity'].upper()})

    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": f"{type_sv}: {log_result['title']}",
                        "weight": "bolder",
                        "size": "medium",
                        "color": color,
                        "wrap": True
                    },
                    {"type": "FactSet", "facts": facts},
                    {
                        "type": "TextBlock",
                        "text": f"_{full_text[:300]}_",
                        "wrap": True,
                        "isSubtle": True,
                        "size": "small"
                    }
                ]
            }
        }]
    }

    try:
        resp = requests.post(webhook_url, json=card, timeout=15)
        if resp.status_code == 202:
            print(f"  Teams {type_sv} notification sent")
        else:
            print(f"  Teams {type_sv} notification FAILED: {resp.status_code}")
    except Exception as e:
        print(f"  Teams {type_sv} notification error: {e}")
