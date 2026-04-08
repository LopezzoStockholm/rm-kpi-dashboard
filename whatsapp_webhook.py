#!/usr/bin/env python3
"""
whatsapp_webhook.py — Flask endpoint receiving WhatsApp messages.

Architecture:
    WhatsApp → Meta Cloud API → POST /webhook/whatsapp → classify → route:
        ÄTA/beslut/avvikelse/dagbok → ata_handler → DB + WhatsApp confirmation + Teams
        task → parse → Planner → Teams

v5 changes (2026-04-03):
- Intelligent message classification: ÄTA, beslut, avvikelse, dagbok, task
- ÄTA register with auto-numbering (ÄTA-2026-001)
- Project log for beslut/avvikelse/dagbok
- WhatsApp image handling — photos linked to ÄTA/avvikelse
- WhatsApp confirmation messages back to sender
- Teams notifications with AdaptiveCards for all types

v4 changes (2026-04-02):
- Whisper serialization lock — prevents concurrent transcriptions and CPU contention
- Sender info (name + phone) included in Teams notifications and Planner task description
- Phone → name contact mapping via whatsapp-contacts.json
- Added "pudde" to known names for split detection

v3 changes (2026-04-02):
- Whisper medium model (was small) — dramatically better Swedish transcription
- Optimized beam_size=5, initial_prompt for Swedish business context
- Multi-task splitting: one voice message → multiple Planner tasks
- Assignee-aware splitting with numbered, ordinal, sequence, and name-boundary detection

v6 changes (2026-04-03):
- Atomic dedup via INSERT ... ON CONFLICT — idempotent message handling
- Replaced _processing_lock + _processing_ids with database-driven dedup
- Race condition fixed: claim_message() atomically claims msg_id or returns False
- SQL DDL: CREATE TABLE whatsapp_message_dedup (msg_id TEXT PRIMARY KEY, received_at TIMESTAMPTZ DEFAULT NOW())
"""

import json
import subprocess
import requests
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from pathlib import Path

from message_parser import parse_message, create_planner_task, update_planner_task_description, load_companies_via_psql, _get_token
from ata_classifier import classify_message
from ata_handler import find_recent_open_ata, attach_photo_to_ata, handle_ata, handle_project_log, handle_time_report, download_whatsapp_image

# ── Voice transcription ──
import tempfile
import os
from faster_whisper import WhisperModel

# ── Database ──
import sys
sys.path.insert(0, '/opt/rm-infra')
from rm_data import query_one, query_dicts, execute, safe_json_query

# ── Config ──
CONFIG_DIR = Path("/opt/rm-infra/config")
AUTHORIZED_FILE = CONFIG_DIR / "whatsapp-authorized-senders.json"
CONTACTS_FILE = CONFIG_DIR / "whatsapp-contacts.json"
META_CONFIG = CONFIG_DIR / "whatsapp-meta-config.json"
WHISPER_CONFIG = CONFIG_DIR / "whisper-config.json"
RM_CONFIG = CONFIG_DIR / "rm-master-config.json"
CENTRAL_DB = "rm_central"
DB_USER = "rmadmin"

# ── Whisper ──
WHISPER_DEVICE = "cuda"
WHISPER_MODEL = "medium"
WHISPER_BEAM = 5
WHISPER_LANGUAGE = "sv"

# ── WhatsApp ──
app = Flask(__name__)
WEBHOOK_TOKEN = None
BUSINESS_ACCOUNT_ID = None
PHONE_NUMBER_ID = None
META_ACCESS_TOKEN = None
COMPANY_ASSIGNMENTS = {}
NOTIFICATION_PLAN_ASSIGNMENTS = {}



# =============================================================================
# Initialization
# =============================================================================

def load_config():
    """Load all configuration files."""
    global WEBHOOK_TOKEN, BUSINESS_ACCOUNT_ID, PHONE_NUMBER_ID, META_ACCESS_TOKEN
    global COMPANY_ASSIGNMENTS, NOTIFICATION_PLAN_ASSIGNMENTS, WHISPER_DEVICE, WHISPER_MODEL, WHISPER_BEAM

    try:
        if META_CONFIG.exists():
            meta_cfg = json.loads(META_CONFIG.read_text())
            WEBHOOK_TOKEN = meta_cfg.get("webhook_verify_token")
            BUSINESS_ACCOUNT_ID = meta_cfg.get("business_account_id") or meta_cfg.get("business_id")
            PHONE_NUMBER_ID = meta_cfg.get("phone_number_id")
            META_ACCESS_TOKEN = meta_cfg.get("access_token")
        if RM_CONFIG.exists():
            rm_cfg = json.loads(RM_CONFIG.read_text())
            COMPANY_ASSIGNMENTS = rm_cfg.get("whatsapp_company_routing", {})
            NOTIFICATION_PLAN_ASSIGNMENTS = rm_cfg.get("notification_plan_assignments", {})
        if WHISPER_CONFIG.exists():
            whisper_cfg = json.loads(WHISPER_CONFIG.read_text())
            WHISPER_DEVICE = whisper_cfg.get("device", "cuda")
            WHISPER_MODEL = whisper_cfg.get("model", "medium")
            WHISPER_BEAM = whisper_cfg.get("beam_size", 5)
    except Exception as e:
        print(f"WARNING: Failed to load config: {e}")


load_config()

# Load Whisper model once
try:
    print(f"Loading Whisper {WHISPER_MODEL} on {WHISPER_DEVICE}...")
    whisper_model = WhisperModel(WHISPER_MODEL, device=WHISPER_DEVICE, compute_type="int8")
    print("Whisper model loaded.")
except Exception as e:
    print(f"ERROR: Failed to load Whisper model: {e}")
    whisper_model = None

# ── Whisper background executor (1 worker = serialized transcription) ──
_whisper_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="whisper")


# =============================================================================
# Configuration loaders
# =============================================================================

def load_contacts_map():
    """Load {phone_number: name} mapping from whatsapp-contacts.json."""
    if CONTACTS_FILE.exists():
        try:
            return json.loads(CONTACTS_FILE.read_text())
        except Exception as e:
            print(f"WARNING: Failed to load contacts: {e}")
    return {}


def get_sender_name_from_phone(sender_phone):
    """Look up sender name by phone number. Normalizes + and 00 prefixes."""
    contacts = load_contacts_map()
    if sender_phone in contacts:
        return contacts[sender_phone]
    # Try normalized variants
    digits = sender_phone.lstrip('+').lstrip('0')
    for key, name in contacts.items():
        key_digits = key.lstrip('+').lstrip('0')
        if key_digits == digits:
            return name
    return sender_phone


def load_notification_targets(plan_id):
    """Load Teams channel URL(s) for plan."""
    if not NOTIFICATION_PLAN_ASSIGNMENTS:
        return []
    targets = NOTIFICATION_PLAN_ASSIGNMENTS.get(plan_id, [])
    if isinstance(targets, str):
        targets = [targets]
    return targets


def load_plan_teams_channels():
    """Load plan → [Teams channels] mapping from config."""
    if not NOTIFICATION_PLAN_ASSIGNMENTS:
        return {}
    return NOTIFICATION_PLAN_ASSIGNMENTS


def load_plan_info(plan_id):
    """Load plan info from config (targets etc). Used by parse_message fallback."""
    cfg = load_plan_teams_channels()
    plan_info = cfg.get(plan_id, {})
    return plan_info


def get_default_targets():
    """Return fallback Teams channel URL(s) if no plan assigned."""
    plan_info = load_plan_info("_default")
    if plan_info:
        targets = plan_info.get("targets", [])
        if targets:
            return targets if isinstance(targets, list) else [targets]
    config = load_plan_teams_channels()
    for plan_id, info in config.items():
        if plan_id != "_default":
            targets = info.get("targets", [])
            if targets:
                return targets if isinstance(targets, list) else [targets]
    return []


def get_targets_for_plan(plan_id):
    """Return Teams targets for a plan."""
    plan_info = load_plan_info(plan_id)
    if plan_info:
        targets = plan_info.get("targets", [])
        if targets:
            return targets if isinstance(targets, list) else [targets]
    default = load_plan_info("_default").get("targets")
    if default:
        return [default] if isinstance(default, str) else (default if default else [])
    return []


def load_authorized():
    if AUTHORIZED_FILE.exists():
        return json.loads(AUTHORIZED_FILE.read_text())
    return {}


def claim_message(msg_id):
    """Attempt to atomically claim a message for processing. Returns True if claimed, False if already exists.

    Uses INSERT ... ON CONFLICT to ensure idempotent handling of duplicate WhatsApp webhooks.
    If the msg_id already exists in whatsapp_message_dedup, the INSERT does nothing and returns None.
    """
    result = execute(
        "INSERT INTO whatsapp_message_dedup (msg_id) VALUES (%s) ON CONFLICT (msg_id) DO NOTHING RETURNING msg_id",
        (msg_id,),
        returning=True
    )
    return result is not None


def log_message(channel, msg_id, raw_text, sender_name, sender_id,
                parsed=None, planner_task_id=None, skipped=False, skip_reason=None,
                subtask_index=None, msg_type='task'):
    """Log message to message_task_log. msg_type added for classification tracking."""
    p = parsed or {}
    log_msg_id = f"{msg_id}_t{subtask_index}" if subtask_index is not None else msg_id

    execute("""INSERT INTO message_task_log
        (source, source_channel, source_message_id, raw_text, sender_name, sender_id,
         parsed_assignee, parsed_assignee_id, parsed_title, parsed_project,
         parsed_bucket_id, parsed_due_date, planner_task_id, confidence, skipped, skip_reason)
    VALUES (
        'whatsapp', %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )""", (
        channel, log_msg_id, raw_text[:500] if raw_text else None, sender_name, sender_id,
        p.get('assignee_name'), p.get('assignee_id'), p.get('title'), p.get('project'),
        p.get('bucket_id'), str(p['due_date']) if p.get('due_date') else None,
        planner_task_id, p.get('confidence'), skipped, skip_reason
    ))


def notify_teams_channel(parsed, plan_id, task_id, sender_name, sender_phone, original_text):
    """Send Teams notification for a new task."""
    targets = load_notification_targets(plan_id)
    if not targets:
        print(f"  No notification targets for plan {plan_id}")
        return

    assignee = parsed.get('assignee_name', 'unassigned')
    title = parsed.get('title', '(no title)')
    project = parsed.get('project', '')

    card = {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "summary": f"New task: {title}",
        "themeColor": "0078D4",
        "sections": [
            {
                "activityTitle": f"New task created",
                "activitySubtitle": f"From: {sender_name}",
                "facts": [
                    {"name": "Title", "value": title},
                    {"name": "Assignee", "value": assignee},
                    {"name": "Project", "value": project},
                    {"name": "Planner task ID", "value": str(task_id)},
                ],
                "text": f"Original message:\n{original_text}",
            }
        ],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "View in Planner",
                "targets": [
                    {"os": "default", "uri": f"https://tasks.microsoft.com/webview/taskslandinghub"}
                ]
            }
        ]
    }

    for target_url in (targets if isinstance(targets, list) else [targets]):
        try:
            requests.post(target_url, json=card, timeout=5)
        except Exception as e:
            print(f"WARNING: Failed to post to Teams: {e}")


# =============================================================================
# WhatsApp Webhooks
# =============================================================================

@app.route("/webhook/whatsapp", methods=["GET"])
def webhook_verify():
    """Webhook verification handshake."""
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == WEBHOOK_TOKEN:
        return challenge, 200

    return "Unauthorized", 403



def sweep_orphan_media():
    """Promote photos older than 120s that are still unconsumed to avvikelse rows.
    Called opportunistically on every incoming message.
    Uses atomic UPDATE to claim rows in a committed transaction so concurrent sweeps don't race."""
    try:
        import psycopg2.extras
        from rm_data import execute as _exec, get_conn
        # Atomic claim in its own connection that we commit explicitly.
        conn = get_conn("rm_central")
        rows = []
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """UPDATE media_buffer
                       SET consumed_by_table='sweep_in_progress', consumed_by_id=0, consumed_at=now()
                       WHERE id IN (
                           SELECT id FROM media_buffer
                           WHERE consumed_by_id IS NULL
                             AND created_at < now() - interval '120 seconds'
                           ORDER BY created_at ASC
                           LIMIT 20
                           FOR UPDATE SKIP LOCKED
                       )
                       RETURNING id, sender_phone, photo_url, whatsapp_message_id"""
                )
                rows = [dict(r) for r in cur.fetchall()]
            conn.commit()
        finally:
            conn.close()
        if not rows:
            return
        for r in rows:
            try:
                sname = get_sender_name_from_phone(r["sender_phone"])
                placeholder = f"(bildrapport utan text från {sname})"
                result = handle_project_log(
                    placeholder, "avvikelse", sname, r["sender_phone"],
                    r.get("whatsapp_message_id") or f"sweep_{r['id']}",
                    severity="normal", photo_urls=[r["photo_url"]],
                )
                # CRITICAL: orphan is >120s old, so attach_buffered_media's 120s window
                # won\'t consume it. Must mark consumed explicitly here to prevent loop.
                log_id = result.get("log_id") if isinstance(result, dict) else None
                if log_id:
                    _exec(
                        "UPDATE media_buffer SET consumed_by_table=%s, consumed_by_id=%s, consumed_at=now() WHERE id=%s",
                        ("project_log", log_id, r["id"]),
                        db="rm_central"
                    )
                print(f"  sweep: promoted buffer id={r['id']} to avvikelse log_id={log_id}")
            except Exception as _se:
                print(f"  sweep error on id={r['id']}: {_se}")
                # Mark as consumed anyway to prevent infinite retry loop
                try:
                    _exec(
                        "UPDATE media_buffer SET consumed_by_table=%s, consumed_by_id=%s, consumed_at=now() WHERE id=%s",
                        ("sweep_error", -1, r["id"]),
                        db="rm_central"
                    )
                except Exception:
                    pass
    except Exception as e:
        print(f"  sweep_orphan_media failed: {e}")


@app.route("/webhook/whatsapp", methods=["POST"])
def webhook_receive():
    """Receive and route WhatsApp messages."""
    data = request.get_json(silent=True)
    if not data:
        return "OK", 200

    # Opportunistic sweep of orphan photos (idempotent, safe to call often)
    try:
        sweep_orphan_media()
    except Exception as _swe:
        print(f"sweep call failed: {_swe}")

    # DEBUG: log raw payload types for diagnostics
    try:
        for _e in data.get("entry", []):
            for _c in _e.get("changes", []):
                _v = _c.get("value", {})
                for _m in _v.get("messages", []):
                    _t = _m.get("type")
                    _has_cap = bool(_m.get(_t, {}).get("caption")) if _t in ("image","video","document") else False
                    print(f"[payload] type={_t} has_caption={_has_cap} keys={list(_m.keys())}")
    except Exception as _le:
        print(f"[payload-debug] {_le}")

    entries = data.get("entry", [])
    for entry in entries:
        changes = entry.get("changes", [])
        for change in changes:
            value = change.get("value", {})
            messages = value.get("messages", [])
            for msg in messages:
                handle_message(msg, value)

    return "OK", 200


def handle_message(msg, metadata):
    """Process a single WhatsApp message."""
    msg_id = msg.get("id")
    sender_id = msg.get("from")
    timestamp = msg.get("timestamp")

    if not msg_id or not sender_id:
        return

    # Track 24h conversation window (UPSERT - idempotent even for duplicate deliveries)
    try:
        _snippet = None
        if msg.get("type") == "text":
            _snippet = (msg.get("text",{}).get("body") or "")[:200]
        execute("""
            INSERT INTO whatsapp_conversation_window (wa_id, last_inbound_at, last_msg_snippet, updated_at)
            VALUES (%s, now(), %s, now())
            ON CONFLICT (wa_id) DO UPDATE
              SET last_inbound_at = EXCLUDED.last_inbound_at,
                  last_msg_snippet = EXCLUDED.last_msg_snippet,
                  updated_at = now()
        """, (sender_id, _snippet))
    except Exception as _wce:
        print(f"[window] update failed: {_wce}")

    # Atomically claim this message for processing
    if not claim_message(msg_id):
        print(f"Message {msg_id} already claimed/processed")
        return

    try:
        sender_name = get_sender_name_from_phone(sender_id)
        authorized = load_authorized()
        if sender_id not in authorized:
            print(f"Sender {sender_id} not authorized")
            return

        msg_type = msg.get("type")
        if msg_type == "text":
            handle_text_message(msg_id, sender_id, sender_name, msg["text"]["body"])
        elif msg_type == "audio":
            handle_audio_message(msg_id, sender_id, sender_name, msg["audio"])
        elif msg_type == "image":
            reply_to = (msg.get("context") or {}).get("id")
            handle_image_message(msg_id, sender_id, sender_name, msg["image"], reply_to_wamid=reply_to)
        elif msg_type == "interactive":
            interactive = msg.get("interactive", {})
            reply_type = interactive.get("type")
            reply_data = interactive.get(reply_type, {}) if reply_type else {}
            reply_id = reply_data.get("id", "")
            reply_title = reply_data.get("title", "")
            print(f"INTERACTIVE {reply_type} from {sender_name}: id={reply_id} title={reply_title}")
            try:
                # Check UE interactive reply first
                from supplier_time_handler import lookup_supplier_worker, handle_supplier_message
                _ue_w = lookup_supplier_worker(sender_id)
                if _ue_w and reply_id.startswith("ue_"):
                    if handle_supplier_message(sender_id, sender_name, "", msg_id,
                                               msg_type="interactive",
                                               interactive_data={"id": reply_id, "title": reply_title}):
                        log_message(sender_id, msg_id, f"interactive:{reply_id}", sender_name, sender_id,
                                    parsed={"type": "ue_interactive"}, msg_type="ue_interactive")
                        print(f"  Handled as UE interactive reply")
                        # skip further processing
                    # fall through if not handled
                from time_sweep import handle_time_list_reply
                if handle_time_list_reply(sender_id, sender_name, reply_id):
                    log_message(sender_id, msg_id, f"interactive:{reply_id}", sender_name, sender_id,
                                parsed={"type": "time_sweep_reply", "project": reply_title}, msg_type="time_sweep")
                else:
                    log_message(sender_id, msg_id, f"interactive:{reply_id}", sender_name, sender_id,
                                skipped=True, skip_reason=f"unhandled interactive {reply_type}")
            except Exception as _ie:
                print(f"ERROR interactive handling: {_ie}")
        else:
            log_message(sender_id, msg_id, f"unsupported type: {msg_type}", sender_name, sender_id, skipped=True, skip_reason=f"type={msg_type}")

    except Exception as e:
        print(f"ERROR handling message {msg_id}: {e}")


def handle_text_message(msg_id, sender_id, sender_name, text, photo_urls=None):
    """Process text message. Classifies first — ÄTA/beslut/avvikelse/dagbok
    route to ata_handler. Task fallback goes to Planner."""
    print(f"TEXT from {sender_name}: {text[:100]}")

    # Check if sender is a UE (supplier) worker — route ALL their messages to UE handler
    try:
        from supplier_time_handler import lookup_supplier_worker, handle_supplier_message
        _ue_worker = lookup_supplier_worker(sender_id)
        if _ue_worker:
            print(f"  UE worker detected: {_ue_worker['worker_name']} ({_ue_worker['supplier_name']})")
            if handle_supplier_message(sender_id, sender_name, text, msg_id, msg_type="text"):
                log_message(sender_id, msg_id, text, sender_name, sender_id,
                            parsed={"type": "ue_time_report"}, msg_type="ue_time_report")
                return
    except Exception as _ue_err:
        print(f"  UE worker check failed: {_ue_err}")

    # Check if user has a pending time sweep reply (awaiting hours)
    try:
        from time_sweep import handle_pending_hours
        if handle_pending_hours(sender_id, sender_name, text, msg_id):
            print(f"  Handled as sweep hours reply")
            log_message(sender_id, msg_id, text, sender_name, sender_id,
                        parsed={"type": "time_sweep_hours"}, msg_type="time_sweep_hours")
            return
    except Exception as _pe:
        print(f"  pending hours check failed: {_pe}")

    # Classify — route non-task messages to ata_handler
    classification = classify_message(text, has_image=bool(photo_urls))
    ctype = classification.get("type")
    conf = classification.get("confidence", 0.0)
    print(f"  classified={ctype} conf={conf} signals={classification.get('signals')}")

    if ctype != "task" and conf >= 0.5:
        try:
            if ctype == "ata":
                result = handle_ata(
                    text, sender_name, sender_id, msg_id,
                    estimated_amount=classification.get("estimated_amount"),
                    photo_urls=photo_urls,
                )
                log_message(
                    sender_id, msg_id, text, sender_name, sender_id,
                    parsed={"title": result.get("ata_number"),
                            "project": result.get("project_name"),
                            "confidence": conf},
                    msg_type="ata",
                )
            elif ctype == "time_report":
                result = handle_time_report(
                    text, sender_name, sender_id, msg_id,
                    hours=classification.get("hours"),
                )
                log_message(
                    sender_id, msg_id, text, sender_name, sender_id,
                    parsed={"title": f"Tid: {result.get('hours')}h {result.get('project_name','')}",
                            "project": result.get("project_name"),
                            "confidence": conf},
                    msg_type="time_report",
                )
            else:
                result = handle_project_log(
                    text, ctype, sender_name, sender_id, msg_id,
                    severity=classification.get("severity") or "normal",
                    photo_urls=photo_urls,
                )
                log_message(
                    sender_id, msg_id, text, sender_name, sender_id,
                    parsed={"title": f"{ctype}: {result.get('project_name','')}",
                            "project": result.get("project_name"),
                            "confidence": conf},
                    msg_type=ctype,
                )
            return
        except Exception as e:
            print(f"ERROR ata/log handling: {e}")
            log_message(
                sender_id, msg_id, text, sender_name, sender_id,
                skipped=True, skip_reason=f"ata_handler_error: {str(e)[:150]}",
            )
            return

    # Task fallback → Task Hub + Planner (parallellt)
    parsed = parse_message(text)
    if not parsed:
        log_message(sender_id, msg_id, text, sender_name, sender_id, skipped=True, skip_reason="parse_failed")
        return

    plan_id = parsed.get("plan_id")
    task_title = parsed.get("title", "(no title)")
    parsed["source_message_id"] = msg_id
    parsed["sender_name"] = sender_name
    parsed["sender_phone"] = sender_id

    # 1. Task Hub (primär destination)
    hub_task_id, hub_col = create_hub_task_from_wa(parsed, sender_id, sender_name, msg_id, text)
    
    # 2. Planner (sekundär, behålls för bakåtkompatibilitet)
    planner_task_id = None
    try:
        planner_task_id = create_planner_task(parsed)
    except Exception as e:
        print(f"WARNING planner task failed (hub has it): {e}")

    # 3. Logga med båda ID:n
    log_parsed = {**parsed}
    if hub_task_id:
        log_parsed["hub_task_id"] = hub_task_id
    log_message(sender_id, msg_id, text, sender_name, sender_id, parsed=log_parsed, planner_task_id=planner_task_id)

    # 4. Teams-notis (oförändrad)
    try:
        notify_teams_channel(parsed, plan_id, planner_task_id or hub_task_id, sender_name, sender_id, text)
    except Exception as e:
        print(f"WARNING teams notification failed: {e}")

    # 5. WhatsApp-bekräftelse till avsändaren
    if hub_task_id:
        assignee = parsed.get("assignee_name")
        send_wa_task_confirmation(sender_id, task_title, hub_col, assignee)



# ─── Task Hub ingest (skalbar extern task-ingress) ───────────────────────────

HUB_INGEST_URL = "http://localhost:8090/api/hub/tasks/ingest"
HUB_INGEST_TOKEN = "rm-internal-2026"

def create_hub_task_from_wa(parsed, sender_id, sender_name, msg_id, original_text):
    """Skicka parsed task till Task Hub via ingest-endpoint.
    
    Skalbar: samma endpoint används av WhatsApp, Teams, email, n8n, API.
    Returnerar (task_id, board_column) eller (None, None) vid fel.
    """
    try:
        payload = {
            "title": parsed.get("title", "(ingen titel)"),
            "source_system": "whatsapp",
            "sender_phone": sender_id,
            "sender_name": sender_name,
            "source_message_id": msg_id,
            "source_channel": "whatsapp",
            "context": original_text,
        }
        # Assignee — om parsern hittade en person
        if parsed.get("assignee_name"):
            payload["assignee_name"] = parsed["assignee_name"]
        if parsed.get("assignee_email"):
            payload["assignee_email"] = parsed["assignee_email"]
        # Projekt
        if parsed.get("project"):
            payload["project_name"] = parsed["project"]
        # Deadline (parse_message kan returnera date-objekt, konvertera till str)
        if parsed.get("due_date"):
            dd = parsed["due_date"]
            payload["due_date"] = dd.isoformat() if hasattr(dd, 'isoformat') else str(dd)

        resp = requests.post(
            HUB_INGEST_URL,
            json=payload,
            headers={"Authorization": f"Bearer {HUB_INGEST_TOKEN}"},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            task_id = data.get("id")
            col = data.get("board_column", "inbox")
            dedup = data.get("deduplicated", False)
            print(f"  HUB ingest OK: {task_id} col={col} dedup={dedup}")
            return task_id, col
        else:
            print(f"  HUB ingest FAILED {resp.status_code}: {resp.text[:200]}")
            return None, None
    except Exception as e:
        print(f"  HUB ingest ERROR: {e}")
        return None, None


WA_PHONE_NUMBER_ID = "1087867614408215"  # RM WhatsApp sender
WA_META_CONFIG = Path("/opt/rm-infra/whatsapp-meta-config.json")

def send_wa_task_confirmation(sender_id, task_title, board_column, assignee_name=None):
    """Skicka bekräftelse till avsändaren att task skapades."""
    try:
        col_labels = {
            "inbox": "Inbox",
            "waiting": "Väntar på åtgärd",
            "today": "Idag",
            "this_week": "Denna vecka",
            "backlog": "Backlog",
        }
        col_label = col_labels.get(board_column, board_column)
        
        parts = [f"Uppgift skapad: {task_title}"]
        parts.append(f"Kolumn: {col_label}")
        if assignee_name:
            parts.append(f"Tilldelad: {assignee_name}")
        
        msg = "\n".join(parts)
        
        # Läs WhatsApp Cloud API token från Meta-config (samma som ata_handler)
        config = json.loads(WA_META_CONFIG.read_text())
        token = config.get("access_token", "")
        if not token:
            print("  WA confirmation skipped: no access_token in meta config")
            return
        
        phone = sender_id.lstrip("+")
        url = f"https://graph.facebook.com/v21.0/{WA_PHONE_NUMBER_ID}/messages"
        resp = requests.post(url, json={
            "messaging_product": "whatsapp",
            "to": phone,
            "type": "text",
            "text": {"body": msg},
        }, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, timeout=15)
        print(f"  WA confirmation sent to {sender_id}: {resp.status_code}")
    except Exception as e:
        print(f"  WA confirmation ERROR: {e}")


def handle_audio_message(msg_id, sender_id, sender_name, audio_data):
    """Process audio message — download and submit to background Whisper executor.

    Downloads audio synchronously (fast), then submits transcription to
    ThreadPoolExecutor so Flask returns 200 OK immediately.
    """
    print(f"AUDIO from {sender_name}")

    if not whisper_model:
        log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason="whisper_not_loaded")
        return

    media_id = audio_data.get("id")
    if not media_id:
        log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason="no_media_id")
        return

    # Download audio (two-step: metadata → binary, same as image download)
    try:
        audio_meta_url = f"https://graph.facebook.com/v21.0/{media_id}"
        token = META_ACCESS_TOKEN
        if not token:
            log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason="no_access_token")
            return

        meta_resp = requests.get(audio_meta_url, params={"access_token": token}, timeout=30)
        if meta_resp.status_code != 200:
            log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason=f"meta_failed_{meta_resp.status_code}")
            return

        media_url = meta_resp.json().get("url")
        if not media_url:
            log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason="no_media_url")
            return

        audio_response = requests.get(media_url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if audio_response.status_code != 200:
            log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason=f"download_failed_{audio_response.status_code}")
            return

        audio_content = audio_response.content
        print(f"  Audio downloaded: {len(audio_content)} bytes")

        # Submit transcription to background executor — returns immediately
        _whisper_executor.submit(_transcribe_and_process, msg_id, sender_id, sender_name, audio_content)
        print(f"  Audio {msg_id} submitted to background executor")

    except Exception as e:
        print(f"ERROR downloading audio: {e}")
        log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason=f"download_exception: {str(e)[:150]}")


def _transcribe_and_process(msg_id, sender_id, sender_name, audio_content):
    """Background task: transcribe audio via Whisper and create tasks.

    Runs in ThreadPoolExecutor (max_workers=1 = serialized).
    Flask has already returned 200 OK to WhatsApp.
    """
    try:
        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
            f.write(audio_content)
            temp_path = f.name

        try:
            segments, info = whisper_model.transcribe(
                temp_path,
                language=WHISPER_LANGUAGE,
                beam_size=WHISPER_BEAM,
                initial_prompt="Du är en transkriber för svenska byggbranschens arbetsuppgifter och avvikelser.",
                condition_on_previous_text=False,
            )
            transcript = " ".join(seg.text for seg in segments).strip()
        finally:
            os.unlink(temp_path)

        if not transcript:
            log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason="empty_transcript")
            return

        parsed = parse_message(transcript)
        if not parsed:
            log_message(sender_id, msg_id, transcript, sender_name, sender_id, skipped=True, skip_reason="parse_failed")
            return

        tasks = _split_audio_tasks(parsed, transcript, sender_name)
        if not tasks:
            log_message(sender_id, msg_id, transcript, sender_name, sender_id, skipped=True, skip_reason="split_empty")
            return

        for idx, task_parsed in enumerate(tasks):
            task_parsed["source_message_id"] = msg_id
            task_parsed["sender_name"] = sender_name
            task_parsed["sender_phone"] = sender_id
            plan_id = task_parsed.get("plan_id")
            try:
                task_id = create_planner_task(task_parsed)
                log_message(sender_id, msg_id, transcript, sender_name, sender_id, parsed=task_parsed, planner_task_id=task_id, subtask_index=idx)
                notify_teams_channel(task_parsed, plan_id, task_id, sender_name, sender_id, transcript)
            except Exception as e:
                print(f"ERROR creating subtask {idx}: {e}")
                log_message(sender_id, msg_id, transcript, sender_name, sender_id, subtask_index=idx, skipped=True, skip_reason=str(e)[:200])

    except Exception as e:
        print(f"ERROR in background transcription for {msg_id}: {e}")
        log_message(sender_id, msg_id, "(audio message)", sender_name, sender_id, skipped=True, skip_reason=f"exception: {str(e)[:150]}")


def _split_audio_tasks(parsed, transcript, sender_name):
    """Split one audio parse into multiple tasks based on assignee or numbered items."""
    tasks = [parsed]

    # Check if transcript contains multiple assignees or numbered tasks
    # For now, return as single task. More sophisticated splitting can go here.

    return tasks


def handle_image_message(msg_id, sender_id, sender_name, image_data, reply_to_wamid=None):
    """Process image message — download photo, use caption to classify,
    route to ÄTA/beslut/avvikelse/dagbok via handle_text_message."""
    print(f"IMAGE from {sender_name}")
    media_id = image_data.get("id")
    caption = (image_data.get("caption") or "").strip()
    if not media_id:
        log_message(sender_id, msg_id, caption or "(image)", sender_name, sender_id,
                    skipped=True, skip_reason="no_media_id")
        return

    photo_url_rel = None
    try:
        image_meta_url = f"https://graph.facebook.com/v18.0/{media_id}"
        token = META_ACCESS_TOKEN
        if not token:
            raise RuntimeError("META_ACCESS_TOKEN not configured")
        meta_resp = requests.get(image_meta_url, params={"access_token": token}, timeout=30)
        if meta_resp.status_code == 200:
            media_url = meta_resp.json().get("url")
            if media_url:
                bin_resp = requests.get(
                    media_url,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=30,
                )
                if bin_resp.status_code == 200:
                    image_filename = f"whatsapp_{msg_id}.jpg"
                    image_path = Path("/opt/rm-infra/uploads") / image_filename
                    image_path.parent.mkdir(parents=True, exist_ok=True)
                    image_path.write_bytes(bin_resp.content)
                    photo_url_rel = f"/uploads/{image_filename}"
                    print(f"  image saved: {photo_url_rel} ({len(bin_resp.content)} bytes)")
    except Exception as e:
        print(f"WARNING: Failed to download image: {e}")

    photo_urls = [photo_url_rel] if photo_url_rel else None

    # ALWAYS save to media_buffer for potential correlation with later/earlier text
    if photo_url_rel:
        try:
            from rm_data import execute as _exec
            _exec(
                "INSERT INTO media_buffer (sender_phone, photo_url, whatsapp_message_id, caption) VALUES (%s,%s,%s,%s)",
                (sender_id, photo_url_rel, msg_id, caption or None),
                db="rm_central"
            )
            print(f"  photo saved to media_buffer")
        except Exception as _be:
            print(f"  WARNING: media_buffer insert failed: {_be}")

    # If user replied to a message (quote-reply), try to match that wamid to an ÄTA
    if reply_to_wamid and photo_url_rel:
        try:
            from rm_data import query_dicts as _qd
            hit = _qd(
                """SELECT id, ata_number FROM ata_register
                   WHERE whatsapp_message_id = %s LIMIT 1""",
                (reply_to_wamid,), db="rm_central"
            )
            if hit:
                attach_photo_to_ata(hit[0]['id'], photo_url_rel)
                print(f"  photo attached to {hit[0]['ata_number']} via reply-context")
                log_message(sender_id, msg_id, f"(photo attached to {hit[0]['ata_number']} via reply)",
                            sender_name, sender_id,
                            parsed={"title": hit[0]['ata_number'], "confidence": 0.95},
                            msg_type="ata_photo_reply")
                # Mark the media_buffer row as consumed so sweep won't promote it
                try:
                    from rm_data import execute as _ex2
                    _ex2(
                        "UPDATE media_buffer SET consumed_by_table='ata_register', consumed_by_id=%s, consumed_at=now() WHERE whatsapp_message_id=%s",
                        (hit[0]['id'], msg_id), db="rm_central"
                    )
                except Exception:
                    pass
                return
        except Exception as _re:
            print(f"  reply-context lookup failed: {_re}")

    if caption:
        # Route caption as text — handle_text_message's downstream handle_ata
        # will auto-attach buffered photos via attach_buffered_media
        handle_text_message(msg_id, sender_id, sender_name, caption, photo_urls=photo_urls)
    else:
        # No caption — check if a recent ÄTA exists from this sender needing photos
        recent = find_recent_open_ata(sender_id)
        if recent and photo_url_rel:
            attach_photo_to_ata(recent['id'], photo_url_rel)
            print(f"  photo attached to recent {recent['ata_number']} (no caption path)")
            log_message(sender_id, msg_id, f"(photo attached to {recent['ata_number']})",
                        sender_name, sender_id,
                        parsed={"title": recent['ata_number'], "confidence": 0.9},
                        msg_type="ata_photo")
            return
        # No recent ÄTA — keep photo in buffer for upcoming text (120s window).
        # Do NOT create avvikelse yet. Sweep job promotes orphan photos after 120s.
        print(f"  photo buffered, waiting 120s for text from {sender_name}")
        log_message(sender_id, msg_id, "(photo buffered — awaiting text)",
                    sender_name, sender_id,
                    parsed={"title": "photo_buffered", "confidence": 0.5},
                    msg_type="photo_buffered")
        # Send lightweight confirmation so user knows photo arrived
        try:
            send_whatsapp_message(sender_id, "Foto mottaget. Skicka text inom 2 minuter så knyts det till rätt ÄTA/avvikelse.")
        except Exception as _se:
            print(f"  confirm send failed: {_se}")
        return
        # (legacy branch kept dead for reference)
        try:
            handle_project_log(
                "(bildrapport)", "avvikelse", sender_name, sender_id, msg_id,
                severity="normal", photo_urls=photo_urls,
            )
        except Exception as e:
            print(f"ERROR logging photo-only message: {e}")
            log_message(sender_id, msg_id, placeholder, sender_name, sender_id,
                        skipped=True, skip_reason=f"photo_only_error: {str(e)[:150]}")


# =============================================================================
# REST API — ÄTA Register
# =============================================================================

@app.route("/api/ata", methods=["GET"])
def list_ata():
    """List ÄTA register. Optional filters: ?status=pending&project=Rocmore"""
    status_filter = request.args.get("status")
    project_filter = request.args.get("project")

    where = ["1=1"]
    params = []

    if status_filter:
        where.append("status = %s")
        params.append(status_filter)
    if project_filter:
        where.append("project_name ILIKE %s")
        params.append(f"%{project_filter}%")

    sql = f"""SELECT json_agg(row_to_json(t)) FROM (
        SELECT id, ata_number, project_code, project_name, description,
               estimated_amount, final_amount, status, category,
               reported_by, decided_by, decided_at, customer_approved,
               photo_urls, created_at, updated_at
        FROM ata_register
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT 50
    ) t"""

    result = safe_json_query(sql, params if params else None)
    return jsonify(result), 200


@app.route("/api/ata/<int:ata_id>", methods=["PATCH"])
def update_ata(ata_id):
    """Update ÄTA status, amount, etc."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "No JSON body"}), 400

    allowed = ['status', 'final_amount', 'decided_by', 'customer_approved', 'invoice_number', 'notes']
    sets = []
    params = []

    for key in allowed:
        if key in data:
            val = data[key]
            if val is None:
                sets.append(f"{key} = NULL")
            elif isinstance(val, bool):
                sets.append(f"{key} = %s")
                params.append(val)
            elif isinstance(val, (int, float)):
                sets.append(f"{key} = %s")
                params.append(val)
            else:
                sets.append(f"{key} = %s")
                params.append(str(val))

    if 'status' in data and data['status'] == 'approved':
        sets.append("decided_at = NOW()")
    if 'customer_approved' in data and data['customer_approved']:
        sets.append("customer_approved_at = NOW()")

    sets.append("updated_at = NOW()")

    sql = f"UPDATE ata_register SET {', '.join(sets)} WHERE id = %s RETURNING ata_number"
    params.append(ata_id)

    result = execute(sql, params, returning=True)
    if result:
        return jsonify({"status": "updated", "ata_number": result}), 200
    return jsonify({"error": "not found"}), 404


# =============================================================================
# REST API — Project Log
# =============================================================================

@app.route("/api/project-log", methods=["GET"])
def list_project_log():
    """List project log entries. Optional filter: ?type=avvikelse&project=Rocmore"""
    type_filter = request.args.get("type")
    project_filter = request.args.get("project")

    where = ["1=1"]
    params = []

    if type_filter:
        where.append("log_type = %s")
        params.append(type_filter)
    if project_filter:
        where.append("project_name ILIKE %s")
        params.append(f"%{project_filter}%")

    sql = f"""SELECT json_agg(row_to_json(t)) FROM (
        SELECT id, project_code, project_name, log_type, title, description,
               severity, reported_by, photo_urls, related_ata_id, tags, created_at
        FROM project_log
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT 50
    ) t"""

    result = safe_json_query(sql, params if params else None)
    return jsonify(result), 200


@app.route("/webhook/whatsapp/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "whatsapp-webhook", "version": "v6-atomic-dedup"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8083, debug=False, threaded=True)
