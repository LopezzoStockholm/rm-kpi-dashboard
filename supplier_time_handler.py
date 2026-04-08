#!/usr/bin/env python3
"""
supplier_time_handler.py — WhatsApp tidrapportering för UE-arbetare

Flöde:
  1. Webhook tar emot meddelande
  2. whatsapp_webhook.py kollar: är telefonnumret en supplier_worker?
  3. Om ja → denna handler tar över (UE hamnar ALDRIG i ÄTA-klassificering)
  4. Om UE har 1 projekt → direkt fråga timmar
  5. Om UE har N projekt → interaktiv lista
  6. UE svarar med siffra → tid sparas

Tabeller:
  supplier_worker (identifiering via phone)
  supplier_project (vilka projekt UE jobbar på)
  whatsapp_time_pending (konversationsstate, delas med intern sweep)
  time_report (slutdestination, report_type='subcontractor')
"""

import sys
import re
sys.path.insert(0, "/opt/rm-infra")
from rm_data import execute, query_dicts

CENTRAL_DB = "rm_central"


# ---------------------------------------------------------------------------
# Lookup: är detta telefonnummer en UE-arbetare?
# ---------------------------------------------------------------------------
def lookup_supplier_worker(wa_id: str):
    """Returnerar worker-dict om wa_id matchar en aktiv supplier_worker, annars None.
    
    Matchar: wa_id = '46707830063' mot phone = '0707830063' eller '46707830063'.
    """
    phone_0 = "0" + wa_id[2:] if wa_id.startswith("46") else wa_id
    rows = query_dicts("""
        SELECT sw.id as worker_id, sw.name as worker_name, sw.phone,
               sw.supplier_id, sw.trade_code, sw.hourly_rate,
               s.name as supplier_name, s.trade_code as supplier_trade,
               s.default_hourly_rate as supplier_rate
        FROM supplier_worker sw
        JOIN supplier s ON s.id = sw.supplier_id AND s.is_active = true
        WHERE sw.is_active = true
          AND (sw.phone = %s OR sw.phone = %s)
        LIMIT 1
    """, (wa_id, phone_0), db=CENTRAL_DB)
    return rows[0] if rows else None


def get_worker_projects(supplier_id: int):
    """Hämta aktiva projekt för en UE."""
    return query_dicts("""
        SELECT sp.project_code, sp.trade_code,
               COALESCE(npe.project_name, sp.project_code) as project_name
        FROM supplier_project sp
        LEFT JOIN next_project_economy npe ON npe.project_no = sp.project_code
        WHERE sp.supplier_id = %s AND sp.is_active = true
          AND (sp.end_date IS NULL OR sp.end_date >= current_date)
        ORDER BY sp.project_code
    """, (supplier_id,), db=CENTRAL_DB)


def worker_has_reported_today(worker_id: int):
    """Kolla om UE-arbetaren redan rapporterat idag."""
    rows = query_dicts("""
        SELECT id FROM time_report
        WHERE supplier_worker_id = %s AND work_date = current_date
        LIMIT 1
    """, (worker_id,), db=CENTRAL_DB)
    return len(rows) > 0


# ---------------------------------------------------------------------------
# Pending state (delar tabell med intern sweep)
# ---------------------------------------------------------------------------
def get_pending(wa_id: str):
    """Hämta aktiv pending prompt (delas med time_sweep)."""
    rows = query_dicts("""
        SELECT id, project_code, project_name, prompt_type, user_id, person
        FROM whatsapp_time_pending
        WHERE wa_id = %s AND resolved = false AND expires_at > now()
        ORDER BY created_at DESC LIMIT 1
    """, (wa_id,), db=CENTRAL_DB)
    return rows[0] if rows else None


def set_pending(wa_id: str, worker_id: int, worker_name: str,
                project_code: str, project_name: str):
    """Sätt pending state för UE-arbetare."""
    # Resolve old
    execute("""
        UPDATE whatsapp_time_pending SET resolved = true, resolved_at = now()
        WHERE wa_id = %s AND resolved = false
    """, (wa_id,), db=CENTRAL_DB)
    # Insert new (user_id = NULL för UE, men vi lagrar worker-info i person-fältet)
    execute("""
        INSERT INTO whatsapp_time_pending (wa_id, user_id, person, project_code, project_name, prompt_type)
        VALUES (%s, %s, %s, %s, %s, 'ue_hours')
    """, (wa_id, None, f"ue:{worker_id}:{worker_name}", project_code, project_name), db=CENTRAL_DB)


def resolve_pending(wa_id: str):
    execute("""
        UPDATE whatsapp_time_pending SET resolved = true, resolved_at = now()
        WHERE wa_id = %s AND resolved = false
    """, (wa_id,), db=CENTRAL_DB)


# ---------------------------------------------------------------------------
# Parse hours (identisk med time_sweep._parse_hours)
# ---------------------------------------------------------------------------
def _parse_hours(text: str):
    text = text.strip().lower()
    if text in ("heldag", "hel dag"):
        return 8.0
    if text in ("halvdag", "halv dag"):
        return 4.0
    m = re.match(r'^(\d+[.,]?\d*)\s*(?:h|tim|timmar)?$', text)
    if m:
        return float(m.group(1).replace(',', '.'))
    return None


# ---------------------------------------------------------------------------
# WhatsApp send helpers (importeras från time_sweep)
# ---------------------------------------------------------------------------
def _get_send_fns():
    from time_sweep import send_text, send_list_message
    return send_text, send_list_message


# ---------------------------------------------------------------------------
# Huvudhandler: anropas från whatsapp_webhook.py
# ---------------------------------------------------------------------------
def handle_supplier_message(wa_id: str, sender_name: str, text: str, msg_id: str,
                            msg_type: str = "text", interactive_data: dict = None):
    """Hantera meddelande från UE-arbetare.
    
    Returnerar True om hanterat, False om inte (ska aldrig hända om lookup matchade).
    """
    send_text, send_list_message = _get_send_fns()
    worker = lookup_supplier_worker(wa_id)
    if not worker:
        return False
    
    # --- Interactive reply (projektval från lista) ---
    if msg_type == "interactive" and interactive_data:
        reply_id = interactive_data.get("id", "")
        if reply_id.startswith("ue_"):
            return _handle_project_selection(wa_id, worker, reply_id, send_text)
        return False
    
    # --- Text: kolla pending först ---
    pending = get_pending(wa_id)
    if pending and pending["prompt_type"] == "ue_hours":
        return _handle_hours_reply(wa_id, worker, text, msg_id, pending, send_text)
    
    # --- Text: spontan rapportering ---
    hours = _parse_hours(text)
    if hours is not None:
        return _handle_spontaneous_hours(wa_id, worker, hours, msg_id, send_text, send_list_message)
    
    # --- Text: hjälp / okänt ---
    projects = get_worker_projects(worker["supplier_id"])
    if projects:
        proj_names = ", ".join(p["project_name"] for p in projects[:3])
        send_text(wa_id,
            f"Hej {worker['worker_name'].split()[0]}! "
            f"Skriv antal timmar (t.ex. '8') för att rapportera tid. "
            f"Dina projekt: {proj_names}.")
    else:
        send_text(wa_id,
            f"Hej {worker['worker_name'].split()[0]}! "
            f"Du har inga aktiva projekt just nu. Kontakta din platschef.")
    return True


def _handle_project_selection(wa_id, worker, reply_id, send_text):
    """UE valde projekt från lista."""
    project_code = reply_id.replace("ue_", "")
    
    if project_code == "SKIP":
        resolve_pending(wa_id)
        send_text(wa_id, "OK, ingen tid registrerad idag.")
        return True
    
    # Hämta projektnamn
    rows = query_dicts(
        "SELECT project_name FROM next_project_economy WHERE project_no = %s LIMIT 1",
        (project_code,), db=CENTRAL_DB
    )
    project_name = rows[0]["project_name"] if rows else project_code
    
    set_pending(wa_id, worker["worker_id"], worker["worker_name"],
                project_code, project_name)
    send_text(wa_id, f"Hur många timmar på {project_name} idag?")
    return True


def _handle_hours_reply(wa_id, worker, text, msg_id, pending, send_text):
    """UE svarade med timmar efter pending prompt."""
    hours = _parse_hours(text)
    if hours is None:
        return False  # Inte en siffra → låt det falla igenom
    
    if hours <= 0 or hours > 24:
        send_text(wa_id, "Ange timmar mellan 0.5 och 24.")
        return True
    
    # Spara tidrapport
    _save_ue_time_report(worker, pending["project_code"], pending["project_name"],
                         hours, msg_id)
    resolve_pending(wa_id)
    
    send_text(wa_id,
        f"Registrerat: {hours}h på {pending['project_name']}. Tack!")
    
    # Kolla om fler projekt
    projects = get_worker_projects(worker["supplier_id"])
    reported_today = _get_reported_projects_today(worker["worker_id"])
    remaining = [p for p in projects if p["project_code"] not in reported_today]
    
    if remaining:
        send_text(wa_id, "Vill du rapportera tid på fler projekt? Skriv antal timmar eller 'nej'.")
        if len(remaining) == 1:
            set_pending(wa_id, worker["worker_id"], worker["worker_name"],
                       remaining[0]["project_code"], remaining[0]["project_name"])
        # Om flera → nästa meddelande triggar spontaneous flow
    
    return True


def _handle_spontaneous_hours(wa_id, worker, hours, msg_id, send_text, send_list_message):
    """UE skickar timmar utan prompt (spontant)."""
    if hours <= 0 or hours > 24:
        send_text(wa_id, "Ange timmar mellan 0.5 och 24.")
        return True
    
    projects = get_worker_projects(worker["supplier_id"])
    
    if not projects:
        send_text(wa_id, "Du har inga aktiva projekt. Kontakta din platschef.")
        return True
    
    if len(projects) == 1:
        # Direkt spara — inga frågor
        p = projects[0]
        _save_ue_time_report(worker, p["project_code"], p["project_name"],
                             hours, msg_id)
        send_text(wa_id, f"Registrerat: {hours}h på {p['project_name']}. Tack!")
        return True
    
    # Flera projekt → skicka lista
    rows = []
    for p in projects[:9]:
        rows.append({
            "id": f"ue_{p['project_code']}",
            "title": p["project_name"][:24],
            "description": f"Kod: {p['project_code']}",
        })
    rows.append({
        "id": "ue_SKIP",
        "title": "Ingen tid idag",
        "description": "Hoppa over",
    })
    
    sections = [{"title": "Projekt", "rows": rows}]
    
    # Spara timmar i pending så vi inte behöver fråga igen
    # Vi sätter pending med hours i notes-fältet (hack, men effektivt)
    send_list_message(wa_id,
        f"Du har {len(projects)} aktiva projekt. Vilket gäller de {hours} timmarna?",
        "Valj projekt",
        sections)
    
    # Override: vi sätter en special pending som inkluderar timmar
    execute("""
        UPDATE whatsapp_time_pending SET resolved = true, resolved_at = now()
        WHERE wa_id = %s AND resolved = false
    """, (wa_id,), db=CENTRAL_DB)
    execute("""
        INSERT INTO whatsapp_time_pending
            (wa_id, user_id, person, project_code, project_name, prompt_type)
        VALUES (%s, %s, %s, %s, %s, 'ue_project_select')
    """, (wa_id, None, f"ue:{worker['worker_id']}:{worker['worker_name']}:{hours}",
          "", ""), db=CENTRAL_DB)
    
    return True


def _save_ue_time_report(worker, project_code, project_name, hours, msg_id):
    """Spara tidrapport för UE-arbetare."""
    rate = float(worker["hourly_rate"] or worker["supplier_rate"] or 0)
    trade = worker["trade_code"] or worker["supplier_trade"]
    
    execute("""
        INSERT INTO time_report (
            company_code, person, project_code, work_date, hours,
            category, notes, source, profession_code,
            cost_unit, price_unit, total_cost, total_revenue,
            supplier_id, supplier_worker_id, trade_code, report_type,
            whatsapp_message_id
        ) VALUES (
            'RM', %s, %s, current_date, %s,
            'arbete', %s, 'whatsapp', %s,
            %s, 0, %s, 0,
            %s, %s, %s, 'subcontractor',
            %s
        )
    """, (
        worker["worker_name"], project_code, hours,
        f"UE: {worker['supplier_name']}", trade or 'allman',
        rate, round(hours * rate, 2),
        worker["supplier_id"], worker["worker_id"], trade,
        msg_id,
    ), db=CENTRAL_DB, returning=True)
    
    print(f"  UE time report: {hours}h on {project_name} by {worker['worker_name']} ({worker['supplier_name']})")


def _get_reported_projects_today(worker_id):
    """Vilka projekt har UE-arbetaren redan rapporterat idag?"""
    rows = query_dicts("""
        SELECT DISTINCT project_code FROM time_report
        WHERE supplier_worker_id = %s AND work_date = current_date
    """, (worker_id,), db=CENTRAL_DB)
    return {r["project_code"] for r in rows}


# ---------------------------------------------------------------------------
# Sweep-hook: skicka påminnelse till UE-arbetare kl 16:30
# ---------------------------------------------------------------------------
def get_ue_workers_without_report_today():
    """UE-arbetare med aktiva projekt som inte rapporterat idag."""
    return query_dicts("""
        SELECT DISTINCT sw.id as worker_id, sw.name as worker_name, sw.phone,
               sw.supplier_id, sw.trade_code, sw.hourly_rate,
               s.name as supplier_name, s.default_hourly_rate as supplier_rate,
               s.trade_code as supplier_trade
        FROM supplier_worker sw
        JOIN supplier s ON s.id = sw.supplier_id AND s.is_active = true
        JOIN supplier_project sp ON sp.supplier_id = sw.supplier_id
             AND sp.is_active = true
             AND (sp.end_date IS NULL OR sp.end_date >= current_date)
        WHERE sw.is_active = true
          AND sw.phone IS NOT NULL AND sw.phone != ''
          AND NOT EXISTS (
              SELECT 1 FROM time_report tr
              WHERE tr.supplier_worker_id = sw.id
                AND tr.work_date = current_date
          )
    """, db=CENTRAL_DB)


def run_ue_sweep():
    """Skicka end-of-day sweep till UE-arbetare utan rapport."""
    send_text, send_list_message = _get_send_fns()
    workers = get_ue_workers_without_report_today()
    
    if not workers:
        print("UE sweep: alla har rapporterat")
        return
    
    print(f"UE sweep: {len(workers)} arbetare att påminna")
    
    for w in workers:
        # Normalisera telefon till wa_id
        phone = w["phone"].lstrip("0")
        wa_id = f"46{phone}" if not phone.startswith("46") else phone
        
        projects = get_worker_projects(w["supplier_id"])
        if not projects:
            continue
        
        if len(projects) == 1:
            # Direkt fråga
            p = projects[0]
            set_pending(wa_id, w["worker_id"], w["worker_name"],
                       p["project_code"], p["project_name"])
            send_text(wa_id,
                f"Hej {w['worker_name'].split()[0]}! "
                f"Hur många timmar jobbade du idag på {p['project_name']}?")
            print(f"  Sent direct to {w['worker_name']} ({wa_id}): {p['project_name']}")
        else:
            # Lista
            rows = []
            for p in projects[:9]:
                rows.append({
                    "id": f"ue_{p['project_code']}",
                    "title": p["project_name"][:24],
                    "description": f"Kod: {p['project_code']}",
                })
            rows.append({
                "id": "ue_SKIP",
                "title": "Ingen tid idag",
                "description": "Hoppa over",
            })
            sections = [{"title": "Projekt", "rows": rows}]
            
            ok = send_list_message(wa_id,
                f"Hej {w['worker_name'].split()[0]}! Vilka projekt jobbade du på idag?",
                "Valj projekt",
                sections)
            print(f"  {'Sent' if ok else 'FAILED'} list to {w['worker_name']} ({wa_id})")
