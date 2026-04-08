"""
ata_approval.py — ÄTA-godkännandeflöde (token + PDF + Graph Mail + audit)

Tre lager:
  1. build_ata_pdf()        — återanvändbar PDF-generator (ostämplad + stämplad)
  2. send_graph_mail()      — Microsoft Graph client_credentials mail-sändning
  3. APIRouter "router"     — auth + publika endpoints

Importeras i portal_api.py via:
    from ata_approval import router as ata_approval_router
    app.include_router(ata_approval_router)
"""

from __future__ import annotations

import json
import base64
import secrets
import logging
import urllib.request
import urllib.parse
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import Response, HTMLResponse
from pydantic import BaseModel

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

from rm_data import query_dicts, execute, get_conn

log = logging.getLogger("ata_approval")

# ============================================================================
# Konfiguration
# ============================================================================

GRAPH_CONFIG_PATH = "/opt/rm-infra/graph-mail-config.json"
PUBLIC_BASE_URL = "https://portal.rmef.se"
TOKEN_TTL_DAYS = 30
PDF_ARCHIVE_DIR = "/opt/rm-infra/ata-archive"

# Lazy-load graph config
_graph_config: Optional[dict] = None
_graph_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": 0}


def _load_graph_config() -> dict:
    global _graph_config
    if _graph_config is None:
        with open(GRAPH_CONFIG_PATH, "r") as f:
            _graph_config = json.load(f)
    return _graph_config


# ============================================================================
# Microsoft Graph Mail
# ============================================================================

def _graph_access_token() -> str:
    """Client-credentials flow. Cachear token mellan anrop."""
    import time
    now = time.time()
    if _graph_token_cache["access_token"] and _graph_token_cache["expires_at"] > now + 60:
        return _graph_token_cache["access_token"]

    cfg = _load_graph_config()
    data = urllib.parse.urlencode({
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
        "scope": cfg["scope"],
        "grant_type": "client_credentials",
    }).encode("utf-8")
    req = urllib.request.Request(cfg["token_endpoint"], data=data, method="POST",
                                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    _graph_token_cache["access_token"] = payload["access_token"]
    _graph_token_cache["expires_at"] = now + int(payload.get("expires_in", 3600))
    return payload["access_token"]


def send_graph_mail(
    to_email: str,
    to_name: Optional[str],
    subject: str,
    html_body: str,
    attachments: Optional[List[Dict[str, Any]]] = None,
    reply_to: Optional[str] = None,
) -> Optional[str]:
    """Skicka mail via Graph /sendMail från ata@rmef.se.

    attachments: list of {"name": "...", "content_bytes": b"...", "content_type": "..."}
    Returnerar Graph message_id om tillgängligt, annars None.
    """
    cfg = _load_graph_config()
    token = _graph_access_token()

    msg: Dict[str, Any] = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html_body},
            "toRecipients": [
                {"emailAddress": {"address": to_email, "name": to_name or to_email}}
            ],
        },
        "saveToSentItems": True,
    }
    if reply_to:
        msg["message"]["replyTo"] = [{"emailAddress": {"address": reply_to}}]
    if attachments:
        msg["message"]["attachments"] = [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": a["name"],
                "contentType": a.get("content_type", "application/pdf"),
                "contentBytes": base64.b64encode(a["content_bytes"]).decode("ascii"),
            }
            for a in attachments
        ]

    body = json.dumps(msg).encode("utf-8")
    req = urllib.request.Request(
        cfg["graph_send_endpoint"],
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            # /sendMail returnerar 202 Accepted utan body
            return resp.headers.get("x-ms-request-id")
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        log.error(f"Graph sendMail failed {e.code}: {err_body}")
        raise HTTPException(status_code=502, detail=f"Graph mail error: {e.code} {err_body[:200]}")


# ============================================================================
# PDF-generator (ostämplad + stämplad)
# ============================================================================

def _fetch_ata_bundle(ata_id: int) -> dict:
    """Returnera ÄTA + projektinfo i en dict."""
    ata_rows = query_dicts("""
        SELECT id, ata_number, company_code, project_code, project_name, description,
               estimated_amount, final_amount, status, category, reported_by, decided_by,
               decided_at, customer_email, sent_to_customer_at, customer_decision,
               customer_decision_at, created_at, notes
        FROM ata_register WHERE id = %s
    """, (ata_id,))
    if not ata_rows:
        raise HTTPException(status_code=404, detail="ÄTA not found")
    ata = ata_rows[0]

    proj_info = {}
    if ata.get("project_name"):
        rows = query_dicts("""
            SELECT project_manager, customer_name, project_no
            FROM next_project_economy
            WHERE project_name = %s LIMIT 1
        """, (ata["project_name"],))
        if rows:
            proj_info = rows[0]
    return {"ata": ata, "project": proj_info}


def build_ata_pdf(ata_id: int, stamped: bool = False, approval: Optional[dict] = None) -> bytes:
    """Genererar PDF för ÄTA.

    stamped=False → version som skickas till beställaren (tomt signaturfält).
    stamped=True  → version med stämpel "GODKÄND av <namn> <datum>" (efter signering).
    approval: dict med keys name, decided_at (datetime), ip, decision.
    """
    bundle = _fetch_ata_bundle(ata_id)
    ata = bundle["ata"]
    proj = bundle["project"]

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm, topMargin=18*mm, bottomMargin=18*mm,
        title=f"ÄTA {ata['ata_number']}",
    )
    styles = getSampleStyleSheet()
    h_style = ParagraphStyle('H', parent=styles['Heading1'], fontSize=18,
                             textColor=colors.HexColor('#0D1117'), spaceAfter=6)
    sub_style = ParagraphStyle('Sub', parent=styles['Normal'], fontSize=10,
                               textColor=colors.HexColor('#6E7681'), spaceAfter=14)
    label_style = ParagraphStyle('Label', parent=styles['Normal'], fontSize=9,
                                 textColor=colors.HexColor('#6E7681'), spaceAfter=2)
    value_style = ParagraphStyle('Value', parent=styles['Normal'], fontSize=11,
                                 textColor=colors.HexColor('#0D1117'), spaceAfter=10)
    body_style = ParagraphStyle('Body', parent=styles['Normal'], fontSize=10,
                                leading=14, textColor=colors.HexColor('#0D1117'))

    story = []

    # Header
    story.append(Paragraph("RM Entreprenad och Fasad AB",
        ParagraphStyle('Company', parent=styles['Normal'], fontSize=9,
                       textColor=colors.HexColor('#F0883E'))))
    story.append(Paragraph(f"Beställning av ÄTA-arbete — {ata['ata_number']}", h_style))
    date_str = ata['created_at'].strftime("%Y-%m-%d") if ata.get('created_at') else ""
    story.append(Paragraph(f"Inkom: {date_str} &nbsp;&nbsp;•&nbsp;&nbsp; Org.nr 559251-1462", sub_style))

    # Meta
    proj_code = proj.get("project_no") or ata.get('project_code') or '—'
    amount = ata.get('estimated_amount')
    amount_str = (f"{int(amount):,} kr".replace(",", " ") if amount
                  else "Tidersättning / enligt ÅF-pris")
    meta = [
        ["Projekt", f"{proj_code} &nbsp;{ata.get('project_name') or ''}"],
        ["Beställare", proj.get("customer_name") or "—"],
        ["Projektledare (RM)", proj.get("project_manager") or "—"],
        ["Rapporterad av (fält)", ata.get('reported_by') or "—"],
        ["Belopp (uppskattat)", amount_str],
    ]
    meta_tbl = Table([[Paragraph(k, label_style), Paragraph(v, value_style)] for k, v in meta],
                    colWidths=[45*mm, 120*mm])
    meta_tbl.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "TOP"),
        ("BOTTOMPADDING", (0,0), (-1,-1), 4),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 10))

    # Beskrivning
    story.append(Paragraph("Beskrivning av arbetet",
        ParagraphStyle('H2', parent=styles['Heading2'], fontSize=12,
                       textColor=colors.HexColor('#0D1117'), spaceAfter=6)))
    story.append(Paragraph((ata.get('description') or '').replace('\n', '<br/>'), body_style))
    story.append(Spacer(1, 14))

    # Avtalsreferens
    ref_box = Table([[Paragraph(
        "<b>Avtalsreferens:</b> Denna ÄTA är en beställning av tillkommande arbete enligt "
        "gällande entreprenadavtal (ABS 18 / AB 04 / ABT 06). Arbete påbörjas först efter "
        "skriftligt godkännande från beställaren (signatur nedan eller digitalt godkännande). "
        "Uppskattat belopp är preliminärt och regleras enligt faktiskt utfört arbete samt "
        "avtalade priser.", body_style
    )]], colWidths=[165*mm])
    ref_box.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), colors.HexColor('#FFF8E7')),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor('#E0C068')),
        ("LEFTPADDING", (0,0), (-1,-1), 10),
        ("RIGHTPADDING", (0,0), (-1,-1), 10),
        ("TOPPADDING", (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
    ]))
    story.append(ref_box)
    story.append(Spacer(1, 22))

    # Signaturblock / stämpel
    if stamped and approval:
        story.append(Paragraph("Godkännande — beställaren",
            ParagraphStyle('H2b', parent=styles['Heading2'], fontSize=12,
                           textColor=colors.HexColor('#0D1117'), spaceAfter=10)))
        decided_at = approval.get("decided_at")
        if isinstance(decided_at, datetime):
            decided_str = decided_at.strftime("%Y-%m-%d %H:%M")
        else:
            decided_str = str(decided_at or "")
        decision = approval.get("decision") or "GODKÄND"
        stamp_color = '#1B7F3B' if decision.upper().startswith("GOD") else '#B33A3A'
        stamp = Table([[Paragraph(
            f"<b>{decision.upper()}</b><br/>"
            f"av {approval.get('name') or '—'}<br/>"
            f"{decided_str}<br/>"
            f"IP: {approval.get('ip') or '—'}", body_style
        )]], colWidths=[80*mm])
        stamp.setStyle(TableStyle([
            ("BOX", (0,0), (-1,-1), 1.2, colors.HexColor(stamp_color)),
            ("TEXTCOLOR", (0,0), (-1,-1), colors.HexColor(stamp_color)),
            ("LEFTPADDING", (0,0), (-1,-1), 12),
            ("RIGHTPADDING", (0,0), (-1,-1), 12),
            ("TOPPADDING", (0,0), (-1,-1), 10),
            ("BOTTOMPADDING", (0,0), (-1,-1), 10),
        ]))
        story.append(stamp)
        story.append(Spacer(1, 10))
        if approval.get("rejection_reason"):
            story.append(Paragraph(
                f"<b>Motivering:</b> {approval['rejection_reason']}", body_style))
        story.append(Spacer(1, 16))
        story.append(Paragraph(
            "Digital signering registrerad i RM Portal — audit-spår bevaras "
            "i enlighet med bokföringslagen (7 år).",
            ParagraphStyle('Fine', parent=styles['Normal'], fontSize=8,
                           textColor=colors.HexColor('#6E7681'), leading=11)))
    else:
        story.append(Paragraph("Godkännande — beställaren",
            ParagraphStyle('H2b', parent=styles['Heading2'], fontSize=12,
                           textColor=colors.HexColor('#0D1117'), spaceAfter=10)))
        sig_tbl = Table([
            ["", "", ""],
            ["Underskrift", "Namnförtydligande", "Datum"],
        ], colWidths=[65*mm, 55*mm, 45*mm], rowHeights=[24*mm, 6*mm])
        sig_tbl.setStyle(TableStyle([
            ("LINEABOVE", (0,1), (-1,1), 0.5, colors.HexColor('#0D1117')),
            ("FONTSIZE", (0,1), (-1,1), 8),
            ("TEXTCOLOR", (0,1), (-1,1), colors.HexColor('#6E7681')),
            ("ALIGN", (0,1), (-1,1), "LEFT"),
        ]))
        story.append(sig_tbl)
        story.append(Spacer(1, 20))
        story.append(Paragraph(
            "Digitalt godkännande: använd länken i följebrevet, eller svara på detta mejl "
            f"med texten <b>\"Godkänt {ata['ata_number']}\"</b>.",
            ParagraphStyle('Fine', parent=styles['Normal'], fontSize=8,
                           textColor=colors.HexColor('#6E7681'), leading=11)))

    doc.build(story)
    return buf.getvalue()


# ============================================================================
# Audit-logg + token-helpers
# ============================================================================

def log_audit(ata_id: int, event_type: str, actor: str, actor_type: str,
              old_status: Optional[str] = None, new_status: Optional[str] = None,
              ip: Optional[str] = None, user_agent: Optional[str] = None,
              details: Optional[dict] = None) -> None:
    execute("""
        INSERT INTO ata_audit_log
            (ata_id, event_type, actor, actor_type, old_status, new_status,
             ip_address, user_agent, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """, (ata_id, event_type, actor, actor_type, old_status, new_status,
          ip, user_agent, json.dumps(details) if details else None))


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# ============================================================================
# Pydantic-modeller
# ============================================================================

class SendForApprovalRequest(BaseModel):
    customer_email: str
    customer_name: Optional[str] = None
    cc_project_manager: bool = True
    custom_message: Optional[str] = None


class ApproveTokenRequest(BaseModel):
    decision: str  # "approve" | "reject"
    approver_name: str
    rejection_reason: Optional[str] = None


# ============================================================================
# Auth-dependency (återanvänd portal_api's get_current_user)
# ============================================================================

def _get_current_user_proxy():
    """Lazy-import för att undvika cirkulär import."""
    from portal_api import get_current_user
    return get_current_user


# ============================================================================
# Router
# ============================================================================

router = APIRouter(tags=["ata-approval"])


@router.post("/api/ata/{ata_id}/send-for-approval")
async def send_for_approval(ata_id: int, body: SendForApprovalRequest, request: Request):
    """Generera approval-token, skicka PDF till beställaren, flytta status till sent_to_customer."""
    from portal_api import get_current_user
    user = await get_current_user(request)

    bundle = _fetch_ata_bundle(ata_id)
    ata = bundle["ata"]
    proj = bundle["project"]

    if ata["status"] not in ("approved_internal", "sent_to_customer"):
        raise HTTPException(
            status_code=400,
            detail=f"ÄTA måste vara approved_internal innan den skickas till beställaren. "
                   f"Nuvarande status: {ata['status']}"
        )

    # Generera token
    token = secrets.token_urlsafe(45)
    expires = datetime.now(timezone.utc) + timedelta(days=TOKEN_TTL_DAYS)
    sent_by = user.get("name") or user.get("email") or "unknown"

    execute("""
        INSERT INTO ata_approval_token
            (token, ata_id, expires_at, recipient_email, recipient_name, sent_by)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (token, ata_id, expires, body.customer_email, body.customer_name, sent_by))

    # Bygg ostämplad PDF
    pdf_bytes = build_ata_pdf(ata_id, stamped=False)

    # Mail-innehåll
    approve_url = f"{PUBLIC_BASE_URL}/godkann/{token}"
    customer_name_safe = (body.customer_name or "").strip() or proj.get("customer_name") or ""
    greeting = f"Hej {customer_name_safe.split()[0]}," if customer_name_safe else "Hej,"
    custom_para = f"<p>{body.custom_message}</p>" if body.custom_message else ""
    amount = ata.get('estimated_amount')
    amount_line = (f"{int(amount):,} kr".replace(",", " ") if amount else "enligt ÅF-pris / tidersättning")
    pm_name = proj.get("project_manager") or "Projektledaren"

    html = f"""<!DOCTYPE html><html><body style="font-family:Arial,Helvetica,sans-serif;color:#0D1117;max-width:620px;margin:0 auto;padding:24px;">
<p style="color:#F0883E;font-size:12px;margin:0 0 4px 0;">RM ENTREPRENAD OCH FASAD AB</p>
<h2 style="margin:0 0 12px 0;">ÄTA {ata['ata_number']} — begäran om godkännande</h2>
<p>{greeting}</p>
<p>Under arbetets gång har ett behov av tillkommande arbete uppstått som ligger utanför
ursprunglig kontraktssumma. Vi ber er godkänna ÄTA:n innan arbetet påbörjas.</p>
{custom_para}
<table style="border-collapse:collapse;margin:16px 0;">
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">Projekt</td><td style="padding:4px 0;"><b>{proj.get('project_no') or ata.get('project_code') or ''} {ata.get('project_name') or ''}</b></td></tr>
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">Belopp (uppskattat)</td><td style="padding:4px 0;"><b>{amount_line}</b></td></tr>
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">Projektledare</td><td style="padding:4px 0;">{pm_name}</td></tr>
</table>
<p>Fullständig beskrivning finns i bifogad PDF.</p>
<p style="margin:28px 0;">
<a href="{approve_url}" style="background:#1B7F3B;color:#fff;padding:14px 28px;text-decoration:none;border-radius:6px;display:inline-block;font-weight:600;">Granska och godkänn ÄTA</a>
</p>
<p style="font-size:12px;color:#6E7681;">Länken är giltig i {TOKEN_TTL_DAYS} dagar. Alternativt kan ni svara på detta mejl med texten "Godkänt {ata['ata_number']}".</p>
<hr style="border:none;border-top:1px solid #E1E4E8;margin:24px 0;"/>
<p style="font-size:11px;color:#6E7681;">RM Entreprenad och Fasad AB · Org.nr 559251-1462 · ata@rmef.se</p>
</body></html>"""

    filename = f"ATA-{ata['ata_number']}.pdf"
    message_id = send_graph_mail(
        to_email=body.customer_email,
        to_name=body.customer_name,
        subject=f"ÄTA {ata['ata_number']} för godkännande — {ata.get('project_name') or ''}",
        html_body=html,
        attachments=[{"name": filename, "content_bytes": pdf_bytes, "content_type": "application/pdf"}],
        reply_to="ata@rmef.se",
    )

    # Uppdatera token med message_id
    execute("UPDATE ata_approval_token SET email_message_id = %s WHERE token = %s",
            (message_id, token))

    # Uppdatera ata_register
    execute("""
        UPDATE ata_register
        SET status = 'sent_to_customer',
            customer_email = %s,
            sent_to_customer_at = NOW(),
            sent_by = %s,
            customer_notified = TRUE,
            updated_at = NOW()
        WHERE id = %s
    """, (body.customer_email, sent_by, ata_id))

    log_audit(ata_id, "sent_to_customer", sent_by, "user",
              old_status=ata["status"], new_status="sent_to_customer",
              ip=_client_ip(request), user_agent=request.headers.get("user-agent"),
              details={"recipient": body.customer_email, "token": token[:10] + "..."})

    return {
        "status": "sent",
        "ata_number": ata["ata_number"],
        "recipient": body.customer_email,
        "expires_at": expires.isoformat(),
        "approve_url": approve_url,
    }


@router.get("/api/public/ata/approve/{token}")
async def public_get_approval(token: str):
    """Läsbar info om ÄTA:n för publika sidan (ingen auth)."""
    rows = query_dicts("""
        SELECT t.ata_id, t.expires_at, t.used_at, t.decision, t.recipient_name,
               a.ata_number, a.project_code, a.project_name, a.description,
               a.estimated_amount, a.status, a.reported_by
        FROM ata_approval_token t
        JOIN ata_register a ON a.id = t.ata_id
        WHERE t.token = %s
    """, (token,))
    if not rows:
        raise HTTPException(status_code=404, detail="Ogiltig länk")
    r = rows[0]
    now = datetime.now(timezone.utc)
    already_used = r["used_at"] is not None
    expired = r["expires_at"] < now if r["expires_at"] else False
    return {
        "ata_number": r["ata_number"],
        "project": f"{r.get('project_code') or ''} {r.get('project_name') or ''}".strip(),
        "description": r["description"],
        "estimated_amount": float(r["estimated_amount"]) if r["estimated_amount"] else None,
        "reported_by": r["reported_by"],
        "recipient_name": r["recipient_name"],
        "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
        "already_used": already_used,
        "expired": expired,
        "previous_decision": r["decision"],
        "current_status": r["status"],
    }


@router.get("/api/public/ata/approve/{token}/pdf")
async def public_get_pdf(token: str):
    """Visa ostämplad (eller stämplad om signerad) PDF via token."""
    rows = query_dicts("""
        SELECT t.ata_id, t.used_at, t.decision, t.approver_name,
               t.approver_ip, t.rejection_reason, a.status
        FROM ata_approval_token t
        JOIN ata_register a ON a.id = t.ata_id
        WHERE t.token = %s
    """, (token,))
    if not rows:
        raise HTTPException(status_code=404, detail="Ogiltig länk")
    r = rows[0]
    if r["used_at"] and r["decision"]:
        approval = {
            "name": r["approver_name"],
            "decided_at": r["used_at"],
            "ip": r["approver_ip"],
            "decision": "GODKÄND" if r["decision"] == "approve" else "AVVISAD",
            "rejection_reason": r["rejection_reason"],
        }
        pdf = build_ata_pdf(r["ata_id"], stamped=True, approval=approval)
    else:
        pdf = build_ata_pdf(r["ata_id"], stamped=False)
    return Response(content=pdf, media_type="application/pdf",
                    headers={"Content-Disposition": "inline; filename=\"ata.pdf\""})


@router.post("/api/public/ata/approve/{token}")
async def public_submit_approval(token: str, body: ApproveTokenRequest, request: Request):
    """Beställare godkänner eller avvisar via token."""
    if body.decision not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="decision måste vara 'approve' eller 'reject'")
    if body.decision == "reject" and not (body.rejection_reason and body.rejection_reason.strip()):
        raise HTTPException(status_code=400, detail="Motivering krävs vid avslag")
    if not body.approver_name or not body.approver_name.strip():
        raise HTTPException(status_code=400, detail="Namn krävs")

    ip = _client_ip(request)
    ua = request.headers.get("user-agent", "")[:500]
    now = datetime.now(timezone.utc)

    # Transaktionell uppdatering — säkerställ att token inte använts
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT t.ata_id, t.used_at, t.expires_at, a.status, a.ata_number
                FROM ata_approval_token t
                JOIN ata_register a ON a.id = t.ata_id
                WHERE t.token = %s
                FOR UPDATE
            """, (token,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Ogiltig länk")
            ata_id, used_at, expires_at, ata_status, ata_number = row
            if used_at is not None:
                raise HTTPException(status_code=409, detail="Länken har redan använts")
            if expires_at and expires_at < now:
                raise HTTPException(status_code=410, detail="Länken har gått ut")

            new_status = "ordered" if body.decision == "approve" else "rejected"

            cur.execute("""
                UPDATE ata_approval_token
                SET used_at = %s,
                    decision = %s,
                    approver_name = %s,
                    approver_ip = %s,
                    approver_user_agent = %s,
                    rejection_reason = %s
                WHERE token = %s
            """, (now, body.decision, body.approver_name.strip(), ip, ua,
                  body.rejection_reason, token))

            cur.execute("""
                UPDATE ata_register
                SET status = %s,
                    customer_decision = %s,
                    customer_decision_at = %s,
                    customer_approved = %s,
                    customer_approved_at = %s,
                    customer_rejection_reason = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (new_status, body.decision, now, body.decision == "approve",
                  now if body.decision == "approve" else None,
                  body.rejection_reason, ata_id))

            cur.execute("""
                INSERT INTO ata_audit_log
                    (ata_id, event_type, actor, actor_type, old_status, new_status,
                     ip_address, user_agent, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (ata_id,
                  "customer_approved" if body.decision == "approve" else "customer_rejected",
                  body.approver_name.strip(), "customer",
                  ata_status, new_status, ip, ua,
                  json.dumps({"token_prefix": token[:10],
                              "rejection_reason": body.rejection_reason})))
        conn.commit()
    finally:
        conn.close()

    # Arkivera stämplad PDF (efter commit, best-effort)
    try:
        _archive_stamped_pdf(ata_id, ata_number, body.decision,
                              body.approver_name.strip(), now, ip,
                              body.rejection_reason)
    except Exception as e:
        log.warning(f"PDF-arkivering misslyckades för ata {ata_id}: {e}")

    # Notifiera projektledaren (efter commit, best-effort)
    try:
        _notify_project_manager(ata_id, body.decision, body.approver_name.strip(),
                                 body.rejection_reason)
    except Exception as e:
        log.warning(f"PM-notifiering misslyckades för ata {ata_id}: {e}")

    return {
        "status": "ok",
        "decision": body.decision,
        "ata_number": ata_number,
        "new_ata_status": new_status,
    }


def _archive_stamped_pdf(ata_id: int, ata_number: str, decision: str,
                           approver_name: str, decided_at: datetime,
                           ip: str, rejection_reason: Optional[str]) -> None:
    """Skriv stämplad PDF till ata-archive/ och spara sökväg i ata_register."""
    import os
    os.makedirs(PDF_ARCHIVE_DIR, exist_ok=True)
    approval = {
        "name": approver_name,
        "decided_at": decided_at,
        "ip": ip,
        "decision": "GODKÄND" if decision == "approve" else "AVVISAD",
        "rejection_reason": rejection_reason,
    }
    pdf = build_ata_pdf(ata_id, stamped=True, approval=approval)
    # Filnamn: ÄTA-2026-004__ordered__2026-04-05.pdf (ASCII-safe)
    safe_num = ata_number.replace("Ä", "A").replace("ä", "a").replace(" ", "_")
    date_str = decided_at.strftime("%Y-%m-%d")
    suffix = "godkand" if decision == "approve" else "avvisad"
    filename = f"{safe_num}__{suffix}__{date_str}.pdf"
    path = os.path.join(PDF_ARCHIVE_DIR, filename)
    with open(path, "wb") as f:
        f.write(pdf)
    # Sätt read-only (arkivskydd)
    os.chmod(path, 0o444)
    execute("UPDATE ata_register SET approval_pdf_path = %s WHERE id = %s",
            (path, ata_id))
    log.info(f"ata {ata_id} arkiverad: {path} ({len(pdf)} bytes)")


def _notify_project_manager(ata_id: int, decision: str, approver_name: str,
                             rejection_reason: Optional[str]) -> None:
    """Mejla projektledaren om kundens beslut."""
    rows = query_dicts("""
        SELECT a.ata_number, a.project_name, a.estimated_amount,
               p.project_manager, p.customer_name
        FROM ata_register a
        LEFT JOIN next_project_economy p ON p.project_name = a.project_name
        WHERE a.id = %s
    """, (ata_id,))
    if not rows:
        return
    r = rows[0]
    pm_name = r.get("project_manager")
    if not pm_name:
        log.info(f"ata {ata_id}: ingen projektledare kopplad, hoppar över notis")
        return

    # Slå upp mejl från portal_user
    pm_rows = query_dicts("""
        SELECT email FROM portal_user
        WHERE display_name = %s OR username = %s OR planner_email = %s OR owner_alias = %s
        LIMIT 1
    """, (pm_name, pm_name, pm_name, pm_name))
    if not pm_rows or not pm_rows[0].get("email"):
        log.info(f"ata {ata_id}: ingen mejl för PL {pm_name}")
        return
    pm_email = pm_rows[0]["email"]

    if decision == "approve":
        subject = f"ÄTA {r['ata_number']} GODKÄND av beställaren"
        color = "#1B7F3B"
        headline = "Beställaren har godkänt ÄTA:n"
        sub = "Arbetet kan påbörjas enligt avtal. Status i portalen: <b>ordered</b>."
        reason_block = ""
    else:
        subject = f"ÄTA {r['ata_number']} avvisad av beställaren"
        color = "#B33A3A"
        headline = "Beställaren har avvisat ÄTA:n"
        sub = "Arbetet får inte påbörjas. Kontakta beställaren för dialog."
        reason_block = (f"<p><b>Motivering:</b><br/>{rejection_reason}</p>"
                        if rejection_reason else "")

    html = f"""<!DOCTYPE html><html><body style="font-family:Arial,Helvetica,sans-serif;color:#0D1117;max-width:600px;margin:0 auto;padding:24px;">
<h2 style="color:{color};margin:0 0 12px 0;">{headline}</h2>
<p>{sub}</p>
<table style="border-collapse:collapse;margin:12px 0;">
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">ÄTA</td><td style="padding:4px 0;"><b>{r['ata_number']}</b></td></tr>
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">Projekt</td><td style="padding:4px 0;">{r.get('project_name') or '—'}</td></tr>
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">Beställare</td><td style="padding:4px 0;">{r.get('customer_name') or '—'}</td></tr>
<tr><td style="padding:4px 16px 4px 0;color:#6E7681;">Godkänt av</td><td style="padding:4px 0;">{approver_name}</td></tr>
</table>
{reason_block}
<p style="margin-top:24px;"><a href="{PUBLIC_BASE_URL}/ata">Öppna ÄTA-registret</a></p>
</body></html>"""

    send_graph_mail(
        to_email=pm_email,
        to_name=pm_name,
        subject=subject,
        html_body=html,
        reply_to="ata@rmef.se",
    )


# ============================================================================
# Publik HTML-sida
# ============================================================================

@router.get("/godkann/{token}", response_class=HTMLResponse)
async def public_approval_page(token: str):
    """Publik HTML-sida för beställare att granska och signera ÄTA."""
    html = """<!DOCTYPE html>
<html lang="sv"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>ÄTA — godkännande</title>
<style>
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif;margin:0;background:#F6F8FA;color:#0D1117}
.wrap{max-width:920px;margin:0 auto;padding:24px}
.brand{color:#F0883E;font-size:12px;font-weight:600;letter-spacing:0.5px;margin-bottom:8px}
h1{font-size:24px;margin:0 0 6px 0}
.sub{color:#6E7681;font-size:14px;margin-bottom:24px}
.grid{display:grid;grid-template-columns:1fr 380px;gap:20px}
@media(max-width:820px){.grid{grid-template-columns:1fr}}
.card{background:#fff;border:1px solid #E1E4E8;border-radius:8px;padding:20px}
.meta-row{display:flex;padding:8px 0;border-bottom:1px solid #F0F2F5}
.meta-row:last-child{border-bottom:0}
.meta-k{color:#6E7681;width:140px;font-size:13px}
.meta-v{font-weight:500;font-size:14px}
.desc{white-space:pre-wrap;font-size:14px;line-height:1.5;padding:12px;background:#F6F8FA;border-radius:6px;margin-top:8px}
iframe{width:100%;height:640px;border:1px solid #E1E4E8;border-radius:6px;background:#fff}
label{display:block;margin-top:14px;font-size:13px;font-weight:600}
input,textarea{width:100%;padding:10px;border:1px solid #D0D7DE;border-radius:6px;font-size:14px;font-family:inherit;margin-top:4px}
textarea{min-height:80px;resize:vertical}
.btn{display:block;width:100%;padding:14px;border:0;border-radius:6px;font-size:15px;font-weight:600;cursor:pointer;margin-top:12px}
.btn-approve{background:#1B7F3B;color:#fff}
.btn-approve:hover{background:#15652F}
.btn-reject{background:#fff;color:#B33A3A;border:1px solid #B33A3A}
.btn-reject:hover{background:#FFF5F5}
.reject-box{display:none;margin-top:12px;padding:12px;background:#FFF5F5;border:1px solid #F4C5C5;border-radius:6px}
.reject-box.active{display:block}
.alert{padding:14px;border-radius:6px;margin-bottom:16px;font-size:14px}
.alert-err{background:#FFF5F5;border:1px solid #F4C5C5;color:#8B2929}
.alert-ok{background:#F0FBF4;border:1px solid #B6E5C5;color:#0F5B27}
.alert-warn{background:#FFF8E7;border:1px solid #E0C068;color:#7A5B0B}
.hidden{display:none}
.legal{font-size:11px;color:#6E7681;margin-top:14px;line-height:1.5}
</style></head>
<body><div class="wrap">
<p class="brand">RM ENTREPRENAD OCH FASAD AB</p>
<h1 id="h1">ÄTA — godkännande</h1>
<p class="sub" id="sub">Laddar…</p>
<div id="alertbox"></div>
<div class="grid" id="grid" style="display:none">
  <div class="card">
    <iframe id="pdfframe" src=""></iframe>
  </div>
  <div>
    <div class="card">
      <div id="meta"></div>
      <div class="desc" id="desc"></div>
    </div>
    <div class="card" id="formcard" style="margin-top:16px">
      <label for="name">Ditt namn (beställare)</label>
      <input id="name" type="text" placeholder="Förnamn Efternamn" autocomplete="name"/>
      <button class="btn btn-approve" id="approveBtn" type="button">Godkänn ÄTA</button>
      <button class="btn btn-reject" id="rejectBtn" type="button">Avvisa</button>
      <div class="reject-box" id="rejectBox">
        <label for="reason">Motivering (obligatorisk)</label>
        <textarea id="reason" placeholder="Varför avvisas ÄTA:n?"></textarea>
        <button class="btn btn-reject" id="confirmRejectBtn" type="button">Skicka avslag</button>
      </div>
      <p class="legal">Genom att godkänna bekräftar ni att arbetet får utföras enligt denna beställning. Godkännandet arkiveras digitalt med tidsstämpel och IP-adress i enlighet med ABS 18 / AB 04 / ABT 06.</p>
    </div>
  </div>
</div>
</div>
<script>
const token = location.pathname.split('/').pop();
const apiBase = '/api/public/ata/approve/' + token;
let ataNumber = '';

async function load(){
  try{
    const r = await fetch(apiBase);
    if(!r.ok){
      const e = await r.json().catch(()=>({detail:'Fel'}));
      showAlert('err', e.detail || 'Kunde inte ladda');
      return;
    }
    const d = await r.json();
    ataNumber = d.ata_number;
    document.getElementById('h1').textContent = 'ÄTA ' + d.ata_number;
    document.getElementById('sub').textContent = d.project;
    document.getElementById('pdfframe').src = apiBase + '/pdf';
    const amt = d.estimated_amount ? (Math.round(d.estimated_amount).toLocaleString('sv-SE') + ' kr') : 'Tidersättning / ÅF-pris';
    document.getElementById('meta').innerHTML =
      '<div class="meta-row"><div class="meta-k">Belopp (uppskattat)</div><div class="meta-v">'+amt+'</div></div>'+
      '<div class="meta-row"><div class="meta-k">Rapporterat av</div><div class="meta-v">'+(d.reported_by||'—')+'</div></div>'+
      '<div class="meta-row"><div class="meta-k">Länk giltig till</div><div class="meta-v">'+(d.expires_at?d.expires_at.slice(0,10):'—')+'</div></div>';
    document.getElementById('desc').textContent = d.description || '';
    document.getElementById('grid').style.display = 'grid';
    if(d.already_used){
      const msg = d.previous_decision==='approve' ? 'GODKÄND' : 'AVVISAD';
      showAlert('warn','Denna länk har redan använts. ÄTA:n är '+msg+'.');
      document.getElementById('formcard').style.display='none';
    }else if(d.expired){
      showAlert('err','Länken har gått ut. Kontakta projektledaren för en ny.');
      document.getElementById('formcard').style.display='none';
    }
    if(d.recipient_name){ document.getElementById('name').value = d.recipient_name; }
  }catch(e){ showAlert('err','Nätverksfel: '+e.message); }
}

function showAlert(kind,msg){
  document.getElementById('alertbox').innerHTML = '<div class="alert alert-'+kind+'">'+msg+'</div>';
}

document.getElementById('approveBtn').onclick = async () => {
  const name = document.getElementById('name').value.trim();
  if(!name){ showAlert('err','Fyll i ditt namn innan du godkänner.'); return; }
  if(!confirm('Bekräfta: Godkänn ÄTA '+ataNumber+'?')) return;
  await submit('approve', name, null);
};

document.getElementById('rejectBtn').onclick = () => {
  document.getElementById('rejectBox').classList.add('active');
};

document.getElementById('confirmRejectBtn').onclick = async () => {
  const name = document.getElementById('name').value.trim();
  const reason = document.getElementById('reason').value.trim();
  if(!name){ showAlert('err','Fyll i ditt namn.'); return; }
  if(!reason){ showAlert('err','Motivering krävs vid avslag.'); return; }
  await submit('reject', name, reason);
};

async function submit(decision, name, reason){
  try{
    const r = await fetch(apiBase, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({decision, approver_name:name, rejection_reason:reason})
    });
    const d = await r.json();
    if(!r.ok){ showAlert('err', d.detail || 'Fel vid signering'); return; }
    const kind = decision==='approve' ? 'ok' : 'warn';
    const msg = decision==='approve'
      ? 'Tack! ÄTA '+ataNumber+' är nu godkänd och arkiverad. Projektledaren är notifierad.'
      : 'ÄTA '+ataNumber+' har avvisats. Projektledaren är notifierad.';
    showAlert(kind, msg);
    document.getElementById('formcard').style.display='none';
    document.getElementById('pdfframe').src = apiBase + '/pdf?_='+Date.now();
  }catch(e){ showAlert('err','Nätverksfel: '+e.message); }
}

load();
</script></body></html>"""
    return HTMLResponse(content=html)


# ============================================================================
# Token-lista (auth) för projektledare/VD
# ============================================================================

@router.get("/api/ata/{ata_id}/tokens")
async def list_tokens(ata_id: int, request: Request):
    from portal_api import get_current_user
    await get_current_user(request)
    rows = query_dicts("""
        SELECT token, created_at, expires_at, used_at, decision,
               recipient_email, recipient_name, approver_name, approver_ip,
               rejection_reason, sent_by
        FROM ata_approval_token
        WHERE ata_id = %s
        ORDER BY created_at DESC
    """, (ata_id,))
    for r in rows:
        r["token"] = r["token"][:12] + "…"
        for k in ("created_at", "expires_at", "used_at"):
            if r.get(k):
                r[k] = r[k].isoformat()
    return {"tokens": rows}


@router.get("/api/ata/{ata_id}/audit")
async def get_audit(ata_id: int, request: Request):
    from portal_api import get_current_user
    await get_current_user(request)
    rows = query_dicts("""
        SELECT event_type, event_at, actor, actor_type, old_status, new_status,
               ip_address, details
        FROM ata_audit_log
        WHERE ata_id = %s
        ORDER BY event_at DESC
    """, (ata_id,))
    for r in rows:
        if r.get("event_at"):
            r["event_at"] = r["event_at"].isoformat()
    return {"events": rows}
