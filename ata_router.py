"""
ata_router.py — ÄTA-modulen (Modul 1: Fält)

Alla ÄTA-endpoints utbrutna från portal_api.py med parametriserad SQL.
Registreras via init_ata_router() som injicerar auth-funktioner från kärnan.
"""

import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.responses import Response, FileResponse
from pathlib import Path

from rm_data import query_dicts, execute

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ata"])

# --- Dependency injection from core -----------------------------------------
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_ata_router(get_current_user, has_perm, require_perm, audit_log, get_company_code_fn=None):
    """Called once from portal_api.py to inject shared auth functions."""
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user
    _has_perm = has_perm
    _require_perm = require_perm
    _audit_log = audit_log
    _get_company_code = get_company_code_fn



def _cc(request, user):
    """Resolve company_code from header via injected function."""
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"

def _user_dep():
    """FastAPI dependency that delegates to the injected get_current_user."""
    return Depends(_get_current_user)


# ============================================================================
# ÄTA CRUD
# ============================================================================

@router.get("/api/ata")
async def list_ata(
    request: Request,
    status: Optional[str] = None,
    project: Optional[str] = None,
):
    """List ÄTA register with optional filters. Projektledare ser bara sina egna projekts ÄTA."""
    user = await _get_current_user(request)
    company = _cc(request, user)

    where = ["company_code = %s"]
    params = [company]

    if status:
        where.append("status = %s")
        params.append(status)
    if project:
        where.append("project_name ILIKE %s")
        params.append(f"%{project}%")

    # Role-based filtering: projektledare sees only their projects
    if not _has_perm(user, "ata.read_all"):
        name = user.get("name") or ""
        where.append(
            "project_name IN ("
            "SELECT DISTINCT project_name FROM next_project_economy "
            "WHERE project_manager ILIKE %s"
            ")"
        )
        params.append(f"%{name}%")

    sql = f"""
        SELECT id, ata_number, project_code, project_name, description,
               estimated_amount, final_amount, status, category,
               reported_by, decided_by, decided_at::text, customer_approved,
               customer_email, sent_to_customer_at::text, sent_by,
               customer_decision, customer_decision_at::text,
               customer_rejection_reason,
               photo_urls::text, created_at::text, updated_at::text
        FROM ata_register
        WHERE {' AND '.join(where)}
        ORDER BY created_at DESC
        LIMIT 50
    """
    rows = query_dicts(sql, tuple(params))
    return {"ata_items": rows, "count": len(rows), "role": user.get("role")}


@router.get("/api/ata/summary")
async def ata_summary(request: Request):
    """ÄTA summary: counts by status, total amounts."""
    user = await _get_current_user(request)
    company = _cc(request, user)

    rows = query_dicts("""
        SELECT status,
               COUNT(*) as count,
               COALESCE(SUM(estimated_amount),0) as estimated_total,
               COALESCE(SUM(final_amount),0) as final_total
        FROM ata_register
        WHERE company_code = %s
        GROUP BY status
    """, (company,))

    total = query_dicts("""
        SELECT COUNT(*) as total,
               COALESCE(SUM(estimated_amount),0) as estimated_total,
               COALESCE(SUM(final_amount),0) as final_total,
               COUNT(*) FILTER (WHERE customer_approved = true) as approved_count
        FROM ata_register WHERE company_code = %s
    """, (company,))

    return {"by_status": rows, "totals": total[0] if total else {}}


@router.patch("/api/ata/{ata_id}")
async def update_ata_status(ata_id: int, request: Request):
    """Update ÄTA status, amount, etc."""
    user = await _get_current_user(request)
    data = await request.json()

    allowed = ['status', 'final_amount', 'estimated_amount', 'project_code', 'project_name',
               'description', 'customer_approved', 'invoice_number', 'notes']
    valid_statuses = {'reported', 'approved_internal', 'rejected', 'sent_to_customer',
                      'ordered', 'signed', 'invoiced', 'pending_project'}

    if 'status' in data and data['status'] not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {sorted(valid_statuses)}")

    sets = []
    params = []
    for key in allowed:
        if key in data:
            val = data[key]
            if val is None:
                sets.append(f"{key} = NULL")
            else:
                sets.append(f"{key} = %s")
                params.append(val)

    # Auto-capture decided_by on internal approval/rejection
    if data.get('status') in ('approved_internal', 'rejected'):
        decider = user.get('name') or user.get('email') or 'unknown'
        sets.append("decided_by = %s")
        params.append(decider)
        sets.append("decided_at = NOW()")
    if data.get('customer_approved'):
        sets.append("customer_approved_at = NOW()")

    sets.append("updated_at = NOW()")
    params.append(ata_id)

    result = query_dicts(
        f"UPDATE ata_register SET {', '.join(sets)} WHERE id = %s RETURNING ata_number, status",
        tuple(params)
    )
    if result:
        # Auto-archive PDF version on any state change
        try:
            from ata_pdf_archive import persist_ata_pdf_version
            trigger = data.get('status') or 'edit'
            user_name = user.get('name') or user.get('email') or 'system'
            persist_ata_pdf_version(ata_id, trigger_event=trigger, user_name=user_name)
        except Exception as _pe:
            logger.warning(f"PDF archive failed for ata_id={ata_id}: {_pe}")
        return {"status": "updated", "ata_number": result[0].get("ata_number"), "new_status": result[0].get("status")}
    raise HTTPException(status_code=404, detail="ÄTA not found")


# ============================================================================
# ÄTA PDF Generation
# ============================================================================

@router.get("/api/ata/{ata_id}/pdf")
async def generate_ata_pdf(ata_id: int, request: Request):
    """Generate a PDF for the ÄTA suitable for sending to the beställare."""
    user = await _get_current_user(request)

    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    rows = query_dicts("""
        SELECT id, ata_number, project_code, project_name, description,
               estimated_amount, status, reported_by, decided_by, decided_at::text,
               created_at::text, notes, company_code
        FROM ata_register WHERE id = %s
    """, (ata_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="ÄTA not found")
    ata = rows[0]

    # Look up project leader + customer
    proj_name = ata.get("project_name") or ""
    proj_info = []
    if proj_name:
        proj_info = query_dicts("""
            SELECT project_manager, customer_name
            FROM next_project_economy
            WHERE project_name = %s
            LIMIT 1
        """, (proj_name,))
    project_leader = proj_info[0].get("project_manager") if proj_info else ""
    customer_name = proj_info[0].get("customer_name") if proj_info else ""

    # Build PDF
    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=20*mm, rightMargin=20*mm,
                             topMargin=18*mm, bottomMargin=18*mm,
                             title=f"ÄTA {ata['ata_number']}")
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
    date_str = ""
    if ata.get('created_at'):
        try:
            date_str = datetime.fromisoformat(
                ata['created_at'].replace('+00', '+00:00')
            ).strftime("%Y-%m-%d")
        except Exception:
            date_str = str(ata['created_at'])[:10]
    story.append(Paragraph(f"Inkom: {date_str} &nbsp;&nbsp;•&nbsp;&nbsp; Org.nr 559251-1462", sub_style))

    # Meta table
    amt_str = (f"{int(ata['estimated_amount']):,} kr".replace(",", " ")
               if ata.get('estimated_amount')
               else "Tidersättning / enligt ÅF-pris")
    meta = [
        ["Projekt", f"{ata.get('project_code') or '—'} &nbsp;{ata.get('project_name') or ''}"],
        ["Beställare", customer_name or "—"],
        ["Projektledare (RM)", project_leader or "—"],
        ["Rapporterad av (fält)", ata.get('reported_by') or "—"],
        ["Belopp (uppskattat)", amt_str],
    ]
    meta_tbl = Table(
        [[Paragraph(k, label_style), Paragraph(v, value_style)] for k, v in meta],
        colWidths=[45*mm, 120*mm]
    )
    meta_tbl.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 10))

    # Description
    story.append(Paragraph("Beskrivning av arbetet",
                 ParagraphStyle('H2', parent=styles['Heading2'], fontSize=12,
                                textColor=colors.HexColor('#0D1117'), spaceAfter=6)))
    story.append(Paragraph((ata.get('description') or '').replace('\n', '<br/>'), body_style))
    story.append(Spacer(1, 14))

    # Contract reference
    ref_box = Table([[Paragraph(
        "<b>Avtalsreferens:</b> Denna ÄTA är en beställning av tillkommande arbete enligt gällande entreprenadavtal "
        "(ABS 18 / AB 04 / ABT 06). Arbete påbörjas först efter skriftligt godkännande från beställaren "
        "(signatur nedan eller svar via e-post). Uppskattat belopp är preliminärt och regleras enligt faktiskt utfört arbete "
        "samt avtalade priser.", body_style
    )]], colWidths=[165*mm])
    ref_box.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor('#FFF8E7')),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor('#E0C068')),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(ref_box)
    story.append(Spacer(1, 22))

    # Signature block
    story.append(Paragraph("Godkännande — beställaren",
                 ParagraphStyle('H2b', parent=styles['Heading2'], fontSize=12,
                                textColor=colors.HexColor('#0D1117'), spaceAfter=10)))
    sig_tbl = Table([
        ["", "", ""],
        ["Underskrift", "Namnförtydligande", "Datum"],
    ], colWidths=[65*mm, 55*mm, 45*mm], rowHeights=[24*mm, 6*mm])
    sig_tbl.setStyle(TableStyle([
        ("LINEABOVE", (0, 1), (-1, 1), 0.5, colors.HexColor('#0D1117')),
        ("FONTSIZE", (0, 1), (-1, 1), 8),
        ("TEXTCOLOR", (0, 1), (-1, 1), colors.HexColor('#6E7681')),
        ("ALIGN", (0, 1), (-1, 1), "LEFT"),
    ]))
    story.append(sig_tbl)
    story.append(Spacer(1, 20))
    story.append(Paragraph(
        f"Alternativt: svara på mejlet med denna PDF bifogad med texten <b>\"Godkänt {ata['ata_number']}\"</b> "
        "så arkiveras svaret som beställarens godkännande.",
        ParagraphStyle('Fine', parent=styles['Normal'], fontSize=8,
                        textColor=colors.HexColor('#6E7681'), leading=11)
    ))

    doc.build(story)
    pdf_bytes = buf.getvalue()

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{ata["ata_number"]}.pdf"'}
    )


# ============================================================================
# ÄTA Document Archive
# ============================================================================

@router.get("/api/ata/documents")
async def list_ata_documents(
    request: Request,
    project_code: Optional[str] = None,
    status: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: int = 200
):
    """List all archived ÄTA PDF versions. Filters: project_code, status, date range."""
    user = await _get_current_user(request)

    where = ["1=1"]
    params = []

    if project_code:
        where.append("v.project_code = %s")
        params.append(project_code)
    if status:
        where.append("v.state_at_generation = %s")
        params.append(status)
    if from_date:
        where.append("v.generated_at >= %s")
        params.append(from_date)
    if to_date:
        where.append("v.generated_at <= %s")
        params.append(to_date)

    sql = f"""
        SELECT v.id, v.ata_id, v.ata_number, v.project_code, v.project_name,
               v.version, v.filepath, v.file_size_bytes, v.stamped,
               v.state_at_generation, v.amount_at_generation,
               v.trigger_event, v.generated_by, v.generated_at::text
        FROM ata_document_version v
        WHERE {' AND '.join(where)}
        ORDER BY v.generated_at DESC
        LIMIT %s
    """
    params.append(int(limit))
    rows = query_dicts(sql, tuple(params))

    # Distinct project_codes for the filter dropdown
    projects = query_dicts("""
        SELECT DISTINCT project_code, project_name
        FROM ata_document_version
        WHERE project_code IS NOT NULL
        ORDER BY project_code
    """)
    return {"documents": rows, "count": len(rows), "projects": projects}


@router.get("/api/ata/{ata_id}/versions")
async def list_ata_versions(ata_id: int, request: Request):
    """Return all archived PDF versions for a single ÄTA, newest first."""
    user = await _get_current_user(request)

    rows = query_dicts("""
        SELECT id, version, filepath, file_size_bytes, stamped,
               state_at_generation, amount_at_generation, trigger_event,
               generated_by, generated_at::text
        FROM ata_document_version
        WHERE ata_id = %s
        ORDER BY version DESC
    """, (ata_id,))
    return {"versions": rows, "count": len(rows)}


@router.post("/api/ata/{ata_id}/archive-now")
async def archive_ata_now(ata_id: int, request: Request):
    """Force-generate and archive a new PDF version for this ÄTA."""
    user = await _get_current_user(request)

    from ata_pdf_archive import persist_ata_pdf_version
    user_name = user.get("name") or user.get("email") or "manual"
    result = persist_ata_pdf_version(ata_id, trigger_event="manual", user_name=user_name)
    if not result:
        raise HTTPException(status_code=404, detail="ÄTA not found or PDF build failed")
    return result


@router.get("/api/ata/documents/{version_id}/file")
async def get_ata_document_file(version_id: int, request: Request):
    """Stream a specific archived PDF version."""
    user = await _get_current_user(request)

    rows = query_dicts("""
        SELECT filepath, ata_number, version
        FROM ata_document_version WHERE id = %s
    """, (version_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Version not found")
    rel = rows[0]["filepath"].lstrip("/")
    full = Path("/opt/rm-infra") / rel
    if not full.exists():
        raise HTTPException(status_code=404, detail=f"PDF file missing on disk: {rel}")
    filename = f'{rows[0]["ata_number"]}_v{rows[0]["version"]}.pdf'
    return FileResponse(str(full), media_type="application/pdf",
                        headers={"Content-Disposition": f'inline; filename="{filename}"'})


# ============================================================================
# Invoicing (ÄTA-relaterat)
# ============================================================================

@router.get("/api/invoicing/history")
async def invoicing_history(
    request: Request,
    days: int = 90,
    project: str = None,
):
    """Lista alla fakturerade ÄTA (invoiced_at IS NOT NULL). Default senaste 90 dagar."""
    user = await _get_current_user(request)
    company = _cc(request, user)

    where = ["company_code = %s", "invoiced_at IS NOT NULL",
             "invoiced_at >= NOW() - make_interval(days => %s)"]
    params = [company, int(days)]

    if project:
        where.append("project_name ILIKE %s")
        params.append(f"%{project}%")
    if not _has_perm(user, "invoices.read_all"):
        name = user.get("name") or ""
        where.append(
            "project_name IN ("
            "SELECT DISTINCT project_name FROM next_project_economy "
            "WHERE project_manager ILIKE %s"
            ")"
        )
        params.append(f"%{name}%")

    rows = query_dicts(f"""
        SELECT id, ata_number, project_code, project_name, description,
               estimated_amount, final_amount, invoice_amount, invoice_number,
               invoiced_at::text, invoiced_by, status,
               customer_decision_at::text, decided_at::text
        FROM ata_register
        WHERE {' AND '.join(where)}
        ORDER BY invoiced_at DESC
    """, tuple(params))

    total = sum(float(r.get("invoice_amount") or r.get("final_amount") or r.get("estimated_amount") or 0) for r in rows)

    by_month: Dict[str, dict] = {}
    by_project: Dict[str, dict] = {}
    for r in rows:
        inv = r.get("invoiced_at") or ""
        month = inv[:7] if inv else "unknown"
        amt = float(r.get("invoice_amount") or r.get("final_amount") or r.get("estimated_amount") or 0)
        by_month.setdefault(month, {"count": 0, "total": 0.0})
        by_month[month]["count"] += 1
        by_month[month]["total"] += amt
        pn = r.get("project_name") or "Utan projekt"
        by_project.setdefault(pn, {"count": 0, "total": 0.0})
        by_project[pn]["count"] += 1
        by_project[pn]["total"] += amt

    this_month = datetime.utcnow().strftime("%Y-%m")
    this_month_data = by_month.get(this_month, {"count": 0, "total": 0.0})
    return {
        "items": rows,
        "count": len(rows),
        "total_amount": total,
        "this_month": {"count": this_month_data["count"], "total": this_month_data["total"]},
        "by_month": by_month,
        "by_project": by_project,
    }


@router.get("/api/invoicing/queue")
async def invoicing_queue(request: Request):
    """Lista alla godkända ÄTA som är redo att fakturera."""
    user = await _get_current_user(request)
    company = _cc(request, user)

    where = ["company_code = %s", "invoiced_at IS NULL",
             "((customer_decision='approve' AND status='ordered') OR status='approved_internal' OR status='signed')"]
    params = [company]

    if not _has_perm(user, "invoices.read_all"):
        name = user.get("name") or ""
        where.append(
            "project_name IN ("
            "SELECT DISTINCT project_name FROM next_project_economy "
            "WHERE project_manager ILIKE %s"
            ")"
        )
        params.append(f"%{name}%")

    rows = query_dicts(f"""
        SELECT id, ata_number, project_code, project_name, description,
               estimated_amount, final_amount, status, customer_decision,
               customer_decision_at::text, decided_at::text,
               decided_by, created_at::text
        FROM ata_register
        WHERE {' AND '.join(where)}
        ORDER BY COALESCE(customer_decision_at, decided_at, created_at) ASC
    """, tuple(params))

    total = sum(float(r.get("final_amount") or r.get("estimated_amount") or 0) for r in rows)
    by_project: Dict[str, dict] = {}
    for r in rows:
        pn = r.get("project_name") or "Utan projekt"
        by_project.setdefault(pn, {"count": 0, "total": 0.0})
        by_project[pn]["count"] += 1
        by_project[pn]["total"] += float(r.get("final_amount") or r.get("estimated_amount") or 0)
    return {"items": rows, "count": len(rows), "total_amount": total, "by_project": by_project}


@router.post("/api/ata/{ata_id}/mark-invoiced")
async def mark_ata_invoiced(ata_id: int, request: Request):
    """Markera en ÄTA som fakturerad. Body: invoice_number (required), invoice_amount (optional)."""
    user = await _get_current_user(request)
    _require_perm(user, "ekonomi.invoices_all")

    data = await request.json()
    invoice_number = (data.get("invoice_number") or "").strip()
    if not invoice_number:
        raise HTTPException(status_code=400, detail="invoice_number krävs")

    invoice_amount = data.get("invoice_amount")
    invoiced_by = user.get("name") or user.get("email") or "unknown"

    params = [invoice_number, invoiced_by]
    amt_clause = ""
    if invoice_amount is not None:
        try:
            amt_val = float(invoice_amount)
            amt_clause = ", invoice_amount = %s"
            params.append(amt_val)
        except (TypeError, ValueError):
            pass

    params.append(ata_id)
    result = query_dicts(f"""
        UPDATE ata_register
           SET invoice_number = %s,
               invoiced_at = NOW(),
               invoiced_by = %s,
               status = 'invoiced',
               updated_at = NOW()
               {amt_clause}
         WHERE id = %s
         RETURNING ata_number, status, invoice_number, invoiced_at::text
    """, tuple(params))

    if not result:
        raise HTTPException(status_code=404, detail="ÄTA not found")
    return {"status": "invoiced", **result[0]}


@router.post("/api/ata/{ata_id}/unmark-invoiced")
async def unmark_ata_invoiced(ata_id: int, request: Request):
    """Ångra en felaktig fakturerad-markering."""
    user = await _get_current_user(request)
    _require_perm(user, "ata.manage")

    result = query_dicts("""
        UPDATE ata_register
           SET invoice_number = NULL,
               invoiced_at = NULL,
               invoiced_by = NULL,
               status = 'draft',
               updated_at = NOW()
         WHERE id = %s
         RETURNING ata_number, status
    """, (ata_id,))

    if not result:
        raise HTTPException(status_code=404, detail="ÄTA not found")
    return {"status": "draft", **result[0]}
