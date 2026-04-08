"""
ata_pdf_archive — builds and versions ÄTA PDFs on every state change.

Provides a single entry point persist_ata_pdf_version() that:
  1. Fetches ata_register row
  2. Generates PDF bytes (reuses same layout as /api/ata/{id}/pdf endpoint)
  3. Writes to /opt/rm-infra/uploads/ata_pdfs/YYYY/MM/ATA-YYYY-NNN_v{N}.pdf
  4. Inserts a row into ata_document_version

Called from state-change hooks (PATCH /api/ata/{id}) and on-demand.
"""
from __future__ import annotations
from pathlib import Path
from datetime import datetime
from io import BytesIO
from typing import Optional
import logging

log = logging.getLogger(__name__)

ARCHIVE_ROOT = Path("/opt/rm-infra/ata-archive/versions")


def _fetch_ata_and_project(ata_id: int) -> Optional[dict]:
    """Fetch ÄTA row + project leader + customer from next_project_economy."""
    from rm_data import query_dicts
    rows = query_dicts(
        """SELECT id, ata_number, project_code, project_name, description,
                  estimated_amount, final_amount, status, reported_by, decided_by,
                  decided_at::text as decided_at, created_at::text as created_at,
                  notes, company_code
           FROM ata_register WHERE id = %s""",
        (ata_id,), db="rm_central"
    )
    if not rows:
        return None
    ata = rows[0]
    proj_name = ata.get("project_name") or ""
    if proj_name:
        pinfo = query_dicts(
            "SELECT project_manager, customer_name FROM next_project_economy WHERE project_name = %s LIMIT 1",
            (proj_name,), db="rm_central"
        )
        if pinfo:
            ata["_project_leader"] = pinfo[0].get("project_manager")
            ata["_customer_name"] = pinfo[0].get("customer_name")
    return ata


def build_ata_pdf_bytes(ata: dict) -> bytes:
    """Generate PDF bytes from an ata dict. Same layout as /api/ata/{id}/pdf."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=20*mm, rightMargin=20*mm,
                             topMargin=18*mm, bottomMargin=18*mm,
                             title=f"ÄTA {ata['ata_number']}")
    styles = getSampleStyleSheet()
    h_style = ParagraphStyle('H', parent=styles['Heading1'], fontSize=18, textColor=colors.HexColor('#0D1117'), spaceAfter=6)
    sub_style = ParagraphStyle('Sub', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#6E7681'), spaceAfter=14)
    label_style = ParagraphStyle('Label', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#6E7681'), spaceAfter=2)
    value_style = ParagraphStyle('Value', parent=styles['Normal'], fontSize=11, textColor=colors.HexColor('#0D1117'), spaceAfter=10)
    body_style = ParagraphStyle('Body', parent=styles['Normal'], fontSize=10, leading=14, textColor=colors.HexColor('#0D1117'))
    story = []
    story.append(Paragraph("RM Entreprenad och Fasad AB", ParagraphStyle('Company', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#F0883E'))))
    story.append(Paragraph(f"Beställning av ÄTA-arbete — {ata['ata_number']}", h_style))
    date_str = ""
    try:
        date_str = datetime.fromisoformat(ata['created_at'].replace('+00', '+00:00')).strftime("%Y-%m-%d") if ata.get('created_at') else ""
    except Exception:
        pass
    story.append(Paragraph(f"Inkom: {date_str} &nbsp;&nbsp;•&nbsp;&nbsp; Org.nr 559251-1462 &nbsp;&nbsp;•&nbsp;&nbsp; Status: {ata.get('status','')}", sub_style))

    amount_val = ata.get('final_amount') or ata.get('estimated_amount')
    meta = [
        ["Projekt", f"{ata.get('project_code') or '—'} &nbsp;{ata.get('project_name') or ''}"],
        ["Beställare", ata.get('_customer_name') or "—"],
        ["Projektledare (RM)", ata.get('_project_leader') or "—"],
        ["Rapporterad av (fält)", ata.get('reported_by') or "—"],
        ["Belopp", f"{int(amount_val):,} kr".replace(",", " ") if amount_val else "Tidersättning / enligt ÅF-pris"],
    ]
    meta_tbl = Table([[Paragraph(k, label_style), Paragraph(v, value_style)] for k, v in meta], colWidths=[45*mm, 120*mm])
    meta_tbl.setStyle(TableStyle([("VALIGN", (0,0), (-1,-1), "TOP"), ("BOTTOMPADDING", (0,0), (-1,-1), 4)]))
    story.append(meta_tbl)
    story.append(Spacer(1, 10))
    story.append(Paragraph("Beskrivning av arbetet", ParagraphStyle('H2', parent=styles['Heading2'], fontSize=12, textColor=colors.HexColor('#0D1117'), spaceAfter=6)))
    story.append(Paragraph((ata.get('description') or '').replace('\n', '<br/>'), body_style))
    story.append(Spacer(1, 14))
    ref_box = Table([[Paragraph(
        "<b>Avtalsreferens:</b> Denna ÄTA är en beställning av tillkommande arbete enligt gällande entreprenadavtal "
        "(ABS 18 / AB 04 / ABT 06). Arbete påbörjas först efter skriftligt godkännande från beställaren "
        "(signatur nedan eller svar via e-post). Uppskattat belopp är preliminärt och regleras enligt faktiskt utfört arbete "
        "samt avtalade priser.", body_style
    )]], colWidths=[165*mm])
    ref_box.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,-1), colors.HexColor('#FFF8E7')),
        ("BOX", (0,0), (-1,-1), 0.5, colors.HexColor('#E0C068')),
        ("LEFTPADDING", (0,0), (-1,-1), 10), ("RIGHTPADDING", (0,0), (-1,-1), 10),
        ("TOPPADDING", (0,0), (-1,-1), 8), ("BOTTOMPADDING", (0,0), (-1,-1), 8),
    ]))
    story.append(ref_box)
    story.append(Spacer(1, 22))
    story.append(Paragraph("Godkännande — beställaren", ParagraphStyle('H2b', parent=styles['Heading2'], fontSize=12, textColor=colors.HexColor('#0D1117'), spaceAfter=10)))
    sig_tbl = Table([["", "", ""], ["Underskrift", "Namnförtydligande", "Datum"]], colWidths=[65*mm, 55*mm, 45*mm], rowHeights=[24*mm, 6*mm])
    sig_tbl.setStyle(TableStyle([("LINEABOVE", (0,1), (-1,1), 0.5, colors.HexColor('#0D1117')), ("FONTSIZE", (0,1), (-1,1), 8), ("TEXTCOLOR", (0,1), (-1,1), colors.HexColor('#6E7681')), ("ALIGN", (0,1), (-1,1), "LEFT")]))
    story.append(sig_tbl)
    story.append(Spacer(1, 20))
    story.append(Paragraph(
        f"Alternativt: svara på mejlet med denna PDF bifogad med texten <b>\"Godkänt {ata['ata_number']}\"</b> "
        "så arkiveras svaret som beställarens godkännande.",
        ParagraphStyle('Fine', parent=styles['Normal'], fontSize=8, textColor=colors.HexColor('#6E7681'), leading=11)
    ))
    doc.build(story)
    return buf.getvalue()


def persist_ata_pdf_version(ata_id: int, trigger_event: str, user_name: Optional[str] = None, stamped: bool = False) -> Optional[dict]:
    """Build, archive, and version a PDF for the ÄTA.

    Returns {version, filepath, file_size_bytes} or None if ÄTA not found.
    Safe to call on every state change — each call creates a new version row.
    """
    from rm_data import query_dicts, execute
    ata = _fetch_ata_and_project(ata_id)
    if not ata:
        log.warning(f"persist_ata_pdf_version: ata_id={ata_id} not found")
        return None
    try:
        pdf_bytes = build_ata_pdf_bytes(ata)
    except Exception as e:
        log.error(f"persist_ata_pdf_version: PDF build failed for {ata_id}: {e}")
        return None

    # Figure out next version
    last = query_dicts(
        "SELECT COALESCE(MAX(version),0) AS v FROM ata_document_version WHERE ata_id = %s",
        (ata_id,), db="rm_central"
    )
    next_version = (last[0]["v"] if last else 0) + 1

    # Write to disk
    now = datetime.now()
    rel_dir = Path(f"{now.year:04d}/{now.month:02d}")
    abs_dir = ARCHIVE_ROOT / rel_dir
    abs_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{ata['ata_number']}_v{next_version}.pdf"
    abs_path = abs_dir / filename
    abs_path.write_bytes(pdf_bytes)
    # Store path relative to uploads root so it maps to the /uploads/... URL
    url_path = f"ata-archive/versions/{rel_dir}/{filename}"  # relative to /opt/rm-infra; served via /api/ata/documents/{id}/file

    amount_val = ata.get("final_amount") or ata.get("estimated_amount")
    new_id = execute(
        """INSERT INTO ata_document_version
           (ata_id, ata_number, project_code, project_name, version, filepath,
            file_size_bytes, stamped, state_at_generation, amount_at_generation,
            trigger_event, generated_by)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
        (ata_id, ata["ata_number"], ata.get("project_code"), ata.get("project_name"),
         next_version, url_path, len(pdf_bytes), stamped,
         ata.get("status") or "unknown", amount_val, trigger_event, user_name),
        db="rm_central", returning=True
    )
    log.info(f"persist_ata_pdf_version: {ata['ata_number']} v{next_version} -> {url_path} ({len(pdf_bytes)} bytes)")
    return {"id": new_id, "version": next_version, "filepath": url_path, "file_size_bytes": len(pdf_bytes)}
