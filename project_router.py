"""
Projekt-modul — Projektöversikt (Fortnox-grupperad), Next Tech economy, CRM+Next unified view.
Utbruten ur portal_api.py 2026-04-07.
"""

from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Request, Depends
from rm_data import query_dicts

router = APIRouter()

# --- Dependency injection ---
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_project_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn, get_company_code_fn=None):
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user_fn
    _has_perm = has_perm_fn
    _require_perm = require_perm_fn
    _audit_log = audit_log_fn
    _get_company_code = get_company_code_fn


def _cc(request, user):
    """Get company code from request or user context."""
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"


# ============================================================================
# /api/projects
# ============================================================================

@router.get("/api/projects")
async def get_projects(request: Request):
    user = await _get_current_user(request)
    try:
        leader_filter = ""
        params = ["RM"]
        if not _has_perm(user, "projects.read_all"):
            name = user["name"]
            leader_filter = "AND project_leader ILIKE %s"
            params.append(f"%{name}%")

        projects = query_dicts(f"""
            SELECT COALESCE(NULLIF(project_group,''), project_name) as group_name,
                   SUM(net_revenue) as revenue,
                   SUM(supplier_costs) as costs,
                   SUM(tb1) as tb1,
                   string_agg(DISTINCT project_leader, ', ') as leaders,
                   SUM(invoice_count) as invoices,
                   SUM(supplier_invoice_count) as supplier_invoices
            FROM project_profitability
            WHERE company_code=%s {leader_filter}
            GROUP BY COALESCE(NULLIF(project_group,''), project_name)
            ORDER BY SUM(supplier_costs) DESC
        """, tuple(params))
        return {"count": len(projects), "projects": projects}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/next/economy
# ============================================================================

@router.get("/api/next/economy")
async def next_economy(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
    try:
        projects = query_dicts("""
            SELECT ne.project_no, ne.project_name as next_name,
                   ne.customer_name, ne.project_manager, ne.status_name as next_status,
                   ne.status_code,
                   ne.project_type, ne.price_type,
                   ne.booked_cost::numeric(14,0) as next_cost,
                   ne.booked_revenue::numeric(14,0) as next_revenue,
                   ne.booked_hours::numeric(10,1) as next_hours,
                   ne.booked_awo_cost::numeric(14,0) as next_awo_cost,
                   ne.booked_awo_revenue::numeric(14,0) as next_awo_revenue,
                   ne.contribution_margin::numeric(14,0) as next_tb,
                   ne.contribution_margin_pct::numeric(6,1) as next_tb_pct,
                   ne.budget_cost::numeric(14,0) as next_budget_cost,
                   ne.budget_revenue::numeric(14,0) as next_budget_rev,
                   ne.budget_contribution_margin::numeric(14,0) as next_budget_tb,
                   ne.slp_contribution_margin::numeric(14,0) as next_slp_tb,
                   ne.slp_contribution_margin_pct::numeric(6,1) as next_slp_pct,
                   ne.earned_revenue::numeric(14,0) as next_earned,
                   ne.earned_revenue_not_invoiced::numeric(14,0) as next_earned_not_inv,
                   ne.invoiceable::numeric(14,0) as next_invoiceable,
                   ne.invoiceable_running::numeric(14,0) as next_invoiceable_running,
                   ne.forecast::numeric(14,0) as next_forecast,
                   ne.payment_plan_amount::numeric(14,0) as next_payment_plan,
                   ne.payment_plan_withheld::numeric(14,0) as next_payment_withheld,
                   ne.project_start::text, ne.project_end::text,
                   ne.final_inspection_date::text,
                   COALESCE(pp.net_revenue,0)::numeric(14,0) as fnx_revenue,
                   COALESCE(pp.supplier_costs,0)::numeric(14,0) as fnx_cost,
                   COALESCE(pp.tb1,0)::numeric(14,0) as fnx_tb,
                   COALESCE(pp.tb1_margin,0)::numeric(6,1) as fnx_tb_pct,
                   COALESCE(pp.invoice_count,0) as fnx_inv_count,
                   COALESCE(pp.supplier_invoice_count,0) as fnx_si_count,
                   COALESCE(pp.project_leader,'') as fnx_leader,
                   (ne.booked_cost - COALESCE(pp.supplier_costs,0))::numeric(14,0) as cost_diff,
                   ne.synced_at::text as next_synced
            FROM next_project_economy ne
            LEFT JOIN project_profitability pp
                ON pp.project_number = ne.project_no AND pp.company_code = ne.company_code
            WHERE ne.company_code=%s
                AND (ne.booked_cost > 0 OR ne.booked_revenue > 0 OR COALESCE(pp.supplier_costs,0) > 0)
            ORDER BY GREATEST(ne.booked_cost, COALESCE(pp.supplier_costs,0)) DESC
        """, (company,))
        return {"count": len(projects), "projects": projects}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/next/unified
# ============================================================================

@router.get("/api/next/unified")
async def next_unified(request: Request):
    user = await _get_current_user(request)
    try:
        rows = query_dicts("""
            SELECT deal_name, crm_stage, next_project_no, next_id, opportunity_id::text, company_name,
                   contact_name, contact_phone, contact_email,
                   next_name, next_status, next_customer,
                   budget_revenue::numeric(14,0), budget_cost::numeric(14,0),
                   budget_contribution_margin::numeric(14,0), budget_contribution_margin_pct::numeric(6,1),
                   booked_revenue::numeric(14,0), booked_cost::numeric(14,0),
                   contribution_margin::numeric(14,0), contribution_margin_pct::numeric(6,1),
                   forecast::numeric(14,0),
                   earned_revenue::numeric(14,0), earned_revenue_not_invoiced::numeric(14,0),
                   invoiceable::numeric(14,0),
                   project_start::text, project_end::text,
                   project_manager,
                   final_inspection_date::text
            FROM crm_next_unified
            ORDER BY deal_name
        """)
        return {"count": len(rows), "unified": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ============================================================================
# /api/projects-raw  — Individual project rows (ungrouped)
# ============================================================================

@router.get("/api/projects-raw")
async def get_projects_raw(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
    try:
        leader_filter = ""
        params = [company]
        if not _has_perm(user, "projects.read_all"):
            name = user["name"]
            leader_filter = "AND project_leader ILIKE %s"
            params.append(f"%{name}%")

        rows = query_dicts(f"""
            SELECT project_number, project_name, project_group,
                   status, project_leader,
                   net_revenue::numeric(14,2) as net_revenue,
                   supplier_costs::numeric(14,2) as supplier_costs,
                   tb1::numeric(14,2) as tb1,
                   tb1_margin::numeric(6,2) as tb1_margin,
                   invoice_count, supplier_invoice_count,
                   synced_at::text
            FROM project_profitability
            WHERE company_code = %s {leader_filter}
            ORDER BY supplier_costs DESC
        """, tuple(params))
        return {"count": len(rows), "projects": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/project/{code}/summary  — Single project detail with ATAs
# ============================================================================

@router.get("/api/project/{code}/summary")
async def get_project_summary(code: str, request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
    try:
        # Project info from profitability
        projects = query_dicts("""
            SELECT project_number, project_name, project_group,
                   status, project_leader,
                   net_revenue::numeric(14,2) as net_revenue,
                   supplier_costs::numeric(14,2) as supplier_costs,
                   tb1::numeric(14,2) as tb1,
                   tb1_margin::numeric(6,2) as tb1_margin,
                   invoice_count, supplier_invoice_count,
                   synced_at::text
            FROM project_profitability
            WHERE company_code = %s AND project_number = %s
        """, (company, code))

        project = projects[0] if projects else None

        # ATAs for this project
        atas = query_dicts("""
            SELECT id, ata_number, description, status, category,
                   estimated_amount::numeric(12,2) as estimated_amount,
                   final_amount::numeric(12,2) as final_amount,
                   reported_by, decided_by,
                   customer_approved as customer_decision,
                   created_at::text, updated_at::text
            FROM ata_register
            WHERE company_code = %s AND project_code = %s
            ORDER BY ata_number
        """, (company, code))

        # PDF versions for these ATAs
        ata_ids = [a["id"] for a in atas]
        pdf_versions = []
        if ata_ids:
            try:
                placeholders = ",".join(["%s"] * len(ata_ids))
                pdf_versions = query_dicts(f"""
                    SELECT id, ata_id, ata_number, version, filepath,
                           file_size_bytes, stamped,
                           state_at_generation, amount_at_generation::numeric(12,2),
                           trigger_event, generated_by, generated_at::text
                    FROM ata_pdf_version
                    WHERE ata_id IN ({placeholders})
                    ORDER BY ata_id, version DESC
                """, tuple(ata_ids))
            except Exception:
                pdf_versions = []

        # Summary
        summary = {
            "ata_count": len(atas),
            "ata_estimated_total": sum(float(a.get("estimated_amount") or 0) for a in atas),
            "ata_final_total": sum(float(a.get("final_amount") or 0) for a in atas),
            "ata_approved_count": sum(1 for a in atas if a.get("status") in ("ordered", "signed", "invoiced")),
            "pdf_version_count": len(pdf_versions),
            "pdf_bytes_total": sum(v.get("file_size_bytes", 0) or 0 for v in pdf_versions),
        }

        return {
            "project": project,
            "atas": atas,
            "pdf_versions": pdf_versions,
            "summary": summary,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
