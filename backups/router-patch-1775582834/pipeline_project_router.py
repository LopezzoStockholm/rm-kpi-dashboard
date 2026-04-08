"""
Affär & Projekt — Pipeline, deals, scoring, CRM audit + Projektöversikt, Next economy, unified.
Modul 4 i RM OS. Sammanfogad från pipeline_router.py + project_router.py 2026-04-07.
"""

import json
import subprocess
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel
from rm_data import query_dicts

router = APIRouter()

TWENTY_SCHEMA = "workspace_13e0qz9uia3v9w5dx0mk6etm5"

# --- Dependency injection ---
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None


def init_pipeline_project_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn):
    global _get_current_user, _has_perm, _require_perm, _audit_log
    _get_current_user = get_current_user_fn
    _has_perm = has_perm_fn
    _require_perm = require_perm_fn
    _audit_log = audit_log_fn


# --- Pydantic models ---

class DealStage(BaseModel):
    deal_id: str
    stage: str


# ============================================================================
# PIPELINE — Deals, warnings, stage, hitrate, scoring, by-type, CRM audit
# ============================================================================

@router.get("/api/deals")
async def get_deals(request: Request):
    user = await _get_current_user(request)
    try:
        role = user.get("role", "")
        twenty_id = user.get("twenty_member_id", "")

        owner_filter = ""
        params = []
        if role not in ("vd", "ekonomi") and twenty_id:
            owner_filter = "WHERE owner = %s"
            params.append(twenty_id)
        elif role not in ("vd", "ekonomi") and not twenty_id:
            owner_filter = "WHERE 1=0"

        deals = query_dicts(f"""
            SELECT id, twenty_id, name, stage, calculated_value, estimated_value, customer_name, owner
            FROM pipeline_deal
            {owner_filter}
            ORDER BY calculated_value DESC
        """, tuple(params) if params else None)

        PLACEHOLDER_VALUE = 1_000_000
        for d in deals:
            ev = float(d.get("estimated_value") or 0)
            d["needs_estimate"] = (ev == PLACEHOLDER_VALUE) or (ev == 0)
        warnings = [d for d in deals if d["needs_estimate"]]
        return {
            "count": len(deals),
            "deals": deals,
            "warnings": {
                "needs_estimate_count": len(warnings),
                "needs_estimate_deals": [{"name": d["name"], "stage": d["stage"]} for d in warnings]
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pipeline/warnings")
async def pipeline_warnings(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    try:
        placeholder = query_dicts("""
            SELECT name, stage, owner, estimated_value::bigint
            FROM pipeline_deal
            WHERE company_code=%s AND estimated_value = 1000000
            ORDER BY stage, name
        """, (company,))

        no_value = query_dicts("""
            SELECT name, stage, owner
            FROM pipeline_deal
            WHERE company_code=%s AND (estimated_value IS NULL OR estimated_value = 0)
            ORDER BY name
        """, (company,))

        stale = []
        total_warnings = len(placeholder) + len(no_value) + len(stale)

        return {
            "total_warnings": total_warnings,
            "needs_estimate": {
                "count": len(placeholder),
                "message": f"{len(placeholder)} deals har platshållarvärde (1 MSEK) och behöver riktiga uppskattningar",
                "deals": placeholder
            },
            "no_value": {
                "count": len(no_value),
                "message": f"{len(no_value)} deals saknar uppskattat värde",
                "deals": no_value
            },
            "stale": {
                "count": len(stale),
                "message": f"{len(stale)} deals i kalkyl/förhandling har inte uppdaterats på 30+ dagar",
                "deals": stale
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/deal/stage")
async def update_deal_stage(body: DealStage, request: Request):
    user = await _get_current_user(request)
    try:
        stage = body.stage.upper()
        # Update Twenty CRM
        result = query_dicts(
            f'UPDATE {TWENTY_SCHEMA}.opportunity SET stage = %s, "updatedAt" = NOW() WHERE id = %s RETURNING id, name, stage::text as stage',
            (stage, body.deal_id),
            db="twenty"
        )
        if not result:
            raise HTTPException(status_code=404, detail="Deal not found")
        # Sync pipeline_deal cache
        query_dicts(
            "UPDATE pipeline_deal SET stage = %s WHERE twenty_id = %s",
            (stage.lower(), body.deal_id)
        )
        return {"success": True, "deal": result[0]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pipeline/hitrate")
async def pipeline_hitrate(request: Request):
    user = await _get_current_user(request)
    try:
        rows = query_dicts("SELECT deal_type, stage, hitrate::int as hitrate FROM hitrate_matrix ORDER BY deal_type, stage")
        matrix: Dict[str, Dict[str, int]] = {}
        for r in rows:
            dt = r["deal_type"]
            if dt not in matrix:
                matrix[dt] = {}
            matrix[dt][r["stage"]] = r["hitrate"]
        return {"matrix": matrix, "row_count": len(rows)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pipeline/scoring")
async def pipeline_scoring(request: Request):
    user = await _get_current_user(request)
    try:
        subprocess.run(
            ["python3", "/opt/rm-infra/deal_scoring.py"],
            capture_output=True, timeout=30
        )
        role = user.get("role", "")
        twenty_id = user.get("twenty_member_id", "")

        owner_filter = ""
        params = []
        if role not in ("vd", "ekonomi") and twenty_id:
            owner_filter = "WHERE ds.twenty_id IN (SELECT twenty_id FROM pipeline_deal WHERE owner = %s)"
            params.append(twenty_id)
        elif role not in ("vd", "ekonomi"):
            owner_filter = "WHERE 1=0"

        scores = query_dicts(f"""
            SELECT ds.twenty_id, ds.deal_name, ds.score, ds.score_value, ds.score_stage,
                   ds.score_type, ds.score_hygiene, ds.score_margin, ds.action
            FROM deal_score ds
            {owner_filter}
            ORDER BY ds.score DESC
        """, tuple(params) if params else None)
        return {"count": len(scores), "scores": scores}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/pipeline/by-type")
async def pipeline_by_type(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    try:
        role = user.get("role", "")
        twenty_id = user.get("twenty_member_id", "")

        owner_filter_deal = ""
        owner_filter_unified = ""
        deal_params = [company]
        unified_params = []

        if role not in ("vd", "ekonomi") and twenty_id:
            owner_filter_deal = "AND d.owner = %s"
            deal_params.append(twenty_id)
            owner_filter_unified = "AND d.owner = %s"
            unified_params.append(twenty_id)
        elif role not in ("vd", "ekonomi"):
            owner_filter_deal = "AND 1=0"
            owner_filter_unified = "AND 1=0"

        # TB per affärstyp (from Next economy)
        tb = query_dicts(f"""
            SELECT
                COALESCE(d.deal_type, 'okand') as affarstyp,
                count(DISTINCT u.deal_name)::int as antal_projekt,
                round(sum(COALESCE(u.booked_revenue,0)))::bigint as intakter,
                round(sum(COALESCE(u.booked_cost,0)))::bigint as kostnader,
                round(sum(COALESCE(u.contribution_margin,0)))::bigint as tb,
                CASE WHEN sum(COALESCE(u.booked_revenue,0)) > 0
                    THEN round(sum(COALESCE(u.contribution_margin,0)) / sum(u.booked_revenue) * 100, 1)
                    ELSE 0 END::float as tb_pct,
                round(sum(COALESCE(u.earned_revenue_not_invoiced,0)))::bigint as ej_fakturerat
            FROM crm_next_unified u
            LEFT JOIN pipeline_deal d ON u.opportunity_id::text = d.twenty_id
            WHERE (u.booked_revenue > 0 OR u.booked_cost > 0) {owner_filter_unified}
            GROUP BY COALESCE(d.deal_type, 'okand')
            ORDER BY sum(COALESCE(u.contribution_margin,0)) DESC
        """, tuple(unified_params) if unified_params else None)

        # Pipeline per affärstyp
        pipe = query_dicts(f"""
            SELECT
                deal_type as affarstyp,
                count(*)::int as deals,
                round(sum(estimated_value))::bigint as pipeline_value,
                round(sum(estimated_value * hit_rate / 100))::bigint as viktat_value
            FROM pipeline_deal d WHERE d.company_code=%s {owner_filter_deal}
            GROUP BY d.deal_type
            ORDER BY sum(d.estimated_value * d.hit_rate / 100) DESC
        """, tuple(deal_params))

        return {"tb_per_type": tb, "pipeline_per_type": pipe}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/audit")
async def crm_audit(request: Request):
    user = await _get_current_user(request)
    try:
        rows = query_dicts("SELECT details, created_at::text FROM crm_audit ORDER BY created_at DESC LIMIT 1")
        if rows and rows[0].get("details"):
            details = rows[0]["details"]
            if isinstance(details, str):
                details = json.loads(details)
            return {"audit": details, "created_at": rows[0].get("created_at")}
        return {"audit": None, "created_at": None}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# PROJEKT — Projektöversikt, Next economy, CRM+Next unified
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


@router.get("/api/next/economy")
async def next_economy(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
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
