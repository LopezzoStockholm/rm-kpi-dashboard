"""
Pipeline-modul — Deals, pipeline warnings, deal stage, hitrate, scoring, by-type, CRM audit.
Utbruten ur portal_api.py 2026-04-07.
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
_get_company_code = None


def init_pipeline_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn, get_company_code_fn=None):
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


# --- Pydantic models ---

class DealStage(BaseModel):
    deal_id: str
    stage: str


# ============================================================================
# /api/deals
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


# ============================================================================
# /api/pipeline/warnings
# ============================================================================

@router.get("/api/pipeline/warnings")
async def pipeline_warnings(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
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


# ============================================================================
# /api/deal/stage
# ============================================================================

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


# ============================================================================
# /api/pipeline/hitrate
# ============================================================================

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


# ============================================================================
# /api/pipeline/scoring
# ============================================================================

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


# ============================================================================
# /api/pipeline/by-type
# ============================================================================

@router.get("/api/pipeline/by-type")
async def pipeline_by_type(request: Request):
    user = await _get_current_user(request)
    company = _cc(request, user)
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


# ============================================================================
# /api/audit (CRM audit)
# ============================================================================

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
# /api/pipeline/project-status — Deal enriched with project operational data
# ============================================================================

@router.get("/api/pipeline/project-status")
async def pipeline_project_status(request: Request):
    """Enrich each deal that has a project_code with operational data:
       hours, cost, invoiced, collected, UE cost, UE outstanding."""
    user = await _get_current_user(request)
    company = _cc(request, user)
    try:
        rows = query_dicts("""
            WITH project_hours AS (
                SELECT project_code,
                       COALESCE(SUM(total_hours), 0) as hours,
                       COALESCE(SUM(total_revenue), 0) as revenue,
                       COALESCE(SUM(total_cost), 0) as cost
                FROM time_report
                WHERE company_code = %s
                GROUP BY project_code
            ),
            project_invoiced AS (
                SELECT project_code,
                       COALESCE(SUM(total), 0) as invoiced,
                       COALESCE(SUM(balance), 0) as outstanding
                FROM fortnox_invoice
                WHERE company_code = %s AND status != 'cancelled'
                GROUP BY project_code
            ),
            project_ue AS (
                SELECT project_code,
                       COALESCE(SUM(total), 0) as ue_total,
                       COALESCE(SUM(balance), 0) as ue_outstanding
                FROM fortnox_supplier_invoice
                WHERE company_code = %s AND project_code IS NOT NULL
                GROUP BY project_code
            )
            SELECT
                d.id, d.twenty_id, d.name, d.stage, d.deal_type,
                d.estimated_value, d.calculated_value, d.customer_name, d.owner,
                d.next_project_no as project_code,
                COALESCE(ph.hours, 0) as hours,
                COALESCE(ph.revenue, 0) as earned_revenue,
                COALESCE(ph.cost, 0) as earned_cost,
                COALESCE(pi.invoiced, 0) as invoiced,
                COALESCE(pi.outstanding, 0) as outstanding,
                COALESCE(pi.invoiced, 0) - COALESCE(pi.outstanding, 0) as collected,
                COALESCE(pu.ue_total, 0) as ue_total,
                COALESCE(pu.ue_outstanding, 0) as ue_outstanding
            FROM pipeline_deal d
            LEFT JOIN project_hours ph ON ph.project_code = d.next_project_no
            LEFT JOIN project_invoiced pi ON pi.project_code = d.next_project_no
            LEFT JOIN project_ue pu ON pu.project_code = d.next_project_no
            WHERE d.company_code = %s
            ORDER BY d.stage, d.estimated_value DESC
        """, (company, company, company, company))

        # Classify operational status per deal
        for r in rows:
            pc = r.get("project_code")
            collected = float(r.get("collected") or 0)
            invoiced = float(r.get("invoiced") or 0)
            hours = float(r.get("hours") or 0)
            earned = float(r.get("earned_revenue") or 0)

            if not pc:
                r["op_status"] = "pipeline"
                r["op_label"] = "Enbart pipeline"
            elif collected > 0 and invoiced > 0 and float(r.get("outstanding") or 0) == 0:
                r["op_status"] = "collected"
                r["op_label"] = "Inkasserat"
            elif invoiced > 0:
                r["op_status"] = "invoiced"
                r["op_label"] = "Fakturerat"
            elif hours > 0 or earned > 0:
                r["op_status"] = "active"
                r["op_label"] = "Aktivt projekt"
            else:
                r["op_status"] = "linked"
                r["op_label"] = "Projektkod tilldelad"

            # Numeric cleanup
            for k in ("hours", "earned_revenue", "earned_cost", "invoiced",
                       "outstanding", "collected", "ue_total", "ue_outstanding",
                       "estimated_value", "calculated_value"):
                r[k] = float(r.get(k) or 0)

        # Summary stats
        summary = {
            "total": len(rows),
            "pipeline_only": sum(1 for r in rows if r["op_status"] == "pipeline"),
            "linked": sum(1 for r in rows if r["op_status"] == "linked"),
            "active": sum(1 for r in rows if r["op_status"] == "active"),
            "invoiced": sum(1 for r in rows if r["op_status"] == "invoiced"),
            "collected": sum(1 for r in rows if r["op_status"] == "collected"),
            "total_earned": sum(r["earned_revenue"] for r in rows),
            "total_invoiced": sum(r["invoiced"] for r in rows),
            "total_collected": sum(r["collected"] for r in rows),
            "total_ue": sum(r["ue_total"] for r in rows),
        }

        return {"summary": summary, "deals": rows}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
