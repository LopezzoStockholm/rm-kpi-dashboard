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
_get_company_code = None


def init_pipeline_project_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn, get_company_code_fn=None):
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
# PROGNOS — Pipeline-driven revenue forecast (Fas 5)
# ============================================================================

@router.get("/api/forecast/revenue")
async def forecast_revenue(request: Request):
    """
    Lean pipeline-driven forecast.
    Combines: Fortnox YTD invoiced + contracted + pipeline weighted + run-rate extrapolation.
    No new tables, no new cron — pure calculation from existing synced data.
    """
    user = await _get_current_user(request)
    company = _cc(request, user)
    try:
        import datetime
        today = datetime.date.today()
        year = today.year
        month = today.month
        months_elapsed = month  # jan=1 .. dec=12
        months_remaining = 12 - months_elapsed

        # 1. YTD fakturerat (Fortnox — paid + unpaid + overdue = all non-cancelled)
        ytd_rows = query_dicts("""
            SELECT
                date_trunc('month', invoice_date)::date as month,
                SUM(total)::bigint as revenue
            FROM fortnox_invoice
            WHERE company_code = %s
              AND invoice_date >= %s
              AND invoice_date < %s
              AND status != 'cancelled'
            GROUP BY 1
            ORDER BY 1
        """, (company, f"{year}-01-01", f"{year+1}-01-01"))

        ytd_total = sum(r["revenue"] or 0 for r in ytd_rows)
        monthly_breakdown = []
        for r in ytd_rows:
            m = r["month"]
            label = m.strftime("%b") if hasattr(m, 'strftime') else str(m)[:7]
            monthly_breakdown.append({"month": label, "revenue": int(r["revenue"] or 0)})

        # 2. Run-rate extrapolation
        run_rate_monthly = ytd_total / max(months_elapsed, 1)
        run_rate_annual = run_rate_monthly * 12

        # 3. Pipeline-viktat (open deals × hitrate per stage)
        pipeline_rows = query_dicts("""
            SELECT
                d.stage,
                d.deal_type,
                d.estimated_value,
                COALESCE(h.hitrate, d.hit_rate, 0) as conversion_rate
            FROM pipeline_deal d
            LEFT JOIN hitrate_matrix h
                ON h.deal_type = d.deal_type AND h.stage = d.stage AND h.company_code = d.company_code
            WHERE d.company_code = %s
        """, (company,))

        pipeline_total = 0
        pipeline_weighted = 0
        stage_summary = {}
        for r in pipeline_rows:
            ev = float(r["estimated_value"] or 0)
            cr = float(r["conversion_rate"] or 0) / 100.0
            weighted = ev * cr
            pipeline_total += ev
            pipeline_weighted += weighted

            s = r["stage"] or "okand"
            if s not in stage_summary:
                stage_summary[s] = {"count": 0, "total": 0, "weighted": 0, "conversion_rate": cr * 100}
            stage_summary[s]["count"] += 1
            stage_summary[s]["total"] += ev
            stage_summary[s]["weighted"] += weighted

        # Convert to list sorted by weighted value
        stage_list = [
            {"stage": k, "count": v["count"], "total": int(v["total"]),
             "weighted": int(v["weighted"]), "conversion_rate": round(v["conversion_rate"], 1)}
            for k, v in sorted(stage_summary.items(), key=lambda x: -x[1]["weighted"])
        ]

        # 4. Kontrakterat men ej fakturerat (deals in stage 'kontrakterat' or 'leverans')
        kontrakterat_rows = query_dicts("""
            SELECT COALESCE(SUM(estimated_value), 0)::bigint as total
            FROM pipeline_deal
            WHERE company_code = %s
              AND stage IN ('kontrakterat', 'leverans')
        """, (company,))
        kontrakterat = int(kontrakterat_rows[0]["total"]) if kontrakterat_rows else 0

        # 5. Annual goal
        goal_rows = query_dicts("""
            SELECT target_value FROM annual_goal
            WHERE company_code = %s AND year = %s AND unit = 'MSEK' AND category = 'cash'
            LIMIT 1
        """, (company, year))
        annual_target = int(float(goal_rows[0]["target_value"]) * 1_000_000) if goal_rows else 25_000_000

        # 6. Scenarios
        # Base: YTD + kontrakterat + 50% of remaining pipeline-weighted (conservative timing)
        base_forecast = ytd_total + kontrakterat + int(pipeline_weighted * 0.5)
        # Optimistic: YTD + kontrakterat + 80% pipeline-weighted
        optimistic = ytd_total + kontrakterat + int(pipeline_weighted * 0.8)
        # Pessimistic: YTD + kontrakterat + 20% pipeline-weighted
        pessimistic = ytd_total + kontrakterat + int(pipeline_weighted * 0.2)

        gap_to_target = annual_target - base_forecast
        on_track = base_forecast >= annual_target * 0.9  # within 10%

        return {
            "year": year,
            "months_elapsed": months_elapsed,
            "annual_target": annual_target,
            "ytd_invoiced": int(ytd_total),
            "ytd_monthly": monthly_breakdown,
            "run_rate_monthly": int(run_rate_monthly),
            "run_rate_annual": int(run_rate_annual),
            "kontrakterat": kontrakterat,
            "pipeline_total": int(pipeline_total),
            "pipeline_weighted": int(pipeline_weighted),
            "pipeline_by_stage": stage_list,
            "scenarios": {
                "pessimistic": int(pessimistic),
                "base": int(base_forecast),
                "optimistic": int(optimistic),
            },
            "gap_to_target": int(gap_to_target),
            "on_track": on_track,
            "pct_of_target": round(base_forecast / max(annual_target, 1) * 100, 1),
        }
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
                       COALESCE(SUM(hours), 0) as hours,
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
            LEFT JOIN project_hours ph ON ph.project_code = d.next_project_no AND d.next_project_no IS NOT NULL AND d.next_project_no != ''
            LEFT JOIN project_invoiced pi ON pi.project_code = d.next_project_no AND d.next_project_no IS NOT NULL AND d.next_project_no != ''
            LEFT JOIN project_ue pu ON pu.project_code = d.next_project_no AND d.next_project_no IS NOT NULL AND d.next_project_no != ''
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

        # PDF versions for these ATAs (table may not exist yet)
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

        # Invoices for this project
        invoices = []
        try:
            invoices = query_dicts("""
                SELECT fortnox_id, customer_name, invoice_date::text,
                       due_date::text, total::numeric(14,2) as total,
                       balance::numeric(14,2) as balance, status, label
                FROM fortnox_invoice
                WHERE company_code = %s AND project_code = %s
                ORDER BY invoice_date DESC
            """, (company, code))
        except Exception:
            pass

        # Supplier invoices for this project
        supplier_invoices = []
        try:
            supplier_invoices = query_dicts("""
                SELECT fortnox_id, supplier_name, invoice_date::text,
                       due_date::text, total::numeric(14,2) as total,
                       balance::numeric(14,2) as balance, status, label
                FROM fortnox_supplier_invoice
                WHERE company_code = %s AND project_code = %s
                ORDER BY invoice_date DESC
            """, (company, code))
        except Exception:
            pass

        # Time report summary for this project
        time_summary = {}
        try:
            ts = query_dicts("""
                SELECT
                    COALESCE(SUM(hours), 0) as total_hours,
                    COALESCE(SUM(CASE WHEN report_type = 'internal' OR report_type IS NULL THEN hours END), 0) as intern_hours,
                    COALESCE(SUM(CASE WHEN report_type = 'subcontractor' THEN hours END), 0) as ue_hours,
                    COALESCE(SUM(total_cost), 0)::numeric(14,2) as total_cost,
                    COALESCE(SUM(total_revenue), 0)::numeric(14,2) as total_revenue,
                    COUNT(*) as entry_count,
                    SUM(CASE WHEN attested THEN 1 ELSE 0 END) as attested_count
                FROM time_report
                WHERE company_code = %s AND project_code = %s
            """, (company, code))
            if ts:
                time_summary = ts[0]
        except Exception:
            pass

        return {
            "project": project,
            "atas": atas,
            "pdf_versions": pdf_versions,
            "summary": summary,
            "invoices": invoices,
            "supplier_invoices": supplier_invoices,
            "time_summary": time_summary,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
