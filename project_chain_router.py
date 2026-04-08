"""
Projektkedja — visar hela flödet: timmar → kostnad → fakturerbart → fakturerat → inkasserat → UE-betalbarhet.
Fas 1 i tidrapporterings-frontend. 2026-04-08.
"""

from fastapi import APIRouter, Request
from rm_data import query_dicts

router = APIRouter()

_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_project_chain_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn, get_company_code_fn=None):
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user_fn
    _has_perm = has_perm_fn
    _require_perm = require_perm_fn
    _audit_log = audit_log_fn
    _get_company_code = get_company_code_fn


def _cc(request, user):
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"


@router.get("/api/project-chain")
async def get_project_chain(request: Request):
    """Full kedjevy per projekt: timmar → kostnad → fakturerbart → fakturerat → inkasserat → UE."""
    user = await _get_current_user(request)
    _require_perm(user, "time.read_all")
    cc = _cc(request, user)

    # Time report aggregation per project
    time_agg = query_dicts("""
        SELECT
            project_code,
            COUNT(*) as entry_count,
            COALESCE(SUM(hours), 0) as total_hours,
            COALESCE(SUM(total_cost), 0) as total_cost,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(CASE WHEN approved THEN total_revenue ELSE 0 END), 0) as approved_revenue,
            COALESCE(SUM(CASE WHEN attested THEN total_revenue ELSE 0 END), 0) as attested_revenue,
            COALESCE(SUM(CASE WHEN report_type = 'subcontractor' THEN total_cost ELSE 0 END), 0) as ue_cost,
            COALESCE(SUM(CASE WHEN report_type = 'subcontractor' THEN hours ELSE 0 END), 0) as ue_hours,
            COALESCE(SUM(CASE WHEN report_type = 'internal' THEN total_cost ELSE 0 END), 0) as intern_cost,
            COALESCE(SUM(CASE WHEN report_type = 'internal' THEN hours ELSE 0 END), 0) as intern_hours,
            COUNT(CASE WHEN NOT COALESCE(approved, false) AND NOT COALESCE(attested, false) THEN 1 END) as draft_count,
            COUNT(CASE WHEN approved AND NOT COALESCE(attested, false) THEN 1 END) as approved_count,
            COUNT(CASE WHEN attested THEN 1 END) as attested_count,
            MIN(work_date) as first_date,
            MAX(work_date) as last_date
        FROM time_report
        WHERE company_code = %s
          AND project_code IS NOT NULL
        GROUP BY project_code
        ORDER BY MAX(work_date) DESC
    """, [cc])

    project_codes = [r["project_code"] for r in time_agg]
    if not project_codes:
        return {"projects": [], "totals": {
            "hours": 0, "cost": 0, "attested_revenue": 0,
            "invoiced": 0, "collected": 0, "ue_cost": 0,
            "uninvoiced_work": 0, "ue_payable": 0, "ue_waiting": 0
        }}

    # Customer invoices per project
    placeholders = ",".join(["%s"] * len(project_codes))
    cust_inv = query_dicts(f"""
        SELECT
            project_code,
            COALESCE(SUM(total), 0) as invoiced,
            COALESCE(SUM(balance), 0) as outstanding,
            COALESCE(SUM(total) - SUM(balance), 0) as collected,
            COUNT(*) as invoice_count,
            COUNT(CASE WHEN balance > 0 THEN 1 END) as unpaid_count
        FROM fortnox_invoice
        WHERE company_code = %s
          AND project_code IN ({placeholders})
          AND COALESCE(status, '') != 'cancelled'
        GROUP BY project_code
    """, [cc] + project_codes)
    cust_map = {r["project_code"]: r for r in cust_inv}

    # Supplier invoices per project
    sup_inv = query_dicts(f"""
        SELECT
            project_code,
            COALESCE(SUM(total), 0) as sup_invoiced,
            COALESCE(SUM(balance), 0) as sup_outstanding,
            COUNT(*) as sup_invoice_count
        FROM fortnox_supplier_invoice
        WHERE company_code = %s
          AND project_code IN ({placeholders})
        GROUP BY project_code
    """, [cc] + project_codes)
    sup_map = {r["project_code"]: r for r in sup_inv}

    # Next project economy (for project names and extra data)
    next_proj = query_dicts(f"""
        SELECT project_no, project_name, customer_name, project_manager,
               earned_revenue_not_invoiced, invoiceable, contribution_margin_pct,
               budget_revenue, budget_cost
        FROM next_project_economy
        WHERE company_code = %s
          AND project_no IN ({placeholders})
    """, [cc] + project_codes)
    next_map = {r["project_no"]: r for r in next_proj}

    # Build chain per project
    projects = []
    for t in time_agg:
        pc = t["project_code"]
        ci = cust_map.get(pc, {})
        si = sup_map.get(pc, {})
        nx = next_map.get(pc, {})

        invoiced = float(ci.get("invoiced", 0))
        collected = float(ci.get("collected", 0))
        outstanding = float(ci.get("outstanding", 0))
        attested_rev = float(t["attested_revenue"])
        ue_cost = float(t["ue_cost"])

        # UE payable = UE cost on projects where customer has fully paid
        # (outstanding = 0 means all customer invoices paid)
        customer_fully_paid = outstanding <= 0 and invoiced > 0
        ue_payable = ue_cost if customer_fully_paid else 0
        ue_waiting = ue_cost - ue_payable

        # Uninvoiced work = attested revenue - invoiced amount (floor at 0)
        uninvoiced = max(0, attested_rev - invoiced)

        # Chain health: how far money has flowed
        # green = collected >= attested, yellow = invoiced but not collected, red = work not invoiced
        if attested_rev <= 0:
            chain_status = "empty"
        elif uninvoiced > attested_rev * 0.3:
            chain_status = "red"
        elif outstanding > invoiced * 0.3:
            chain_status = "yellow"
        else:
            chain_status = "green"

        projects.append({
            "project_code": pc,
            "project_name": nx.get("project_name", ""),
            "customer_name": nx.get("customer_name", ""),
            "project_manager": nx.get("project_manager", ""),
            # Time data
            "total_hours": float(t["total_hours"]),
            "intern_hours": float(t["intern_hours"]),
            "ue_hours": float(t["ue_hours"]),
            "entry_count": t["entry_count"],
            # Cost chain
            "total_cost": float(t["total_cost"]),
            "intern_cost": float(t["intern_cost"]),
            "ue_cost": ue_cost,
            # Revenue chain
            "total_revenue": float(t["total_revenue"]),
            "attested_revenue": attested_rev,
            "invoiced": invoiced,
            "collected": collected,
            "outstanding": outstanding,
            # Gaps
            "uninvoiced_work": uninvoiced,
            "ue_payable": ue_payable,
            "ue_waiting": ue_waiting,
            # Status counts
            "draft_count": t["draft_count"],
            "approved_count": t["approved_count"],
            "attested_count": t["attested_count"],
            "invoice_count": ci.get("invoice_count", 0),
            "unpaid_invoice_count": ci.get("unpaid_count", 0),
            # Chain health
            "chain_status": chain_status,
            # Dates
            "first_date": str(t["first_date"]) if t["first_date"] else None,
            "last_date": str(t["last_date"]) if t["last_date"] else None,
            # Next economy extras
            "next_earned_not_invoiced": float(nx.get("earned_revenue_not_invoiced", 0) or 0),
            "next_invoiceable": float(nx.get("invoiceable", 0) or 0),
            "budget_revenue": float(nx.get("budget_revenue", 0) or 0),
            "tb_pct": float(nx.get("contribution_margin_pct", 0) or 0),
        })

    # Totals
    totals = {
        "hours": sum(p["total_hours"] for p in projects),
        "cost": sum(p["total_cost"] for p in projects),
        "attested_revenue": sum(p["attested_revenue"] for p in projects),
        "invoiced": sum(p["invoiced"] for p in projects),
        "collected": sum(p["collected"] for p in projects),
        "ue_cost": sum(p["ue_cost"] for p in projects),
        "uninvoiced_work": sum(p["uninvoiced_work"] for p in projects),
        "ue_payable": sum(p["ue_payable"] for p in projects),
        "ue_waiting": sum(p["ue_waiting"] for p in projects),
    }

    return {"projects": projects, "totals": totals}


@router.get("/api/invoicing/uninvoiced-work")
async def get_uninvoiced_work(request: Request):
    """Attesterat arbete som inte fakturerats — för 'Att fakturera'-fliken."""
    user = await _get_current_user(request)
    _require_perm(user, "time.read_all")
    cc = _cc(request, user)

    rows = query_dicts("""
        WITH time_agg AS (
            SELECT
                project_code,
                SUM(total_revenue) as attested_revenue,
                SUM(hours) as attested_hours,
                COUNT(*) as entry_count,
                MAX(attested_at) as last_attested
            FROM time_report
            WHERE company_code = %s
              AND attested = true
              AND project_code IS NOT NULL
            GROUP BY project_code
        ),
        inv_agg AS (
            SELECT
                project_code,
                COALESCE(SUM(total), 0) as invoiced
            FROM fortnox_invoice
            WHERE company_code = %s
              AND COALESCE(status, '') != 'cancelled'
              AND project_code IS NOT NULL
            GROUP BY project_code
        )
        SELECT
            t.project_code,
            COALESCE(n.project_name, '') as project_name,
            COALESCE(n.customer_name, '') as customer_name,
            t.attested_revenue,
            COALESCE(i.invoiced, 0) as invoiced,
            GREATEST(t.attested_revenue - COALESCE(i.invoiced, 0), 0) as uninvoiced,
            t.attested_hours,
            t.entry_count,
            t.last_attested,
            EXTRACT(DAY FROM NOW() - t.last_attested) as days_since_attested
        FROM time_agg t
        LEFT JOIN inv_agg i ON t.project_code = i.project_code
        LEFT JOIN next_project_economy n ON t.project_code = n.project_no AND n.company_code = %s
        WHERE t.attested_revenue > COALESCE(i.invoiced, 0)
        ORDER BY (t.attested_revenue - COALESCE(i.invoiced, 0)) DESC
    """, [cc, cc, cc])

    total_uninvoiced = sum(float(r["uninvoiced"]) for r in rows)

    return {
        "items": [{
            "project_code": r["project_code"],
            "project_name": r["project_name"],
            "customer_name": r["customer_name"],
            "attested_revenue": float(r["attested_revenue"]),
            "invoiced": float(r["invoiced"]),
            "uninvoiced": float(r["uninvoiced"]),
            "hours": float(r["attested_hours"]),
            "entry_count": r["entry_count"],
            "days_since_attested": int(r["days_since_attested"] or 0),
        } for r in rows],
        "total_uninvoiced": total_uninvoiced,
        "count": len(rows),
    }


@router.get("/api/ue/payable")
async def get_ue_payable(request: Request):
    """UE-fakturor som kan betalas (kundfaktura inkasserad)."""
    user = await _get_current_user(request)
    _require_perm(user, "time.read_all")
    cc = _cc(request, user)

    rows = query_dicts("""
        WITH project_paid AS (
            SELECT project_code
            FROM fortnox_invoice
            WHERE company_code = %s
              AND COALESCE(status, '') != 'cancelled'
            GROUP BY project_code
            HAVING SUM(balance) <= 0 AND SUM(total) > 0
        )
        SELECT
            si.fortnox_id,
            si.supplier_name,
            si.project_code,
            si.total,
            si.balance,
            si.due_date,
            COALESCE(n.project_name, '') as project_name,
            CASE WHEN pp.project_code IS NOT NULL THEN 'payable' ELSE 'waiting' END as ue_status
        FROM fortnox_supplier_invoice si
        LEFT JOIN project_paid pp ON si.project_code = pp.project_code
        LEFT JOIN next_project_economy n ON si.project_code = n.project_no AND n.company_code = %s
        WHERE si.company_code = %s
          AND si.balance > 0
          AND si.project_code IS NOT NULL
        ORDER BY
            CASE WHEN pp.project_code IS NOT NULL THEN 0 ELSE 1 END,
            si.due_date ASC
    """, [cc, cc, cc])

    payable = [r for r in rows if r["ue_status"] == "payable"]
    waiting = [r for r in rows if r["ue_status"] == "waiting"]

    def to_item(r):
        return {
            "fortnox_id": r["fortnox_id"],
            "supplier_name": r["supplier_name"],
            "project_code": r["project_code"],
            "project_name": r["project_name"],
            "total": float(r["total"]),
            "balance": float(r["balance"]),
            "due_date": str(r["due_date"]) if r["due_date"] else None,
            "ue_status": r["ue_status"],
        }

    return {
        "payable": [to_item(r) for r in payable],
        "waiting": [to_item(r) for r in waiting],
        "payable_total": sum(float(r["balance"]) for r in payable),
        "waiting_total": sum(float(r["balance"]) for r in waiting),
    }
