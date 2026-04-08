"""
Ekonomi-modul — Fortnox summary, fakturor, leverantörsfakturor, kassaflöde, label, projektlönsamhet.
Utbruten ur portal_api.py 2026-04-07.
"""

import json
from datetime import date
from typing import Dict, Any
from collections import defaultdict

from fastapi import APIRouter, HTTPException, Request, Depends
from rm_data import query_dicts

router = APIRouter()

# --- Dependency injection (set by init_finance_router) ---
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None


def init_finance_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn):
    global _get_current_user, _has_perm, _require_perm, _audit_log
    _get_current_user = get_current_user_fn
    _has_perm = has_perm_fn
    _require_perm = require_perm_fn
    _audit_log = audit_log_fn


# ============================================================================
# /api/fortnox/summary
# ============================================================================

@router.get("/api/fortnox/summary")
async def fortnox_summary(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    try:
        data: Dict[str, Any] = {"company": company}

        # Revenue & credit notes (rolling 12 months)
        rev = query_dicts("""
            SELECT
                COALESCE(SUM(CASE WHEN NOT is_credit AND status != 'cancelled' THEN total ELSE 0 END),0)::numeric(14,0) as revenue,
                COALESCE(SUM(CASE WHEN is_credit AND status != 'cancelled' THEN total ELSE 0 END),0)::numeric(14,0) as credit_notes,
                COALESCE(SUM(CASE WHEN balance > 0 AND status != 'cancelled' THEN balance ELSE 0 END),0)::numeric(14,0) as receivables,
                COALESCE(SUM(CASE WHEN balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled' THEN balance ELSE 0 END),0)::numeric(14,0) as overdue_receivables,
                COUNT(*) FILTER (WHERE NOT is_credit AND status != 'cancelled') as invoice_count
            FROM fortnox_invoice
            WHERE company_code=%s AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
        """, (company,))
        r = rev[0] if rev else {}
        data["revenue"] = float(r.get("revenue", 0))
        data["credit_notes"] = float(r.get("credit_notes", 0))
        data["net_revenue"] = data["revenue"] - abs(data["credit_notes"])
        data["receivables"] = float(r.get("receivables", 0))
        data["overdue_receivables"] = float(r.get("overdue_receivables", 0))
        data["invoice_count"] = int(r.get("invoice_count", 0))

        # Overdue label breakdown
        labels = query_dicts("""
            SELECT
                COALESCE(SUM(balance) FILTER (WHERE (label IS NULL OR label = '' OR label = 'Bevakas')),0)::numeric(14,0) as overdue_real,
                COALESCE(SUM(balance) FILTER (WHERE label IN ('Parkerad','Tvist')),0)::numeric(14,0) as overdue_labeled
            FROM fortnox_invoice
            WHERE company_code=%s AND balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled'
        """, (company,))
        lb = labels[0] if labels else {}
        data["overdue_real"] = float(lb.get("overdue_real", 0))
        data["overdue_labeled"] = float(lb.get("overdue_labeled", 0))

        # CFO risk classification
        risk = query_dicts("""
            SELECT
                COALESCE(SUM(balance) FILTER (WHERE due_date >= CURRENT_DATE AND (label IS NULL OR label = '')),0)::numeric(14,0) as recv_safe,
                COALESCE(SUM(balance) FILTER (WHERE label = 'Bevakas'),0)::numeric(14,0) as recv_risk,
                COALESCE(SUM(balance) FILTER (WHERE due_date < CURRENT_DATE AND (label IS NULL OR label = '' OR label = 'Bevakas')),0)::numeric(14,0) as recv_critical,
                COALESCE(SUM(balance) FILTER (WHERE label = 'Parkerad'),0)::numeric(14,0) as recv_parked
            FROM fortnox_invoice
            WHERE company_code=%s AND balance > 0 AND status != 'cancelled'
        """, (company,))
        rk = risk[0] if risk else {}
        data["recv_safe"] = float(rk.get("recv_safe", 0))
        data["recv_risk"] = float(rk.get("recv_risk", 0))
        data["recv_critical"] = float(rk.get("recv_critical", 0))
        data["recv_parked"] = float(rk.get("recv_parked", 0))
        data["recv_weighted"] = data["recv_safe"] + data["recv_risk"] * 0.5 + data["recv_critical"] * 0.25

        # Supplier costs & payables
        sup = query_dicts("""
            SELECT
                COALESCE(SUM(total),0)::numeric(14,0) as supplier_costs,
                COALESCE(SUM(CASE WHEN balance > 0 THEN balance ELSE 0 END),0)::numeric(14,0) as payables,
                COALESCE(SUM(CASE WHEN balance > 0 AND due_date < CURRENT_DATE THEN balance ELSE 0 END),0)::numeric(14,0) as overdue_payables,
                COUNT(*) as supplier_invoice_count
            FROM fortnox_supplier_invoice
            WHERE company_code=%s AND status != 'cancelled' AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
        """, (company,))
        s = sup[0] if sup else {}
        data["supplier_costs"] = float(s.get("supplier_costs", 0))
        data["payables"] = float(s.get("payables", 0))
        data["overdue_payables"] = float(s.get("overdue_payables", 0))
        data["supplier_invoice_count"] = int(s.get("supplier_invoice_count", 0))
        data["tb1"] = data["net_revenue"] - data["supplier_costs"]
        data["tb1_margin"] = round((data["tb1"] / data["net_revenue"] * 100), 1) if data["net_revenue"] > 0 else 0

        # Top 5 customers
        top_cust = query_dicts("""
            SELECT customer_name as name, SUM(total)::numeric(14,0) as total, COUNT(*) as count
            FROM fortnox_invoice
            WHERE company_code=%s AND NOT is_credit AND status != 'cancelled'
                AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY customer_name ORDER BY SUM(total) DESC LIMIT 5
        """, (company,))
        data["top_customers"] = top_cust

        # Top 5 suppliers
        top_sup = query_dicts("""
            SELECT supplier_name as name, SUM(total)::numeric(14,0) as total, COUNT(*) as count
            FROM fortnox_supplier_invoice
            WHERE company_code=%s AND status != 'cancelled'
                AND invoice_date >= CURRENT_DATE - INTERVAL '12 months'
            GROUP BY supplier_name ORDER BY SUM(total) DESC LIMIT 5
        """, (company,))
        data["top_suppliers"] = top_sup

        # Bank balance
        bank = query_dicts("""
            SELECT balance::numeric(14,2) as balance, balance_date::text, updated_at::text
            FROM bank_balance WHERE company_code=%s
        """, (company,))
        data["bank"] = bank[0] if bank else None

        # Fortnox deep-link app_id
        try:
            with open("/opt/rm-infra/fortnox-config.json") as f:
                fnx_cfg = json.load(f)
            data["app_id"] = fnx_cfg.get("app_id", "")
        except Exception:
            data["app_id"] = ""

        data["period_label"] = "Rullande 12 mån"
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/fortnox/invoices
# ============================================================================

@router.get("/api/fortnox/invoices")
async def fortnox_invoices(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    if not _has_perm(user, "invoices.read_all"):
        return {"invoices": [], "count": 0}
    try:
        invoices = query_dicts("""
            SELECT fortnox_id, customer_name, invoice_date::text, due_date::text,
                   total::numeric(14,0), balance::numeric(14,0),
                   COALESCE(label,'') as label, status,
                   CASE WHEN due_date < CURRENT_DATE THEN true ELSE false END as is_overdue
            FROM fortnox_invoice
            WHERE company_code=%s AND balance > 0 AND status != 'cancelled'
            ORDER BY label NULLS LAST, due_date
        """, (company,))

        summary = {
            "parkerad_count": len([i for i in invoices if i.get("label") == "Parkerad"]),
            "parkerad_total": sum(float(i["balance"]) for i in invoices if i.get("label") == "Parkerad"),
            "bevakas_count": len([i for i in invoices if i.get("label") == "Bevakas"]),
            "bevakas_total": sum(float(i["balance"]) for i in invoices if i.get("label") == "Bevakas"),
            "unpaid_count": len([i for i in invoices if not i.get("is_overdue") and not i.get("label")]),
            "unpaid_total": sum(float(i["balance"]) for i in invoices if not i.get("is_overdue") and not i.get("label")),
            "overdue_unlabeled_count": len([i for i in invoices if i.get("is_overdue") and not i.get("label")]),
            "overdue_unlabeled_total": sum(float(i["balance"]) for i in invoices if i.get("is_overdue") and not i.get("label")),
        }

        return {"count": len(invoices), "summary": summary, "invoices": invoices}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/fortnox/supplier-invoices
# ============================================================================

@router.get("/api/fortnox/supplier-invoices")
async def fortnox_supplier_invoices(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    if not _has_perm(user, "invoices.read_all"):
        return {"invoices": [], "count": 0}
    try:
        invoices = query_dicts("""
            SELECT fortnox_id, supplier_name, due_date::text, balance::numeric(14,0),
                   COALESCE(label,'') as label,
                   CASE WHEN due_date < CURRENT_DATE THEN true ELSE false END as is_overdue
            FROM fortnox_supplier_invoice
            WHERE company_code=%s AND balance > 0 AND status != 'cancelled'
            ORDER BY label NULLS LAST, due_date
        """, (company,))

        summary = {
            "parkerad_count": len([i for i in invoices if i.get("label") == "Parkerad"]),
            "parkerad_total": sum(float(i["balance"]) for i in invoices if i.get("label") == "Parkerad"),
            "bevakas_count": len([i for i in invoices if i.get("label") == "Bevakas"]),
            "bevakas_total": sum(float(i["balance"]) for i in invoices if i.get("label") == "Bevakas"),
            "overdue_unlabeled_count": len([i for i in invoices if i.get("is_overdue") and not i.get("label")]),
            "overdue_unlabeled_total": sum(float(i["balance"]) for i in invoices if i.get("is_overdue") and not i.get("label")),
        }

        return {"count": len(invoices), "summary": summary, "invoices": invoices}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/fortnox/cashflow
# ============================================================================

@router.get("/api/fortnox/cashflow")
async def fortnox_cashflow(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    if not _has_perm(user, "cashflow.read"):
        return {"weeks": [], "current_bank": 0, "summary": {}}
    try:
        data: Dict[str, Any] = {}

        # Receivables per week (risk-weighted)
        forecast_recv = query_dicts("""
            SELECT
                CASE WHEN due_date < CURRENT_DATE THEN 0
                     ELSE GREATEST(EXTRACT(WEEK FROM due_date) - EXTRACT(WEEK FROM CURRENT_DATE)
                          + 52 * (EXTRACT(YEAR FROM due_date) - EXTRACT(YEAR FROM CURRENT_DATE)), 0)
                END::int as week_offset,
                SUM(balance)::numeric(14,0) as amount,
                SUM(CASE
                    WHEN label = 'Parkerad' THEN 0
                    WHEN label = 'Tvist' THEN 0
                    WHEN label = 'Bevakas' THEN balance * 0.5
                    WHEN due_date < CURRENT_DATE AND (label IS NULL OR label = '') THEN balance * 0.25
                    ELSE balance
                END)::numeric(14,0) as weighted_amount
            FROM fortnox_invoice
            WHERE company_code=%s AND balance > 0 AND status != 'cancelled'
            GROUP BY 1 ORDER BY 1
        """, (company,))

        # Payables per week (adjusted)
        forecast_pay = query_dicts("""
            SELECT
                CASE WHEN due_date < CURRENT_DATE THEN 0
                     ELSE GREATEST(EXTRACT(WEEK FROM due_date) - EXTRACT(WEEK FROM CURRENT_DATE)
                          + 52 * (EXTRACT(YEAR FROM due_date) - EXTRACT(YEAR FROM CURRENT_DATE)), 0)
                END::int as week_offset,
                SUM(balance)::numeric(14,0) as amount,
                SUM(CASE
                    WHEN label = 'Parkerad' THEN 0
                    WHEN label = 'Tvist' THEN 0
                    WHEN label = 'Bevakas' THEN balance * 0.5
                    ELSE balance
                END)::numeric(14,0) as adjusted_amount
            FROM fortnox_supplier_invoice
            WHERE company_code=%s AND balance > 0 AND status != 'cancelled'
            GROUP BY 1 ORDER BY 1
        """, (company,))

        # Build weekly arrays
        today = date.today()
        current_week = today.isocalendar()[1]
        recv_by_week = {int(r["week_offset"]): float(r["amount"]) for r in forecast_recv}
        recv_w_by_week = {int(r["week_offset"]): float(r["weighted_amount"]) for r in forecast_recv}
        pay_by_week = {int(r["week_offset"]): float(r["amount"]) for r in forecast_pay}
        pay_a_by_week = {int(r["week_offset"]): float(r["adjusted_amount"]) for r in forecast_pay}

        forecast_weeks = []
        for w in range(52):
            abs_week = ((current_week - 1 + w) % 52) + 1
            forecast_weeks.append({
                "label": f"V{abs_week}",
                "inbetalningar": recv_by_week.get(w, 0),
                "inbetalningar_viktat": recv_w_by_week.get(w, 0),
                "utbetalningar": pay_by_week.get(w, 0),
                "utbetalningar_justerat": pay_a_by_week.get(w, 0),
            })
        data["forecast_weeks"] = forecast_weeks

        # Salary events (löner & AGI)
        salary_nettolon = 180150
        salary_skv = 126218
        salary_events = []
        for month_offset in range(0, 13):
            m = today.month + month_offset
            y = today.year + (m - 1) // 12
            m = ((m - 1) % 12) + 1
            try:
                pay_date = date(y, m, 25)
            except ValueError:
                pay_date = date(y, m, 28)
            if pay_date >= today:
                salary_events.append({
                    "date": pay_date.isoformat(), "type": "nettolon",
                    "amount": salary_nettolon,
                    "label": f"Nettolön {pay_date.strftime('%b')}"
                })
            try:
                skv_date = date(y, m, 12)
            except ValueError:
                skv_date = date(y, m, 11)
            if skv_date >= today:
                salary_events.append({
                    "date": skv_date.isoformat(), "type": "skv",
                    "amount": salary_skv,
                    "label": f"SKV AGI+skatt {skv_date.strftime('%b')}"
                })
        data["salary_events"] = salary_events
        data["salary_nettolon"] = salary_nettolon
        data["salary_skv"] = salary_skv

        # Bank balance
        bank = query_dicts("""
            SELECT balance::numeric(14,2) as balance, balance_date::text, updated_at::text
            FROM bank_balance WHERE company_code=%s
        """, (company,))
        data["bank"] = bank[0] if bank else None

        # Revenue forecast (6 month rolling average)
        rev_hist = query_dicts("""
            SELECT to_char(invoice_date, 'YYYY-MM') as month,
                   SUM(CASE WHEN NOT is_credit THEN total ELSE -total END)::numeric(14,0) as netto
            FROM fortnox_invoice
            WHERE company_code=%s AND status != 'cancelled'
                AND invoice_date >= date_trunc('month', CURRENT_DATE) - interval '6 months'
                AND invoice_date < date_trunc('month', CURRENT_DATE)
            GROUP BY 1 ORDER BY 1
        """, (company,))
        data["revenue_history"] = rev_hist
        if rev_hist:
            avg_monthly = sum(float(r["netto"]) for r in rev_hist) / len(rev_hist)
            data["revenue_avg_monthly"] = round(avg_monthly)
            data["revenue_avg_weekly"] = round(avg_monthly / 4.33)
            data["revenue_months_used"] = len(rev_hist)
        else:
            data["revenue_avg_monthly"] = 0
            data["revenue_avg_weekly"] = 0
            data["revenue_months_used"] = 0

        # Recurring costs (fasta kostnader)
        recurring_raw = query_dicts("""
            SELECT description, category,
                   ROUND(monthly_amount) as amount,
                   COALESCE(payment_interval, 1) as interval
            FROM recurring_cost
            WHERE company_code=%s AND is_fixed = true
            ORDER BY monthly_amount DESC
        """, (company,))
        recurring_total = sum(float(r['amount']) for r in recurring_raw) if recurring_raw else 0
        data["recurring_total_monthly"] = round(recurring_total)

        # Build recurring events per month from today forward
        recurring_events_raw = []
        for rc in (recurring_raw or []):
            amount_monthly = float(rc['amount'])
            interval = int(rc.get('interval', 1))
            event_amount = round(amount_monthly * interval)
            start_m = today.month
            start_y = today.year
            mo = 0
            while mo < 13:
                m = start_m + mo
                y = start_y + (m - 1) // 12
                m = ((m - 1) % 12) + 1
                try:
                    ev_date = date(y, m, 1)
                except ValueError:
                    mo += interval
                    continue
                if ev_date >= today:
                    recurring_events_raw.append({
                        'date_key': ev_date.isoformat(),
                        'amount': event_amount,
                    })
                mo += interval

        events_by_date = defaultdict(lambda: {'amount': 0, 'count': 0})
        for ev in recurring_events_raw:
            events_by_date[ev['date_key']]['amount'] += ev['amount']
            events_by_date[ev['date_key']]['count'] += 1
        data["recurring_events"] = [
            {"date": d, "amount": v['amount'],
             "label": f"Fasta kostnader ({v['count']} poster)"}
            for d, v in sorted(events_by_date.items())
        ]

        # Månadsmoms (VAT)
        vat_raw = query_dicts("""
            SELECT to_char(invoice_date, 'YYYY-MM') as month,
                   COALESCE(SUM(CASE WHEN source='customer' THEN calculated_vat ELSE 0 END), 0)::numeric(14,0) as vat_out,
                   COALESCE(SUM(CASE WHEN source='supplier' THEN calculated_vat ELSE 0 END), 0)::numeric(14,0) as vat_in
            FROM (
                SELECT invoice_date, total - (total / 1.25) as calculated_vat, 'customer' as source
                FROM fortnox_invoice WHERE company_code=%s AND status != 'cancelled' AND NOT is_credit
                UNION ALL
                SELECT invoice_date, total - (total / 1.25) as calculated_vat, 'supplier' as source
                FROM fortnox_supplier_invoice WHERE company_code=%s AND status != 'cancelled'
            ) sub
            WHERE invoice_date >= date_trunc('month', CURRENT_DATE) - interval '2 months'
            GROUP BY 1 ORDER BY 1
        """, (company, company))
        vat_events = []
        for vm in (vat_raw or []):
            net = float(vm['vat_out']) - float(vm['vat_in'])
            parts = vm['month'].split('-')
            vy, vmo = int(parts[0]), int(parts[1])
            pay_m = vmo + 1
            pay_y = vy
            if pay_m > 12:
                pay_m = 1
                pay_y += 1
            try:
                pay_date = date(pay_y, pay_m, 12)
            except ValueError:
                pay_date = date(pay_y, pay_m, 11)
            if pay_date >= today:
                vat_events.append({
                    "date": pay_date.isoformat(),
                    "net": round(net),
                    "vat_out": round(float(vm['vat_out'])),
                    "vat_in": round(float(vm['vat_in'])),
                    "report_month": vm['month'],
                    "label": f"Moms {vm['month']}"
                })
        data["vat_events"] = vat_events

        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/label
# ============================================================================

@router.post("/api/label")
async def set_invoice_label(request: Request):
    user = await _get_current_user(request)
    try:
        body = await request.json()
        inv_type = body.get("type", "supplier")
        fortnox_id = body.get("fortnox_id", "")
        label = body.get("label", "")
        company = body.get("company_code", "RM")

        if not fortnox_id:
            raise HTTPException(status_code=400, detail="fortnox_id required")

        valid_labels = ["", "Parkerad", "Bevakas", "Tvist"]
        if label not in valid_labels:
            raise HTTPException(status_code=400, detail=f"Invalid label. Valid: {valid_labels}")

        table = "fortnox_supplier_invoice" if inv_type == "supplier" else "fortnox_invoice"

        result = query_dicts(
            f"UPDATE {table} SET label = %s WHERE company_code = %s AND fortnox_id = %s RETURNING fortnox_id",
            (label if label else None, company, fortnox_id)
        )

        if not result:
            raise HTTPException(status_code=404, detail=f"Invoice {fortnox_id} not found")

        # Try to sync label to Fortnox Comments via label API
        fortnox_synced = False
        try:
            import urllib.request, json as _json
            label_body = _json.dumps({
                "type": inv_type, "fortnox_id": fortnox_id,
                "label": label, "company_code": company
            }).encode()
            req = urllib.request.Request(
                "http://localhost:8082/api/label",
                data=label_body,
                method="POST",
                headers={"Content-Type": "application/json", "X-Api-Key": "rm-label-2026"}
            )
            resp = urllib.request.urlopen(req, timeout=10)
            resp_data = _json.loads(resp.read())
            fortnox_synced = resp_data.get("fortnox_synced", False)
        except Exception:
            pass

        return {
            "ok": True,
            "fortnox_id": fortnox_id,
            "label": label,
            "type": inv_type,
            "fortnox_synced": fortnox_synced
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/fortnox/projects
# ============================================================================

@router.get("/api/fortnox/projects")
async def fortnox_projects(request: Request, company: str = "RM"):
    user = await _get_current_user(request)
    try:
        # Individual projects
        individual = query_dicts("""
            SELECT project_number, project_name, COALESCE(project_group,'') as project_group,
                   COALESCE(project_leader,'') as project_leader,
                   revenue::numeric(14,0), net_revenue::numeric(14,0),
                   supplier_costs::numeric(14,0), tb1::numeric(14,0), tb1_margin,
                   invoice_count, supplier_invoice_count
            FROM project_profitability
            WHERE company_code=%s AND status='ONGOING' AND (net_revenue > 0 OR supplier_costs > 0)
            ORDER BY supplier_costs DESC
        """, (company,))

        # Grouped by project_group
        grouped = query_dicts("""
            SELECT COALESCE(NULLIF(project_group,''), project_name) as group_name,
                   SUM(net_revenue)::numeric(14,0) as net_revenue,
                   SUM(supplier_costs)::numeric(14,0) as supplier_costs,
                   SUM(tb1)::numeric(14,0) as tb1,
                   CASE WHEN SUM(net_revenue) > 0
                        THEN ((SUM(tb1) / SUM(net_revenue)) * 100)::numeric(6,1)
                        ELSE 0 END as tb1_margin,
                   SUM(invoice_count) as invoice_count,
                   SUM(supplier_invoice_count) as supplier_invoice_count,
                   string_agg(DISTINCT project_leader, ', ') FILTER (WHERE project_leader != '') as leaders
            FROM project_unified
            WHERE company_code=%s AND fortnox_status='ONGOING' AND (net_revenue > 0 OR supplier_costs > 0)
            GROUP BY COALESCE(NULLIF(project_group,''), project_name)
            ORDER BY supplier_costs DESC
        """, (company,))

        return {"individual": individual, "grouped": grouped}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
