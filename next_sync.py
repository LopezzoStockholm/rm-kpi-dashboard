#!/usr/bin/env python3
"""
next_sync.py — Synkar projektdata från Next Tech v1 REST API till PostgreSQL.
Använder curl (bypassa Cloudflare) med Bearer token.
Token: permanent, ingen refresh behövs.
"""
import json, sys, subprocess, os, time
from datetime import datetime

CONFIG_PATH = "/opt/rm-infra/next-config.json"
COMPANY_CODE = "RM"
API_BASE = "https://api.next-tech.com/v1"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def curl_get(url, token):
    """GET via curl för att bypassa Cloudflare."""
    r = subprocess.run(
        ["curl", "-s", "-w", "\n__HTTP__%{http_code}", url,
         "-H", f"Authorization: Bearer {token}",
         "-H", "Accept: application/json"],
        capture_output=True, text=True, timeout=30
    )
    if r.returncode != 0:
        log(f"curl error {r.returncode}: {url} — {r.stderr[:200]}")
        return None
    parts = r.stdout.rsplit("\n__HTTP__", 1)
    body = parts[0] if parts else r.stdout
    http_code = int(parts[1]) if len(parts) > 1 else 0
    if http_code >= 400:
        log(f"HTTP {http_code}: {url} — {body[:200]}")
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        log(f"JSON parse error: {body[:200]}")
        return None

def psql(query, db="rm_central"):
    r = subprocess.run(
        ["docker", "exec", "-i", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-t", "-A"],
        input=query, capture_output=True, text=True
    )
    if r.returncode != 0:
        log(f"SQL error: {r.stderr[:300]}")
    return r.stdout.strip()

def esc(val):
    if val is None:
        return "NULL"
    s = str(val).replace("'", "''")
    return f"'{s}'"

def num(val):
    try:
        return float(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0

def num_or_null(val):
    """Returnera NULL om None, annars float."""
    if val is None:
        return "NULL"
    try:
        return str(float(val))
    except (ValueError, TypeError):
        return "NULL"

def date_or_null(val):
    if val is None:
        return "NULL"
    return esc(str(val)[:10])

def fetch_all_projects(token):
    """Hämtar alla projekt från /v1/project/."""
    data = curl_get(f"{API_BASE}/project/?page=1&size=100", token)
    if not data:
        return []
    items = data.get("items", data) if isinstance(data, dict) else data
    return items if isinstance(items, list) else []

def fetch_overview(token, project_id):
    """Hämtar ekonomiöversikt för ett projekt."""
    return curl_get(f"{API_BASE}/project/{project_id}/overview", token)

def sync_projects(token):
    """Synkar alla projekt med ekonomidata."""
    log("Hämtar projektlista...")
    projects = fetch_all_projects(token)
    if not projects:
        log("Inga projekt — kontrollera token")
        return 0

    log(f"{len(projects)} projekt hämtade")

    sql_parts = []
    count = 0
    errors = 0

    for proj in projects:
        pid = proj.get("id")
        pno = proj.get("projectno", "")
        pname = proj.get("name", "")
        status_code = proj.get("projectstatuscode")

        # Hoppa över raderade (status 0) och mall-projekt
        if status_code == 0:
            continue

        log(f"  {pno} {pname} (id={pid})...")
        ov = fetch_overview(token, pid)
        time.sleep(0.15)  # Rate limit

        if not ov:
            errors += 1
            continue

        sql = f"""
            INSERT INTO next_project_economy (
                company_code, project_id, project_no, project_name,
                customer_name, project_manager, status_name, status_code,
                project_type, price_type, cost_center,
                booked_cost, booked_revenue, booked_hours,
                booked_awo_cost, booked_awo_revenue,
                workorder_cost, workorder_revenue,
                contribution_margin, contribution_margin_pct,
                budget_cost, budget_revenue,
                budget_contribution_margin, budget_contribution_margin_pct,
                slp_cost, slp_revenue,
                slp_contribution_margin, slp_contribution_margin_pct,
                earned_revenue, earned_revenue_not_invoiced,
                invoiceable, invoiceable_running,
                forecast, forecast_period_date,
                procurement_payment, payment_plan_amount,
                payment_plan_withheld, payment_plan_outcome,
                project_start, project_end,
                final_inspection_date, guarantee_inspection_date,
                synced_at
            ) VALUES (
                '{COMPANY_CODE}', {pid}, {esc(pno)}, {esc(ov.get('projectname', pname))},
                {esc(ov.get('customername'))},
                {esc(ov.get('projectmanagername'))},
                {esc(ov.get('projectstatusname'))},
                {num_or_null(ov.get('projectstatuscode'))},
                {esc(ov.get('projecttypename'))},
                {esc(ov.get('pricetypename'))},
                {esc(ov.get('costcentername'))},
                {num(ov.get('bookedcost'))},
                {num(ov.get('bookedrevenue'))},
                {num(ov.get('bookedhours'))},
                {num(ov.get('bookedawocost'))},
                {num(ov.get('bookedaworevenue'))},
                {num(ov.get('workordercost'))},
                {num(ov.get('workorderrevenue'))},
                {num(ov.get('contributionmarginamount'))},
                {num_or_null(ov.get('contributionmarginpercent'))},
                {num_or_null(ov.get('budgetcost'))},
                {num_or_null(ov.get('budgetrevenue'))},
                {num(ov.get('budgetcontributionmarginamount'))},
                {num_or_null(ov.get('budgetcontributionmarginpercent'))},
                {num_or_null(ov.get('slpcost'))},
                {num_or_null(ov.get('slprevenue'))},
                {num(ov.get('contributionmarginamountslp'))},
                {num_or_null(ov.get('contributionmarginpercentslp'))},
                {num(ov.get('earnedrevenue'))},
                {num(ov.get('earnedrevenuenotinvoiced'))},
                {num(ov.get('invoiceable'))},
                {num(ov.get('invoiceablerunning'))},
                {num_or_null(ov.get('forecast'))},
                {date_or_null(ov.get('forecastperioddate'))},
                {num_or_null(ov.get('procurementpaymentamount'))},
                {num_or_null(ov.get('paymentplanamount'))},
                {num_or_null(ov.get('paymentplanwithheldamount'))},
                {num_or_null(ov.get('paymentplanoutcome'))},
                {date_or_null(ov.get('projectstart'))},
                {date_or_null(ov.get('projectend'))},
                {date_or_null(ov.get('finalinspectiondate'))},
                {date_or_null(ov.get('guaranteeinspectiondate'))},
                NOW()
            ) ON CONFLICT (company_code, project_id) DO UPDATE SET
                project_name = EXCLUDED.project_name,
                customer_name = EXCLUDED.customer_name,
                project_manager = EXCLUDED.project_manager,
                status_name = EXCLUDED.status_name,
                status_code = EXCLUDED.status_code,
                project_type = EXCLUDED.project_type,
                price_type = EXCLUDED.price_type,
                cost_center = EXCLUDED.cost_center,
                booked_cost = EXCLUDED.booked_cost,
                booked_revenue = EXCLUDED.booked_revenue,
                booked_hours = EXCLUDED.booked_hours,
                booked_awo_cost = EXCLUDED.booked_awo_cost,
                booked_awo_revenue = EXCLUDED.booked_awo_revenue,
                workorder_cost = EXCLUDED.workorder_cost,
                workorder_revenue = EXCLUDED.workorder_revenue,
                contribution_margin = EXCLUDED.contribution_margin,
                contribution_margin_pct = EXCLUDED.contribution_margin_pct,
                budget_cost = EXCLUDED.budget_cost,
                budget_revenue = EXCLUDED.budget_revenue,
                budget_contribution_margin = EXCLUDED.budget_contribution_margin,
                budget_contribution_margin_pct = EXCLUDED.budget_contribution_margin_pct,
                slp_cost = EXCLUDED.slp_cost,
                slp_revenue = EXCLUDED.slp_revenue,
                slp_contribution_margin = EXCLUDED.slp_contribution_margin,
                slp_contribution_margin_pct = EXCLUDED.slp_contribution_margin_pct,
                earned_revenue = EXCLUDED.earned_revenue,
                earned_revenue_not_invoiced = EXCLUDED.earned_revenue_not_invoiced,
                invoiceable = EXCLUDED.invoiceable,
                invoiceable_running = EXCLUDED.invoiceable_running,
                forecast = EXCLUDED.forecast,
                forecast_period_date = EXCLUDED.forecast_period_date,
                procurement_payment = EXCLUDED.procurement_payment,
                payment_plan_amount = EXCLUDED.payment_plan_amount,
                payment_plan_withheld = EXCLUDED.payment_plan_withheld,
                payment_plan_outcome = EXCLUDED.payment_plan_outcome,
                project_start = EXCLUDED.project_start,
                project_end = EXCLUDED.project_end,
                final_inspection_date = EXCLUDED.final_inspection_date,
                guarantee_inspection_date = EXCLUDED.guarantee_inspection_date,
                synced_at = NOW();
        """
        sql_parts.append(sql)
        count += 1

        bc = num(ov.get('bookedcost'))
        br = num(ov.get('bookedrevenue'))
        cm = num(ov.get('contributionmarginamount'))
        if bc > 0 or br > 0:
            log(f"    Kostnad: {bc:,.0f} | Intäkt: {br:,.0f} | TB: {cm:,.0f}")

    if sql_parts:
        # Kör i batchar om 10 för att undvika för långa queries
        batch_size = 10
        for i in range(0, len(sql_parts), batch_size):
            batch = sql_parts[i:i+batch_size]
            psql("\n".join(batch))

    log(f"Synk klar: {count} projekt uppdaterade, {errors} fel")
    return count

def print_summary():
    """Skriv ut sammanfattning av synkad data."""
    summary = psql("""
        SELECT project_no, project_name, status_name,
               booked_cost::numeric(14,0),
               booked_revenue::numeric(14,0),
               contribution_margin::numeric(14,0),
               earned_revenue_not_invoiced::numeric(14,0)
        FROM next_project_economy
        WHERE company_code='RM' AND (booked_cost > 0 OR booked_revenue > 0)
        ORDER BY booked_cost DESC
    """)
    if summary:
        log("Projekt med ekonomi:")
        for line in summary.split("\n"):
            if line.strip():
                parts = line.split("|")
                if len(parts) >= 4:
                    log(f"  {parts[0].strip():6s} {parts[1].strip()[:25]:25s} "
                        f"Kostnad: {parts[3].strip():>12s} "
                        f"Intäkt: {parts[4].strip():>12s} "
                        f"TB: {parts[5].strip():>12s} "
                        f"Ej fakt: {parts[6].strip():>12s}")

    # Totaler
    totals = psql("""
        SELECT COUNT(*),
               SUM(booked_cost)::numeric(14,0),
               SUM(booked_revenue)::numeric(14,0),
               SUM(contribution_margin)::numeric(14,0),
               SUM(earned_revenue_not_invoiced)::numeric(14,0)
        FROM next_project_economy
        WHERE company_code='RM' AND (booked_cost > 0 OR booked_revenue > 0)
    """)
    if totals:
        log(f"Totalt: {totals}")

def main():
    log("Next Tech v1 API synk startar")
    log("=" * 50)

    cfg = load_config()
    token = cfg.get("access_token") or cfg.get("api_key")

    if not token:
        log("Ingen access_token i config — kör token-exchange först")
        sys.exit(1)

    count = sync_projects(token)
    print_summary()

if __name__ == "__main__":
    main()
