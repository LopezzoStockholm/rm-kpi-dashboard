#!/usr/bin/env python3
"""
Fortnox project profitability sync → PostgreSQL.
Fetches ongoing projects with revenue, credits, supplier costs, calculates TB1.
v2: Fixed Description field, added project_leader, project_group.
"""
import json, sys, subprocess, os, time
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode

CONFIG_PATH = "/opt/rm-infra/fortnox-config.json"
COMPANY_CODE = "RM"
BASE_URL = "https://api.fortnox.se/3"
LOG_PREFIX = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

def refresh_token_if_needed(cfg):
    expires = datetime.fromisoformat(cfg.get("token_expires", "2000-01-01T00:00:00"))
    if datetime.now() < expires - timedelta(minutes=5):
        return cfg
    print(f"{LOG_PREFIX()} Refreshing access token...")
    data = urlencode({
        "grant_type": "refresh_token",
        "refresh_token": cfg["refresh_token"],
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
    }).encode()
    req = Request("https://apps.fortnox.se/oauth-v1/token", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    resp = urlopen(req)
    token_data = json.loads(resp.read())
    cfg["access_token"] = token_data["access_token"]
    cfg["refresh_token"] = token_data["refresh_token"]
    cfg["token_expires"] = (datetime.now() + timedelta(seconds=token_data.get("expires_in", 3600))).isoformat()
    save_config(cfg)
    print(f"{LOG_PREFIX()} Token refreshed, expires {cfg['token_expires']}")
    return cfg

def fortnox_get(cfg, endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    if params:
        url += "?" + urlencode(params)
    req = Request(url)
    req.add_header("Authorization", f"Bearer {cfg['access_token']}")
    req.add_header("Accept", "application/json")
    try:
        resp = urlopen(req)
        return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            print(f"{LOG_PREFIX()} Rate limited, waiting 6s...")
            time.sleep(6)
            return fortnox_get(cfg, endpoint, params)
        print(f"{LOG_PREFIX()} API error {e.code}: {e.read().decode()[:200]}")
        raise

def fetch_all_pages(cfg, endpoint, key, params=None):
    all_items = []
    page = 1
    while True:
        p = dict(params or {})
        p["page"] = page
        p["limit"] = 500
        data = fortnox_get(cfg, endpoint, p)
        items = data.get(key, [])
        all_items.extend(items)
        meta = data.get("MetaInformation", {})
        total_pages = meta.get("@TotalPages", 1)
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.15)
    return all_items

def psql(query, db="rm_central"):
    r = subprocess.run(
        ["docker", "exec", "-i", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-t", "-A"],
        input=query, capture_output=True, text=True
    )
    if r.returncode != 0:
        print(f"{LOG_PREFIX()} SQL error: {r.stderr[:300]}")
    return r.stdout.strip()

def escape_sql(val):
    if val is None:
        return "NULL"
    s = str(val).replace("'", "''")
    return f"'{s}'"

def sync_projects(cfg):
    print(f"{LOG_PREFIX()} Fetching all projects...")
    projects = fetch_all_pages(cfg, "projects", "Projects")
    print(f"{LOG_PREFIX()} Got {len(projects)} total projects")
    if not projects:
        return 0

    # Load group mappings
    group_raw = psql("SELECT project_number, group_name FROM project_group_map WHERE company_code='RM'")
    group_map = {}
    for line in group_raw.split("\n"):
        if "|" in line:
            parts = line.split("|")
            group_map[parts[0].strip()] = parts[1].strip()

    ongoing = [p for p in projects if p.get("Status") == "ONGOING"]
    print(f"{LOG_PREFIX()} Processing {len(ongoing)} ongoing projects")

    sql_parts = [f"DELETE FROM project_profitability WHERE company_code='{COMPANY_CODE}';"]

    for proj in ongoing:
        proj_nr = proj.get("ProjectNumber", "")
        proj_name = proj.get("Description", "")
        proj_leader = proj.get("ProjectLeader", "") or ""
        status = proj.get("Status", "ONGOING")
        proj_group = group_map.get(proj_nr, "")

        print(f"{LOG_PREFIX()} {proj_nr} - {proj_name} (ledare: {proj_leader}, grupp: {proj_group or '-'})")

        # Customer invoices: query OUR database (has row-level project codes)
        rev_raw = psql(f"SELECT COALESCE(SUM(total),0) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND project_code='{proj_nr}' AND NOT is_credit AND status != 'cancelled'")
        revenue = float(rev_raw or 0)
        cred_raw = psql(f"SELECT COALESCE(SUM(ABS(total)),0) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND project_code='{proj_nr}' AND is_credit AND status != 'cancelled'")
        credits = float(cred_raw or 0)
        inv_cnt_raw = psql(f"SELECT COUNT(*) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND project_code='{proj_nr}' AND NOT is_credit AND status != 'cancelled'")
        invoice_count = int(inv_cnt_raw or 0)

        # Supplier invoices: still use Fortnox API (project filter works for these)
        supplier_invoices = fetch_all_pages(cfg, "supplierinvoices", "SupplierInvoices", {"project": proj_nr})
        time.sleep(0.15)
        supplier_costs = sum(float(inv.get("Total", 0)) for inv in supplier_invoices
                             if not inv.get("Cancelled", False) and not inv.get("Cancel", False))
        supplier_invoice_count = len([i for i in supplier_invoices
                                      if not i.get("Cancelled", False) and not i.get("Cancel", False)])

        net_revenue = revenue - credits
        tb1 = net_revenue - supplier_costs
        tb1_margin = (tb1 / net_revenue * 100) if net_revenue > 0 else 0

        sql_parts.append(
            f"INSERT INTO project_profitability "
            f"(company_code, project_number, project_name, status, revenue, credit_notes, net_revenue, "
            f"supplier_costs, tb1, tb1_margin, invoice_count, supplier_invoice_count, project_leader, project_group) "
            f"VALUES ('{COMPANY_CODE}', {escape_sql(proj_nr)}, {escape_sql(proj_name)}, "
            f"{escape_sql(status)}, {revenue}, {credits}, {net_revenue}, "
            f"{supplier_costs}, {tb1}, {tb1_margin}, {invoice_count}, {supplier_invoice_count}, "
            f"{escape_sql(proj_leader)}, {escape_sql(proj_group)}) "
            f"ON CONFLICT (company_code, project_number) DO UPDATE SET "
            f"project_name=EXCLUDED.project_name, status=EXCLUDED.status, "
            f"revenue=EXCLUDED.revenue, credit_notes=EXCLUDED.credit_notes, "
            f"net_revenue=EXCLUDED.net_revenue, supplier_costs=EXCLUDED.supplier_costs, "
            f"tb1=EXCLUDED.tb1, tb1_margin=EXCLUDED.tb1_margin, "
            f"invoice_count=EXCLUDED.invoice_count, supplier_invoice_count=EXCLUDED.supplier_invoice_count, "
            f"project_leader=EXCLUDED.project_leader, project_group=EXCLUDED.project_group, "
            f"synced_at=NOW();"
        )

    batch_sql = "\n".join(sql_parts)
    psql(batch_sql)
    return len(ongoing)

def main():
    print(f"\n{'='*60}")
    print(f"{LOG_PREFIX()} Project profitability sync starting for {COMPANY_CODE}")
    print(f"{'='*60}")

    if not os.path.exists(CONFIG_PATH):
        print(f"{LOG_PREFIX()} ERROR: Config not found at {CONFIG_PATH}")
        sys.exit(1)

    cfg = load_config()
    cfg = refresh_token_if_needed(cfg)
    proj_count = sync_projects(cfg)

    # Summary
    summary = psql(f"""
        SELECT project_number, project_name, project_group,
               net_revenue::numeric(14,0), supplier_costs::numeric(14,0),
               tb1::numeric(14,0), tb1_margin::numeric(6,1)
        FROM project_profitability
        WHERE company_code='{COMPANY_CODE}' AND (net_revenue > 0 OR supplier_costs > 0)
        ORDER BY supplier_costs DESC
    """)
    print(f"\n{LOG_PREFIX()} Sync complete: {proj_count} projects")
    print(f"\nProjekt med ekonomi:")
    for line in summary.split("\n"):
        if line.strip():
            print(f"  {line}")

    # Grouped summary
    grouped = psql(f"""
        SELECT COALESCE(NULLIF(project_group,''), project_name) as grp,
               SUM(net_revenue)::numeric(14,0) as rev,
               SUM(supplier_costs)::numeric(14,0) as cost,
               SUM(tb1)::numeric(14,0) as tb1
        FROM project_profitability
        WHERE company_code='{COMPANY_CODE}' AND (net_revenue > 0 OR supplier_costs > 0)
        GROUP BY grp ORDER BY cost DESC
    """)
    print(f"\nGrupperad sammanfattning:")
    for line in grouped.split("\n"):
        if line.strip():
            print(f"  {line}")

if __name__ == "__main__":
    main()
