#!/usr/bin/env python3
"""
Fortnox → PostgreSQL sync for RM Entreprenad och Fasad.
READ-ONLY — fetches invoices, supplier invoices, labels.
v3: label support for customer invoices.
"""
import json, sys, subprocess, os, time, re, base64
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


def extract_app_id_from_jwt(access_token):
    """
    Extraherar app_id ur Fortnox JWT access token.
    Fortnox inkluderar den ej idag, men funktionen framtidssäkrar för när de gör det.
    Returnerar None om inte hittad.
    """
    try:
        parts = access_token.split('.')
        if len(parts) != 3:
            return None
        payload_padded = parts[1] + '=' * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_padded))
        for field in ['appId', 'app_id', 'applicationId', 'integrationId']:
            val = payload.get(field)
            if isinstance(val, str) and re.match(r'^[0-9a-f]{32}$', val.lower()):
                return val
        return None
    except Exception:
        return None

def validate_app_id(app_id):
    """Validerar att app_id är 32 hexadecimala tecken (Fortnox SPA routing-id)."""
    if not app_id:
        return False
    return bool(re.match(r'^[0-9a-f]{32}$', str(app_id).lower()))

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

    # Försök extrahera app_id direkt ur ny JWT (framtidssäker)
    discovered = extract_app_id_from_jwt(cfg["access_token"])
    if discovered and discovered != cfg.get("app_id"):
        print(f"{LOG_PREFIX()} [app_id] Ny app_id hittad i JWT: {discovered} (ersätter {cfg.get('app_id','saknas')})")
        cfg["app_id"] = discovered
        save_config(cfg)
    elif not validate_app_id(cfg.get("app_id", "")):
        print(f"{LOG_PREFIX()} [app_id] VARNING: app_id saknas eller ogiltigt format!")
        print(f"{LOG_PREFIX()} [app_id] Uppdatera 'app_id' i {CONFIG_PATH}")
        print(f"{LOG_PREFIX()} [app_id] Hitta rätt värde: logga in på Fortnox → URL-fältet visar:")
        print(f"{LOG_PREFIX()} [app_id] https://apps2.fortnox.se/app/[APP_ID]/...")
    else:
        print(f"{LOG_PREFIX()} [app_id] Giltig: {cfg['app_id']}")

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
        time.sleep(0.25)
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

def fetch_invoice_details(cfg, doc_nrs):
    """Fetch detail for invoices to get labels AND row-level project codes.
    Returns: (labels_map, projects_map) where projects_map has row-level projects."""
    labels_map = {}
    projects_map = {}
    for i, doc_nr in enumerate(doc_nrs):
        try:
            data = fortnox_get(cfg, f"invoices/{doc_nr}")
            inv_detail = data.get("Invoice", {})
            
            # Labels
            labels = inv_detail.get("Labels", [])
            if labels:
                label_descs = []
                for lb in labels:
                    lb_id = lb.get("Id", "")
                    label_descs.append(str(lb_id))
                labels_map[str(doc_nr)] = label_descs
            
            # Project from rows (if header is empty)
            header_proj = inv_detail.get("Project", "")
            if not header_proj:
                rows = inv_detail.get("InvoiceRows", [])
                row_projects = set()
                for r in rows:
                    rp = r.get("Project", "")
                    if rp:
                        row_projects.add(rp)
                if row_projects:
                    # Use the most common project, or first one
                    projects_map[str(doc_nr)] = sorted(row_projects)[0]
            
            time.sleep(0.15)
            if (i + 1) % 25 == 0:
                print(f"{LOG_PREFIX()} Fetched details for {i+1}/{len(doc_nrs)} invoices...")
        except Exception as e:
            print(f"{LOG_PREFIX()} Warning: could not fetch detail for invoice {doc_nr}: {e}")
    return labels_map, projects_map


def fetch_supplier_invoice_projects(cfg, doc_nrs):
    """Fetch detail for supplier invoices to get project codes.
    Returns: projects_map where projects_map has row-level projects."""
    projects_map = {}
    for i, doc_nr in enumerate(doc_nrs):
        try:
            data = fortnox_get(cfg, f"supplierinvoices/{doc_nr}")
            inv_detail = data.get("SupplierInvoice", {})
            
            # Project from header ELLER rows
            header_proj = inv_detail.get("Project", "")
            if header_proj:
                projects_map[str(doc_nr)] = header_proj
            else:
                rows = inv_detail.get("SupplierInvoiceRows", [])
                row_projects = set()
                for r in rows:
                    rp = r.get("Project", "")
                    if rp:
                        row_projects.add(rp)
                if row_projects:
                    projects_map[str(doc_nr)] = sorted(row_projects)[0]
            
            time.sleep(0.15)
            if (i + 1) % 25 == 0:
                print(f"{LOG_PREFIX()} Fetched details for {i+1}/{len(doc_nrs)} supplier invoices...")
        except Exception as e:
            print(f"{LOG_PREFIX()} Warning: could not fetch detail for supplier invoice {doc_nr}: {e}")
    return projects_map

def sync_invoices(cfg):
    print(f"{LOG_PREFIX()} Fetching customer invoices...")
    invoices = fetch_all_pages(cfg, "invoices", "Invoices")
    print(f"{LOG_PREFIX()} Got {len(invoices)} invoices")
    if not invoices:
        return 0

    # Fetch label definitions
    try:
        label_data = fortnox_get(cfg, "labels")
        label_defs = {lb["Id"]: lb["Description"] for lb in label_data.get("Labels", [])}
        print(f"{LOG_PREFIX()} Label definitions: {label_defs}")
    except Exception:
        label_defs = {}

    # Fetch detail for ALL invoices to get labels + row-level projects
    all_ids = [inv.get("DocumentNumber") for inv in invoices if not inv.get("Cancelled", False)]
    print(f"{LOG_PREFIX()} Fetching details for {len(all_ids)} invoices (labels + projects)...")
    labels_map, row_projects_map = fetch_invoice_details(cfg, all_ids)
    print(f"{LOG_PREFIX()} Found labels on {len(labels_map)} invoices, row-projects on {len(row_projects_map)} invoices")

    sql_parts = [f"DELETE FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}';"]
    for inv in invoices:
        doc_nr = inv.get("DocumentNumber", "")
        customer = inv.get("CustomerName", "")
        inv_date = inv.get("InvoiceDate", None)
        due_date = inv.get("DueDate", None)
        total = inv.get("Total", 0)
        balance = inv.get("Balance", 0)
        is_credit = inv.get("Credit", False)
        cancelled = inv.get("Cancelled", False)
        booked = inv.get("Booked", False)
        project = inv.get("Project", "") or row_projects_map.get(str(doc_nr), "")

        if cancelled:
            status = "cancelled"
        elif booked and balance == 0:
            status = "paid"
        elif due_date and due_date < datetime.now().strftime("%Y-%m-%d"):
            status = "overdue"
        else:
            status = "unpaid"

        # Resolve labels
        raw_labels = labels_map.get(str(doc_nr), [])
        label_str = None
        if raw_labels:
            resolved = [label_defs.get(int(lid), f"id:{lid}") for lid in raw_labels]
            label_str = ", ".join(resolved)

        sql_parts.append(
            f"INSERT INTO fortnox_invoice "
            f"(company_code, fortnox_id, customer_name, invoice_date, due_date, total, balance, status, project_code, is_credit, label) "
            f"VALUES ('{COMPANY_CODE}', {escape_sql(doc_nr)}, {escape_sql(customer)}, "
            f"{escape_sql(inv_date)}, {escape_sql(due_date)}, "
            f"{total}, {balance}, {escape_sql(status)}, {escape_sql(project)}, {str(is_credit).lower()}, {escape_sql(label_str)}) "
            f"ON CONFLICT (company_code, fortnox_id) DO UPDATE SET "
            f"customer_name=EXCLUDED.customer_name, invoice_date=EXCLUDED.invoice_date, "
            f"due_date=EXCLUDED.due_date, total=EXCLUDED.total, balance=EXCLUDED.balance, "
            f"status=EXCLUDED.status, project_code=EXCLUDED.project_code, "
            f"is_credit=EXCLUDED.is_credit, label=EXCLUDED.label, synced_at=NOW();"
        )

    batch_sql = "\n".join(sql_parts)
    psql(batch_sql)
    return len(invoices)

def sync_supplier_invoices(cfg):
    print(f"{LOG_PREFIX()} Fetching supplier invoices...")
    invoices = fetch_all_pages(cfg, "supplierinvoices", "SupplierInvoices")
    print(f"{LOG_PREFIX()} Got {len(invoices)} supplier invoices")
    if not invoices:
        return 0

    # Track which fortnox_ids we see from API — mark others as stale after
    seen_ids = []
    sql_parts = []
    unpaid_doc_nrs = []
    for inv in invoices:
        doc_nr = inv.get("GivenNumber", "") or inv.get("DocumentNumber", "")
        supplier = inv.get("SupplierName", "")
        inv_date = inv.get("InvoiceDate", None)
        due_date = inv.get("DueDate", None)
        total = inv.get("Total", 0)
        balance = inv.get("Balance", 0)
        cancelled = inv.get("Cancelled", False)

        if cancelled:
            status = "cancelled"
        elif balance == 0:
            status = "paid"
        else:
            status = "unpaid"
            unpaid_doc_nrs.append(str(doc_nr))

        seen_ids.append(str(doc_nr))

        # Upsert: update financial data but PRESERVE label and parked (manually set)
        sql_parts.append(
            f"INSERT INTO fortnox_supplier_invoice "
            f"(company_code, fortnox_id, supplier_name, invoice_date, due_date, total, balance, status, parked) "
            f"VALUES ('{COMPANY_CODE}', {escape_sql(doc_nr)}, {escape_sql(supplier)}, "
            f"{escape_sql(inv_date)}, {escape_sql(due_date)}, "
            f"{total}, {balance}, {escape_sql(status)}, false) "
            f"ON CONFLICT (company_code, fortnox_id) DO UPDATE SET "
            f"supplier_name=EXCLUDED.supplier_name, invoice_date=EXCLUDED.invoice_date, "
            f"due_date=EXCLUDED.due_date, total=EXCLUDED.total, balance=EXCLUDED.balance, "
            f"status=EXCLUDED.status, synced_at=NOW();"
        )

    batch_sql = "\n".join(sql_parts)
    psql(batch_sql)
    # Fetch project codes for unpaid invoices
    # Get all unpaid from DB to ensure we cover all
    db_unpaid_raw = psql(f"SELECT fortnox_id FROM fortnox_supplier_invoice WHERE company_code='{COMPANY_CODE}' AND balance > 0 AND (project_code IS NULL OR project_code='')")
    db_unpaid_ids = [line.strip() for line in db_unpaid_raw.split(chr(10)) if line.strip()]
    
    # Combine with newly imported unpaid (balance != 0 from API)
    unpaid_from_api = [str(inv.get("GivenNumber", "") or inv.get("DocumentNumber", "")) for inv in invoices if inv.get("Balance", 0) != 0]
    all_unpaid = list(set(unpaid_from_api + db_unpaid_ids))
    
    projects_map = {}
    if all_unpaid:
        projects_map = fetch_supplier_invoice_projects(cfg, all_unpaid)
        if projects_map:
            print(f"{LOG_PREFIX()} Found projects on {len(projects_map)} supplier invoices")
            for doc_nr, project_code in projects_map.items():
                psql(f"UPDATE fortnox_supplier_invoice SET project_code = {escape_sql(project_code)} WHERE company_code='{COMPANY_CODE}' AND fortnox_id = {escape_sql(doc_nr)};")
    # Remove invoices no longer in Fortnox (deleted/voided)
    if seen_ids:
        id_list = ",".join([escape_sql(i) for i in seen_ids])
        psql(f"DELETE FROM fortnox_supplier_invoice WHERE company_code='{COMPANY_CODE}' AND fortnox_id NOT IN ({id_list});")

    return len(invoices)

LABEL_TAGS = ["#Parkerad", "#Bevakas", "#Tvist"]

def extract_label_from_comments(comments):
    """Extrahera label-tagg från Fortnox Comments-fält."""
    if not comments:
        return None
    for tag in LABEL_TAGS:
        if tag in comments:
            return tag[1:]  # Ta bort # -> "Parkerad"
    return None

def sync_supplier_labels_from_fortnox(cfg):
    """
    Läser #Parkerad/#Bevakas/#Tvist från Fortnox Comments-fält och
    sätter label i vår DB. Körs enbart för fakturor som saknar label.
    Begränsar API-anrop till unlabeled + aktiva fakturor.
    """
    # Hämta alla fortnox_ids utan label i vår DB
    unlabeled = psql(
        f"SELECT fortnox_id FROM fortnox_supplier_invoice "
        f"WHERE company_code='{COMPANY_CODE}' AND (label IS NULL OR label = '') "
        f"AND status != 'cancelled' ORDER BY fortnox_id DESC LIMIT 100;"
    )
    if not unlabeled:
        return 0

    ids = [row.strip() for row in unlabeled.split("\n") if row.strip()]
    if not ids:
        return 0

    print(f"{LOG_PREFIX()} Checking Fortnox Comments for labels on {len(ids)} unlabeled invoices...")
    updated = 0
    for doc_nr in ids:
        try:
            data = fortnox_get(cfg, f"supplierinvoices/{doc_nr}")
            inv = data.get("SupplierInvoice", {})
            comments = inv.get("Comments", "") or ""
            label = extract_label_from_comments(comments)
            if label:
                psql(
                    f"UPDATE fortnox_supplier_invoice SET label = {escape_sql(label)} "
                    f"WHERE company_code='{COMPANY_CODE}' AND fortnox_id = {escape_sql(doc_nr)};"
                )
                print(f"{LOG_PREFIX()} Label '{label}' importerad fran Fortnox kommentar pa faktura {doc_nr}")
                updated += 1
            time.sleep(0.15)  # Respektera rate limit
        except Exception as e:
            print(f"{LOG_PREFIX()} Kunde inte hamta faktura {doc_nr}: {e}")
            continue

    if updated:
        print(f"{LOG_PREFIX()} {updated} fakturor fick label fran Fortnox Comments")
    return updated



def sync_bank_balance(cfg):
    """Hämta verkligt banksaldo från Fortnox PSD2/bankintegration (Handelsbanken)."""
    print(f"{LOG_PREFIX()} Synkar banksaldo via bank/accounts/balance-v1...")
    token = cfg.get("access_token", "")

    try:
        import urllib.request
        req = urllib.request.Request(
            "https://api.fortnox.se/api/bank/accounts/balance-v1",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json"
            }
        )
        resp = urllib.request.urlopen(req, timeout=30)
        accounts = json.loads(resp.read())

        if not accounts:
            print(f"{LOG_PREFIX()} Inga bankkonton hittades")
            return

        # Använd första kontot (Handelsbanken affärskonto)
        acct = accounts[0]
        bank_balance = acct.get("balance", 0)
        balance_date = acct.get("balanceDate", datetime.now().strftime("%Y-%m-%d"))
        bank_name = acct.get("bank", "unknown")
        description = acct.get("description", "")
        iban = acct.get("iban", "")

        # Hämta också bokfört saldo från konto 1930 för jämförelse
        booked_balance = bank_balance  # Default
        try:
            acct_data = fortnox_get(cfg, "accounts/1930")
            booked_balance = acct_data.get("Account", {}).get("BalanceCarriedForward", bank_balance)
        except Exception:
            pass

        psql(f"""INSERT INTO bank_balance (company_code, balance, booked_balance, balance_date, updated_at, updated_by)
            VALUES ('{COMPANY_CODE}', {bank_balance}, {booked_balance}, '{balance_date}', NOW(), 'fortnox_psd2_{bank_name}')
            ON CONFLICT (company_code) DO UPDATE SET
                balance = {bank_balance},
                booked_balance = {booked_balance},
                balance_date = '{balance_date}',
                updated_at = NOW(),
                updated_by = 'fortnox_psd2_{bank_name}'""")
        print(f"{LOG_PREFIX()} Banksaldo ({description} - {bank_name}): {bank_balance:,.2f} kr (bokfört: {booked_balance:,.2f} kr) per {balance_date}")

    except Exception as e:
        print(f"{LOG_PREFIX()} Banksaldo-synk fel: {e}")


def sync_salary_from_vouchers(cfg):
    """Hämta löne- och AGI-data från verifikationsserier F och L (Lön)."""
    print(f"{LOG_PREFIX()} Synkar lönedata från vouchers (serie F+L)...")

    try:
        import time as _time
        monthly = {}  # "2025-11" -> {gross, agi, tax, net, count}

        # Hämta från alla relevanta räkenskapsår och serier
        for fy in [6, 5]:
            for serie in ["F", "L"]:
                try:
                    data = fortnox_get(cfg, f"vouchers/sublist/{serie}", {"financialyear": fy})
                    vouchers = data.get("Vouchers", [])
                except Exception:
                    continue

                if not vouchers:
                    continue

                for v in vouchers:
                    nr = v.get("VoucherNumber")
                    date = v.get("TransactionDate", "")
                    if len(date) < 7:
                        continue
                    ym = date[:7]
                    if ym not in monthly:
                        monthly[ym] = {"gross": 0, "agi": 0, "tax": 0, "net": 0, "count": 0}

                    try:
                        detail = fortnox_get(cfg, f"vouchers/{serie}/{nr}", {"financialyear": fy})
                        rows = detail.get("Voucher", {}).get("VoucherRows", [])
                        for r in rows:
                            acct = r.get("Account", 0)
                            debit = float(r.get("Debit", 0) or 0)
                            credit = float(r.get("Credit", 0) or 0)
                            if 7010 <= acct <= 7090:
                                monthly[ym]["gross"] += debit - credit
                            elif 7510 <= acct <= 7590:
                                monthly[ym]["agi"] += debit - credit
                            elif acct == 2710:
                                monthly[ym]["tax"] += credit - debit
                            elif acct == 1930:
                                monthly[ym]["net"] += credit - debit
                        monthly[ym]["count"] += 1
                        _time.sleep(0.15)
                    except Exception as e:
                        print(f"{LOG_PREFIX()} Kunde inte hämta {serie}-{nr}: {e}")

        # Spara till databas (full replace per månad)
        for ym, d in sorted(monthly.items()):
            total_cost = d["gross"] + d["agi"]
            psql(f"""INSERT INTO fortnox_salary_monthly
                (company_code, year_month, gross_salary, employer_tax, personal_tax, net_payout, total_cost, voucher_count, synced_at)
                VALUES ('{COMPANY_CODE}', '{ym}', {d['gross']:.2f}, {d['agi']:.2f}, {d['tax']:.2f}, {d['net']:.2f}, {total_cost:.2f}, {d['count']}, NOW())
                ON CONFLICT (company_code, year_month) DO UPDATE SET
                    gross_salary = {d['gross']:.2f},
                    employer_tax = {d['agi']:.2f},
                    personal_tax = {d['tax']:.2f},
                    net_payout = {d['net']:.2f},
                    total_cost = {total_cost:.2f},
                    voucher_count = {d['count']},
                    synced_at = NOW()""")
            print(f"{LOG_PREFIX()} {ym}: Brutto {d['gross']:,.0f} kr, AGI {d['agi']:,.0f} kr, Total {total_cost:,.0f} kr")

    except Exception as e:
        print(f"{LOG_PREFIX()} Lönesynk fel: {e}")


def main():
    print(f"\n{'='*60}")
    print(f"{LOG_PREFIX()} Fortnox sync starting for {COMPANY_CODE}")
    print(f"{'='*60}")

    if not os.path.exists(CONFIG_PATH):
        print(f"{LOG_PREFIX()} ERROR: Config not found at {CONFIG_PATH}")
        sys.exit(1)

    cfg = load_config()
    cfg = refresh_token_if_needed(cfg)

    inv_count = sync_invoices(cfg)
    si_count = sync_supplier_invoices(cfg)
    sync_supplier_labels_from_fortnox(cfg)
    sync_bank_balance(cfg)
    sync_salary_from_vouchers(cfg)

    # Summary
    revenue = psql(f"SELECT COALESCE(SUM(total),0) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND NOT is_credit AND status != 'cancelled'")
    costs = psql(f"SELECT COALESCE(SUM(total),0) FROM fortnox_supplier_invoice WHERE company_code='{COMPANY_CODE}' AND status != 'cancelled'")
    unpaid_recv = psql(f"SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND balance > 0 AND status != 'cancelled'")
    unpaid_pay = psql(f"SELECT COALESCE(SUM(balance),0) FROM fortnox_supplier_invoice WHERE company_code='{COMPANY_CODE}' AND balance > 0 AND status != 'cancelled'")
    overdue_real = psql(f"SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND status='overdue' AND (label IS NULL OR label = '')")
    overdue_labeled = psql(f"SELECT COALESCE(SUM(balance),0) FROM fortnox_invoice WHERE company_code='{COMPANY_CODE}' AND status='overdue' AND label IS NOT NULL AND label != ''")

    print(f"\n{LOG_PREFIX()} Sync complete:")
    print(f"  Kundfakturor:           {inv_count}")
    print(f"  Leverantorsfakturor:    {si_count}")
    print(f"  Fakturerad omsattning:  {revenue} SEK")
    print(f"  Leverantorskostnader:   {costs} SEK")
    print(f"  Kundfordringar:         {unpaid_recv} SEK")
    print(f"  Leverantorsskulder:     {unpaid_pay} SEK")
    print(f"  Genuint forfallna:      {overdue_real} SEK")
    print(f"  Forfallna m etikett:    {overdue_labeled} SEK")

if __name__ == "__main__":
    main()
