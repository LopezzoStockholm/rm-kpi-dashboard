#!/usr/bin/env python3
"""
RM Label Sync — Fortnox Comments -> Dashboard label.
Kör var 5:e minut via cron. Lätt och snabbt.
Kontrollerar olabelade, obetalda fakturor (max 50 per typ per körning).
Synkar BÅDE leverantörs- och kundfakturor.
"""
import json, subprocess, time, sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from datetime import datetime

CONFIG_PATH = "/opt/rm-infra/fortnox-config.json"
COMPANY_CODE = "RM"
LABEL_TAGS = ["#Parkerad", "#Bevakas", "#Tvist"]

def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {msg}", flush=True)

def psql(sql):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", "rm_central", "-t", "-A", "-c", sql],
        capture_output=True, text=True
    )
    return r.stdout.strip()

def fortnox_get(token, endpoint):
    req = Request(
        f"https://api.fortnox.se/3/{endpoint}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}
    )
    try:
        resp = urlopen(req, timeout=10)
        return json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            time.sleep(6)
            return fortnox_get(token, endpoint)
        raise

def extract_label(comments):
    if not comments:
        return None
    for tag in LABEL_TAGS:
        if tag in comments:
            return tag[1:]  # #Parkerad -> Parkerad
    return None

def sync_supplier_invoices(token):
    """Synka labels från Fortnox Comments till DB för leverantörsfakturor."""
    result = psql(
        f"SELECT fortnox_id FROM fortnox_supplier_invoice "
        f"WHERE company_code='{COMPANY_CODE}' AND (label IS NULL OR label='') "
        f"AND status='unpaid' ORDER BY due_date ASC LIMIT 50;"
    )
    if not result:
        return 0

    ids = [r.strip() for r in result.split("\n") if r.strip()]
    updated = 0

    for doc_nr in ids:
        try:
            data = fortnox_get(token, f"supplierinvoices/{doc_nr}")
            comments = data.get("SupplierInvoice", {}).get("Comments", "") or ""
            label = extract_label(comments)
            if label:
                psql(
                    f"UPDATE fortnox_supplier_invoice SET label='{label}' "
                    f"WHERE company_code='{COMPANY_CODE}' AND fortnox_id='{doc_nr}';"
                )
                log(f"LF {doc_nr} -> {label} (från Fortnox Comments)")
                updated += 1
            time.sleep(0.1)
        except Exception as e:
            log(f"Fel vid LF {doc_nr}: {e}")
            continue

    return updated

def sync_customer_invoices(token):
    """Synka labels från Fortnox Comments till DB för kundfakturor."""
    result = psql(
        f"SELECT fortnox_id FROM fortnox_invoice "
        f"WHERE company_code='{COMPANY_CODE}' AND (label IS NULL OR label='') "
        f"AND balance > 0 ORDER BY due_date ASC LIMIT 50;"
    )
    if not result:
        return 0

    ids = [r.strip() for r in result.split("\n") if r.strip()]
    updated = 0

    for doc_nr in ids:
        try:
            data = fortnox_get(token, f"invoices/{doc_nr}")
            comments = data.get("Invoice", {}).get("Comments", "") or ""
            label = extract_label(comments)
            if label:
                psql(
                    f"UPDATE fortnox_invoice SET label='{label}' "
                    f"WHERE company_code='{COMPANY_CODE}' AND fortnox_id='{doc_nr}';"
                )
                log(f"KF {doc_nr} -> {label} (från Fortnox Comments)")
                updated += 1
            time.sleep(0.1)
        except Exception as e:
            log(f"Fel vid KF {doc_nr}: {e}")
            continue

    return updated

def main():
    try:
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        token = cfg.get("access_token", "")
    except Exception as e:
        log(f"Kunde inte läsa config: {e}")
        sys.exit(1)

    updated_si = sync_supplier_invoices(token)
    updated_ci = sync_customer_invoices(token)
    total = updated_si + updated_ci

    if total:
        log(f"Dashboard regenererad efter {total} label-uppdateringar (LF:{updated_si} KF:{updated_ci})")

if __name__ == "__main__":
    main()
