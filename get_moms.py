#!/usr/bin/env python3
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode
from datetime import datetime, timedelta
from collections import defaultdict
import sys
import time

def load_config(path="/opt/rm-infra/fortnox-config.json"):
    with open(path) as f:
        return json.load(f)

def refresh_token_if_needed(cfg):
    expires = datetime.fromisoformat(cfg.get("token_expires", "2000-01-01T00:00:00"))
    if datetime.now() < expires - timedelta(minutes=5):
        return cfg
    data = urlencode({
        "grant_type": "refresh_token",
        "refresh_token": cfg["refresh_token"],
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
    }).encode()
    req = urllib.request.Request("https://apps.fortnox.se/oauth-v1/token", data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            token_data = json.loads(res.read())
            cfg["access_token"] = token_data["access_token"]
            cfg["token_expires"] = datetime.now().isoformat() + "Z"
            if "refresh_token" in token_data:
                cfg["refresh_token"] = token_data["refresh_token"]
    except Exception as e:
        print(f"Token refresh failed: {e}", file=sys.stderr)
    return cfg

def fortnox_get(cfg, endpoint, retry=0):
    cfg = refresh_token_if_needed(cfg)
    url = f"https://api.fortnox.se/{endpoint}"
    auth = f"Bearer {cfg['access_token']}"
    req = urllib.request.Request(url, headers={
        "Authorization": auth,
        "Accept": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return json.loads(res.read())
    except urllib.error.HTTPError as e:
        if e.code == 429 and retry < 3:
            wait = 2 ** (retry + 1)
            time.sleep(wait)
            return fortnox_get(cfg, endpoint, retry+1)
        if e.code == 401 and retry < 1:
            cfg = refresh_token_if_needed(cfg)
            return fortnox_get(cfg, endpoint, retry+1)
        raise

cfg = load_config()
vat_accounts = set([2610, 2611, 2614, 2640, 2641, 2650])
monthly_data = defaultdict(lambda: defaultdict(float))

print("Fetching voucher list...", file=sys.stderr)
page_data = fortnox_get(cfg, "3/vouchers?financialyear=6&limit=100&page=1")
meta = page_data.get("MetaInformation", {})
total_pages = int(meta.get("@TotalPages", 1))
total_vouchers = int(meta.get("@TotalResources", 0))

print(f"Processing {total_vouchers} vouchers across {total_pages} pages", file=sys.stderr)

voucher_count = 0
max_vouchers = 809

for page in range(1, total_pages + 1):
    page_start = time.time()
    print(f"Page {page}/{total_pages}...", file=sys.stderr, end=" ")
    sys.stderr.flush()
    
    if page == 1:
        vouchers = page_data.get("Vouchers", [])
    else:
        page_data = fortnox_get(cfg, f"3/vouchers?financialyear=6&limit=100&page={page}")
        vouchers = page_data.get("Vouchers", [])
    
    if not vouchers:
        print("(empty)", file=sys.stderr)
        break
    
    processed = 0
    for voucher in vouchers:
        voucher_url = voucher.get("@url", "")
        if not voucher_url:
            continue
        
        endpoint = voucher_url.replace("https://api.fortnox.se/", "")
        try:
            full_data = fortnox_get(cfg, endpoint)
            v = full_data.get("Voucher", {})
            transaction_date = v.get("TransactionDate")
            rows = v.get("VoucherRows", [])
            
            for row in rows:
                account = row.get("Account")
                if account in vat_accounts and transaction_date:
                    debit = float(row.get("Debit", 0))
                    credit = float(row.get("Credit", 0))
                    amount = debit - credit
                    month_key = transaction_date[:7]
                    monthly_data[account][month_key] += amount
            
            voucher_count += 1
            processed += 1
        except Exception as e:
            pass
        
        time.sleep(0.05)
    
    elapsed = time.time() - page_start
    print(f"{processed} processed ({elapsed:.1f}s)", file=sys.stderr)
    time.sleep(2)

print("\n=== MOMS/VAT per månad (2025-2026) ===\n")
account_names = {
    2610: "Utgående moms 25%",
    2611: "Utgående moms 12%",
    2614: "Utgående moms 6%",
    2640: "Ingående moms",
    2641: "Debiterad ingående moms",
    2650: "Moms på förvärv från utlandet",
}

grand_total = 0
for account in sorted(vat_accounts):
    if account in monthly_data:
        print(f"Konto {account}: {account_names.get(account, 'Okänd')}")
        months = sorted(monthly_data[account].keys())
        total = 0
        for month in months:
            amount = monthly_data[account][month]
            total += amount
            print(f"  {month}: {amount:>15,.2f} SEK")
        grand_total += total
        print(f"  TOTALT: {total:>14,.2f} SEK\n")

print(f"\nNetto moms alla konton: {grand_total:,.2f} SEK")
print(f"Verifikationer processerade: {voucher_count}")
