#!/usr/bin/env python3
import json
import urllib.request
import urllib.error
from urllib.parse import urlencode
from datetime import datetime, timedelta
from collections import defaultdict
import sys

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
    auth_header = f"Bearer {cfg['access_token']}"
    req = urllib.request.Request(url, headers={
        "Authorization": auth_header,
        "Accept": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            return json.loads(res.read())
    except urllib.error.HTTPError as e:
        if e.code == 401 and retry < 2:
            cfg = refresh_token_if_needed(cfg)
            return fortnox_get(cfg, endpoint, retry+1)
        raise
    except Exception as e:
        print(f"Error fetching {endpoint}: {e}", file=sys.stderr)
        raise

cfg = load_config()
vat_accounts = [2610, 2611, 2614, 2640, 2641, 2650]
monthly_data = defaultdict(lambda: defaultdict(float))

print("Fetching voucher list...", file=sys.stderr)
voucher_list = fortnox_get(cfg, "3/vouchers?financialyear=6&limit=100")
total_pages = int(voucher_list.get("@TotalResources", 1))
pages_needed = (total_pages + 99) // 100

print(f"Processing {total_pages} vouchers across {pages_needed} pages", file=sys.stderr)

voucher_count = 0
for page in range(1, pages_needed + 1):
    print(f"Fetching page {page}/{pages_needed}...", file=sys.stderr, end=" ")
    sys.stderr.flush()
    
    vouchers_page = fortnox_get(cfg, f"3/vouchers?financialyear=6&limit=100&page={page}")
    vouchers = vouchers_page.get("Vouchers", [])
    
    if not vouchers:
        print("done (empty)", file=sys.stderr)
        break
    
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
        except Exception as e:
            voucher_url_str = voucher.get("@url", "unknown")
            print(f"Error processing voucher {voucher_url_str}: {e}", file=sys.stderr)
            continue
    
    print("done", file=sys.stderr)

print("\n=== MOMS/VAT per månad (2025-2026) ===\n")
account_names = {
    2610: "Utgående moms 25%",
    2611: "Utgående moms 12%",
    2614: "Utgående moms 6%",
    2640: "Ingående moms",
    2641: "Debiterad ingående moms",
    2650: "Moms på förvärv från utlandet",
}

for account in sorted(vat_accounts):
    if account in monthly_data:
        print(f"Konto {account}: {account_names.get(account, 'Okänd')}")
        months = sorted(monthly_data[account].keys())
        total = 0
        for month in months:
            amount = monthly_data[account][month]
            total += amount
            print(f"  {month}: {amount:>12,.2f} SEK")
        print(f"  Totalt: {total:>12,.2f} SEK")

print(f"\nProcesserad: {voucher_count} verifikationer")
