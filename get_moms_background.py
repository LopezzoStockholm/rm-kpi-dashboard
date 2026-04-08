#!/usr/bin/env python3
import json
import urllib.request
import urllib.error
import time
import sys
from collections import defaultdict
from datetime import datetime

cfg = json.load(open("/opt/rm-infra/fortnox-config.json"))
token = cfg["access_token"]

vat_accounts = {2610, 2611, 2614, 2640, 2641, 2650}
result_file = "/tmp/moms_result.json"

# Load existing results if any
try:
    with open(result_file) as f:
        results = json.load(f)
        monthly_totals = defaultdict(float, results.get("monthly", {}))
        start_page = results.get("last_page", 0) + 1
        total_processed = results.get("total_processed", 0)
except:
    monthly_totals = defaultdict(float)
    start_page = 1
    total_processed = 0

ts = datetime.now().strftime("%H:%M:%S")
print("Resuming from page " + str(start_page) + ", " + str(total_processed) + " vouchers processed so far")

# Process all remaining pages
for page in range(start_page, 10):
    url = "https://api.fortnox.se/3/vouchers?financialyear=6&limit=100&page=" + str(page)
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            vouchers = data.get("Vouchers", [])
            ts = datetime.now().strftime("%H:%M:%S")
            print("[" + ts + "] Page " + str(page) + ": " + str(len(vouchers)) + " vouchers")
            
            for idx, v in enumerate(vouchers):
                voucher_url = v.get("@url", "").replace("https://api.fortnox.se/", "")
                trans_date = v.get("TransactionDate", "")
                
                req2 = urllib.request.Request("https://api.fortnox.se/" + voucher_url, 
                                              headers={"Authorization": "Bearer " + token})
                retry_count = 0
                while retry_count < 3:
                    try:
                        with urllib.request.urlopen(req2, timeout=30) as resp2:
                            detail = json.loads(resp2.read().decode("utf-8"))
                            voucher = detail.get("Voucher", {})
                            rows = voucher.get("VoucherRows", [])
                            
                            for row in rows:
                                account = row.get("Account", 0)
                                if account in vat_accounts:
                                    debit = float(row.get("Debit", 0))
                                    credit = float(row.get("Credit", 0))
                                    amount = debit - credit
                                    month = trans_date[:7] if trans_date else "unknown"
                                    monthly_totals[month] += amount
                            
                            total_processed += 1
                            time.sleep(0.15)
                            break
                            
                    except urllib.error.HTTPError as e:
                        if e.code == 429:
                            wait = (2 ** retry_count) * 2
                            print("    429 rate limit, retry " + str(retry_count+1) + "/3, waiting " + str(wait) + "s...")
                            time.sleep(wait)
                            retry_count += 1
                        else:
                            print("    HTTP " + str(e.code) + " voucher " + str(idx+1))
                            break
                            
            # Save progress
            with open(result_file, "w") as f:
                json.dump({
                    "monthly": dict(monthly_totals),
                    "last_page": page,
                    "total_processed": total_processed
                }, f, indent=2)
            
            time.sleep(3)  # 3s between pages
            
    except Exception as e:
        print("Error page " + str(page) + ": " + str(e))
        time.sleep(5)

# Final output
print("="*50)
print("Total processed: " + str(total_processed) + " vouchers")
print("="*50)
print("\nMoms per month (SEK):")
print("-" * 50)
for month in sorted(monthly_totals.keys()):
    amount = monthly_totals[month]
    sign = "+" if amount >= 0 else ""
    print(month + ":  " + sign + "{:>15,.2f}".format(amount))
    
# Save final results
with open(result_file, "w") as f:
    json.dump({
        "monthly": dict(monthly_totals),
        "total_processed": total_processed,
        "completed": True
    }, f, indent=2)
