#!/usr/bin/env python3
"""
next_time_sync.py — Synkroniserar bookedhours från Next Tech v1 API till time_report.
Kör varannan timme via cron. Deduplicerar via next_bookedhours_id.
Uppdaterar approved/attested/invoiced/locked-status på befintliga poster.
"""

import json, subprocess, sys, os
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────────────────────
NEXT_CONFIG = "/opt/rm-infra/next-config.json"
DB_NAME = "rm_central"
DB_USER = "rmadmin"
DB_PASS = "Rm4x7KoncernDB2026stack"
DB_HOST = "localhost"
COMPANY_CODE = "RM"
PAGE_SIZE = 100

# ── Helpers ─────────────────────────────────────────────────────────────
def load_token():
    with open(NEXT_CONFIG) as f:
        cfg = json.load(f)
    return cfg.get("access_token") or cfg.get("bearer_token") or cfg.get("token")

def fetch_next_api(endpoint, token):
    """Fetch via curl (Cloudflare blocks Python urllib)."""
    url = f"https://api.next-tech.com/v1/{endpoint}"
    result = subprocess.run(
        ["curl", "-s", "-H", f"Authorization: Bearer {token}", url],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        print(f"  ERROR curl {url}: {result.stderr[:200]}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"  ERROR parse {url}: {result.stdout[:200]}")
        return None

def get_db():
    import psycopg2
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

# ── Main sync ───────────────────────────────────────────────────────────
def sync():
    token = load_token()
    if not token:
        print("ERROR: no token in config")
        sys.exit(1)

    # Fetch all pages
    all_items = []
    page = 1
    while True:
        data = fetch_next_api(f"bookedhours/?page={page}&size={PAGE_SIZE}", token)
        if not data or "items" not in data:
            print(f"  ERROR fetching page {page}")
            break
        items = data["items"]
        all_items.extend(items)
        if page >= data.get("pages", 1):
            break
        page += 1

    print(f"Fetched {len(all_items)} bookedhours from Next ({page} pages)")

    if not all_items:
        print("Nothing to sync")
        return

    conn = get_db()
    cur = conn.cursor()

    # Get existing next_bookedhours_ids for fast lookup
    cur.execute("SELECT next_bookedhours_id FROM time_report WHERE next_bookedhours_id IS NOT NULL")
    existing_ids = {row[0] for row in cur.fetchall()}
    print(f"Existing Next-imported reports: {len(existing_ids)}")

    inserted = 0
    updated = 0
    skipped = 0

    for item in all_items:
        bh_id = item["id"]
        project_no = item.get("projectno") or ""
        project_code = project_no.split("-")[0] if project_no else None

        if bh_id in existing_ids:
            # Update status fields (approved, attested, invoiced, locked)
            cur.execute("""
                UPDATE time_report
                SET approved = %s, attested = %s, invoiced = %s, locked = %s, updated_at = now()
                WHERE next_bookedhours_id = %s
                  AND (approved IS DISTINCT FROM %s
                    OR attested IS DISTINCT FROM %s
                    OR invoiced IS DISTINCT FROM %s
                    OR locked IS DISTINCT FROM %s)
            """, (
                item.get("approved", False), item.get("attested", False),
                item.get("invoiced", False), item.get("locked", False),
                bh_id,
                item.get("approved", False), item.get("attested", False),
                item.get("invoiced", False), item.get("locked", False),
            ))
            if cur.rowcount > 0:
                updated += 1
            else:
                skipped += 1
            continue

        # Insert new
        try:
            cur.execute("""
                INSERT INTO time_report (
                    company_code, person, next_project_no, project_code, work_date, hours,
                    notes, source, profession_code, cost_unit, price_unit, total_cost, total_revenue,
                    travel_km, start_time, stop_time, break_minutes,
                    approved, attested, invoiced, locked, is_absence,
                    next_bookedhours_id, report_type, created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, 'next', %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, 'internal', %s
                )
            """, (
                COMPANY_CODE, item["fullname"], project_no, project_code,
                item["date"], item["hours"],
                item.get("diarynote"), item.get("professioncode"),
                item.get("costunit"), item.get("priceunit"),
                item.get("totalcost"), item.get("totalrevenue"),
                item.get("travelondutykm"), item.get("starttime"), item.get("stoptime"),
                int(item["breaktime"]) if item.get("breaktime") else None,
                item.get("approved", False), item.get("attested", False),
                item.get("invoiced", False), item.get("locked", False),
                item.get("isabsence", False),
                bh_id, item.get("created") or datetime.utcnow().isoformat()
            ))
            inserted += 1
        except Exception as e:
            print(f"  ERROR insert bh_id={bh_id}: {e}")
            conn.rollback()
            conn = get_db()
            cur = conn.cursor()

    conn.commit()
    cur.close()
    conn.close()

    print(f"Result: {inserted} inserted, {updated} updated, {skipped} unchanged")

if __name__ == "__main__":
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] next_time_sync starting")
    sync()
    print("Done")
