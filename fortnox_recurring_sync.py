#!/usr/bin/env python3
"""
fortnox_recurring_sync.py — Hämtar återkommande kostnader från Fortnox bokföring

Analyserar verifikationer/fakturor per bokföringskonto de senaste 6 månaderna.
Identifierar återkommande poster och populerar recurring_cost-tabellen automatiskt.

Körs veckovis via cron (söndag 06:00).

Datakälla: fortnox_supplier_invoice (leverantörsfakturor) grupperade per leverantör.
Fallback: Fortnox API vouchers endpoint (om tillgänglig).

Logik:
  1. Hämta alla leverantörsfakturor senaste 6 mån
  2. Gruppera per leverantör
  3. Om en leverantör har fakturor >= 3 av 6 månader → troligen recurring
  4. Beräkna snitt per månad
  5. Upsert till recurring_cost-tabellen
"""

import json
import subprocess
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from collections import defaultdict
from statistics import mean

LOG_PREFIX = "[recurring_sync]"
CENTRAL_DB = "rm_central"
DB_USER = "rmadmin"

# Minsta antal månader en leverantör måste förekomma för att räknas som recurring
MIN_MONTHS = 3
LOOKBACK_MONTHS = 6
MIN_MONTHLY_AMOUNT = 500  # Ignorera småposter under 500 kr/månad


def log(msg):
    print(f"{LOG_PREFIX} {datetime.now().isoformat()} {msg}")


def psql(query):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", DB_USER, "-d", CENTRAL_DB,
         "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    return r.stdout.strip()


def psql_rows(query):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", DB_USER, "-d", CENTRAL_DB,
         "--csv", "-c", query],
        capture_output=True, text=True
    )
    output = r.stdout.strip()
    if not output or "\n" not in output:
        return []
    import csv
    from io import StringIO
    return list(csv.DictReader(StringIO(output)))


def categorize_supplier(supplier_name):
    """Kategorisera leverantör baserat på namn."""
    name = supplier_name.lower()

    # Lokalkostnader
    if any(w in name for w in ["fastighets", "hyres", "lokal", "kontor", "property"]):
        return "lokal"

    # Fordon
    if any(w in name for w in ["lease", "leasing", "bil", "auto", "volvo finans",
                                 "toyota", "ford", "volkswagen", "bmw", "mercedes",
                                 "scan", "drivmedel", "circle k", "okq8", "preem",
                                 "st1", "shell", "ingo"]):
        return "fordon"

    # Försäkringar
    if any(w in name for w in ["försäkring", "insurance", "if ", "trygg", "folksam",
                                 "gjensidige", "länsförsäkring", "zurich"]):
        return "forsakring"

    # IT / Telefoni
    if any(w in name for w in ["telia", "telenor", "tre ", "comviq", "hallon",
                                 "microsoft", "google", "amazon web", "digital ocean",
                                 "github", "adobe", "fortnox", "license", "licens",
                                 "it-", "software", "saas"]):
        return "it_telefoni"

    # Revision / Redovisning
    if any(w in name for w in ["revision", "redovisning", "bokför", "audit",
                                 "pwc", "deloitte", "kpmg", "ernst", "grant thornton",
                                 "mazars", "bdo"]):
        return "revision_redovisning"

    # Arbetsmarknad / Pension
    if any(w in name for w in ["pension", "fora ", "collectum", "afa ",
                                 "arbetsgivar", "svenskt näring"]):
        return "personal"

    return "ovrigt"


def analyze_recurring():
    """Analysera leverantörsfakturor och identifiera recurring costs."""
    lookback_date = date.today() - timedelta(days=LOOKBACK_MONTHS * 31)

    rows = psql_rows(f"""
        SELECT
            supplier_name,
            to_char(invoice_date, 'YYYY-MM') as month,
            SUM(total) as month_total,
            COUNT(*) as invoice_count
        FROM fortnox_supplier_invoice
        WHERE company_code = 'RM'
          AND invoice_date >= '{lookback_date}'
          AND status != 'cancelled'
        GROUP BY supplier_name, to_char(invoice_date, 'YYYY-MM')
        ORDER BY supplier_name, month
    """)

    if not rows:
        log("Inga leverantörsfakturor hittades")
        return []

    # Gruppera per leverantör
    suppliers = defaultdict(list)
    for r in rows:
        suppliers[r["supplier_name"]].append({
            "month": r["month"],
            "total": float(r["month_total"]),
            "count": int(r["invoice_count"])
        })

    recurring = []
    for supplier, months in suppliers.items():
        num_months = len(months)

        if num_months < MIN_MONTHS:
            continue

        avg_monthly = mean(m["total"] for m in months)

        if avg_monthly < MIN_MONTHLY_AMOUNT:
            continue

        # Beräkna stddev för att bedöma regelbundenhet
        amounts = [m["total"] for m in months]
        if len(amounts) > 1:
            avg = mean(amounts)
            variance = sum((x - avg) ** 2 for x in amounts) / len(amounts)
            stddev = variance ** 0.5
            cv = stddev / avg if avg > 0 else 0  # Coefficient of variation
        else:
            cv = 0

        category = categorize_supplier(supplier)

        # Bestäm betaldag (vanligaste dagen)
        # Approximera: leverantörsfakturor betalas typiskt 30 dagar efter fakturadatum
        # = runt den 1:a varje månad
        pay_day = 1

        recurring.append({
            "supplier": supplier,
            "monthly_amount": round(avg_monthly, 2),
            "months_seen": num_months,
            "cv": round(cv, 2),
            "category": category,
            "pay_day": pay_day,
        })

    # Bestäm is_fixed baserat på kategori
    FIXED_CATEGORIES = {"lokal", "fordon", "forsakring", "it_telefoni", "personal",
                        "revision_redovisning", "energi", "ovrigt_fast"}
    for item in recurring:
        item["is_fixed"] = item["category"] in FIXED_CATEGORIES

    # Sortera på belopp, störst först
    recurring.sort(key=lambda x: x["monthly_amount"], reverse=True)
    return recurring


def upsert_recurring_costs(items):
    """Uppdatera recurring_cost-tabellen med identifierade poster."""
    if not items:
        log("Inga recurring costs att uppdatera")
        return

    # Markera alla befintliga auto-detekterade som inaktiva först
    # (manuellt tillagda med source != 'auto' berörs inte)
    psql("""
        UPDATE recurring_cost
        SET active = false
        WHERE company_code = 'RM'
          AND source = 'auto'
    """)

    for item in items:
        # Upsert per leverantör
        supplier_escaped = item["supplier"].replace("'", "''")
        psql(f"""
            INSERT INTO recurring_cost
                (company_code, description, monthly_amount, pay_day, category, active, source, months_seen, cv, is_fixed)
            VALUES
                ('RM', '{supplier_escaped}', {item['monthly_amount']}, {item['pay_day']},
                 '{item['category']}', true, 'auto', {item['months_seen']}, {item['cv']}, {item['is_fixed']})
            ON CONFLICT (company_code, description)
            DO UPDATE SET
                monthly_amount = EXCLUDED.monthly_amount,
                pay_day = EXCLUDED.pay_day,
                category = EXCLUDED.category,
                active = true,
                months_seen = EXCLUDED.months_seen,
                cv = EXCLUDED.cv,
                updated_at = NOW()
        """)

    log(f"Uppdaterade {len(items)} recurring costs")


def ensure_schema():
    """Säkerställ att tabellen har rätt kolumner."""
    # Lägg till source, months_seen, cv, updated_at om de saknas
    for col, typ, default in [
        ("source", "VARCHAR(20)", "'manual'"),
        ("months_seen", "INTEGER", "0"),
        ("is_fixed", "BOOLEAN", "false"),
        ("cv", "NUMERIC(5,2)", "0"),
        ("updated_at", "TIMESTAMP", "NOW()"),
    ]:
        psql(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'recurring_cost' AND column_name = '{col}'
                ) THEN
                    ALTER TABLE recurring_cost ADD COLUMN {col} {typ} DEFAULT {default};
                END IF;
            END $$;
        """)

    # Skapa unik constraint om den saknas
    psql("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'recurring_cost_company_desc_uq'
            ) THEN
                ALTER TABLE recurring_cost
                ADD CONSTRAINT recurring_cost_company_desc_uq
                UNIQUE (company_code, description);
            END IF;
        END $$;
    """)


def main():
    log("Starting recurring cost analysis...")

    ensure_schema()

    items = analyze_recurring()

    log(f"Identifierade {len(items)} recurring costs:")
    total = 0
    for item in items:
        log(f"  {item['supplier']}: {item['monthly_amount']:,.0f} kr/mån "
            f"({item['months_seen']} mån, CV={item['cv']}, {item['category']})")
        total += item["monthly_amount"]
    log(f"Total recurring: {total:,.0f} kr/mån")

    upsert_recurring_costs(items)

    log("Done.")


if __name__ == "__main__":
    main()
