#!/usr/bin/env python3
"""
ccc_snapshot.py — Daglig CCC-tidsserie.
Kors via cron 06:30 (efter Fortnox-synk 06:00).
Beraknar DIO/DSO/DPO/CCC fran Fortnox-data och sparar i ccc_snapshot.
"""

import logging
from datetime import date
from rm_data import query_dicts, execute, query_one

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("ccc_snapshot")

ORG_ID = "rmef"


def calc_dso() -> tuple:
    """DSO: dagar fran kundfaktura till betalning (senaste 90d)."""
    rows = query_dicts("""
        SELECT
            AVG(fp.payment_date - fi.invoice_date) as avg_dso,
            COUNT(*) as n
        FROM fortnox_invoice fi
        JOIN fortnox_payment fp ON fi.fortnox_id = fp.fortnox_id
        WHERE fi.invoice_date > NOW() - INTERVAL '90 days'
          AND fp.payment_date IS NOT NULL
          AND fp.payment_date > fi.invoice_date
    """)
    if rows and rows[0]["avg_dso"] is not None:
        return float(rows[0]["avg_dso"]), int(rows[0]["n"])
    # Fallback: alder pa obetalda
    rows2 = query_dicts("""
        SELECT AVG(CURRENT_DATE - fi.invoice_date) as avg_age, COUNT(*) as cnt
        FROM fortnox_invoice fi
        WHERE fi.invoice_date > NOW() - INTERVAL '90 days' AND fi.balance > 0
    """)
    if rows2 and rows2[0]["avg_age"] is not None:
        return float(rows2[0]["avg_age"]), int(rows2[0]["cnt"])
    return 30.0, 0


def calc_dpo() -> tuple:
    """DPO: dagar vi haller lev.fakturor innan betalning."""
    rows = query_dicts("""
        SELECT
            AVG(fsi.due_date - fsi.invoice_date) as avg_dpo,
            COUNT(*) as n
        FROM fortnox_supplier_invoice fsi
        WHERE fsi.invoice_date > NOW() - INTERVAL '90 days'
          AND fsi.due_date IS NOT NULL
          AND fsi.due_date > fsi.invoice_date
          AND fsi.balance = 0
    """)
    if rows and rows[0]["avg_dpo"] is not None:
        return float(rows[0]["avg_dpo"]), int(rows[0]["n"])
    rows2 = query_dicts("""
        SELECT AVG(fsi.due_date - fsi.invoice_date) as avg_terms, COUNT(*) as cnt
        FROM fortnox_supplier_invoice fsi
        WHERE fsi.invoice_date > NOW() - INTERVAL '90 days' AND fsi.due_date IS NOT NULL
    """)
    if rows2 and rows2[0]["avg_terms"] is not None:
        return float(rows2[0]["avg_terms"]), int(rows2[0]["cnt"])
    return 30.0, 0


def calc_dio() -> tuple:
    """DIO: tid fran lev.kostnad till delfakturering per projekt."""
    rows = query_dicts("""
        SELECT
            AVG(fi_min.first_inv_date - fsi_min.first_cost_date) as avg_dio,
            COUNT(*) as n
        FROM (
            SELECT project_code, MIN(invoice_date) as first_cost_date
            FROM fortnox_supplier_invoice
            WHERE project_code IS NOT NULL AND project_code != ''
                  AND invoice_date > NOW() - INTERVAL '180 days'
            GROUP BY project_code
        ) fsi_min
        JOIN (
            SELECT project_code, MIN(invoice_date) as first_inv_date
            FROM fortnox_invoice
            WHERE project_code IS NOT NULL AND project_code != ''
                  AND invoice_date > NOW() - INTERVAL '180 days'
            GROUP BY project_code
        ) fi_min ON fsi_min.project_code = fi_min.project_code
        WHERE fi_min.first_inv_date > fsi_min.first_cost_date
    """)
    if rows and rows[0]["avg_dio"] is not None:
        return float(rows[0]["avg_dio"]), int(rows[0]["n"])
    return 30.0, 0


def calc_revenue() -> float:
    val = query_one("""
        SELECT COALESCE(SUM(total), 0) FROM fortnox_invoice
        WHERE invoice_date > NOW() - INTERVAL '365 days'
    """)
    return float(val or 0)


def run():
    today = date.today()
    log.info(f"CCC snapshot for {today}")

    dso, n_dso = calc_dso()
    dpo, n_dpo = calc_dpo()
    dio, n_dio = calc_dio()
    ccc = dio + dso - dpo
    rev = calc_revenue()
    wc = (ccc / 365) * rev if rev > 0 else 0
    sample_total = n_dso + n_dpo + n_dio

    log.info(f"DIO={dio:.1f} (n={n_dio}) DSO={dso:.1f} (n={n_dso}) DPO={dpo:.1f} (n={n_dpo}) CCC={ccc:.1f} WC={wc:.0f}")

    execute(
        """INSERT INTO ccc_snapshot (company_code, project_code, snapshot_date, dio_days, dso_days, dpo_days, ccc_days, working_capital, sample_size)
           VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (company_code, project_code, snapshot_date) DO UPDATE SET
               dio_days = EXCLUDED.dio_days,
               dso_days = EXCLUDED.dso_days,
               dpo_days = EXCLUDED.dpo_days,
               ccc_days = EXCLUDED.ccc_days,
               working_capital = EXCLUDED.working_capital,
               sample_size = EXCLUDED.sample_size""",
        (ORG_ID, today, round(dio, 2), round(dso, 2), round(dpo, 2), round(ccc, 2), round(wc, 2), sample_total)
    )
    log.info("Snapshot saved")


if __name__ == "__main__":
    run()
