"""
Simuleringsmodell: Cash Conversion Cycle + Power of One.
Läser Fortnox-data (fakturor, leverantörsfakturor, betalningar) och beräknar CCC.
Power of One: 5 spakar som visar effekt av +/- 1 enhets förändring.
"""

import random
from rm_data import query_dicts
from simulation_engine import (
    SimModel, ParamDef, SimResult, MCResult,
    register_model, build_histogram, percentile, now_iso,
)


class CCCModel(SimModel):
    name = "ccc"
    display_name = "Cash Conversion Cycle"
    description = "CCC + Power of One — kassaflödespåverkan av pris, volym, COGS, DSO och DPO"
    category = "kassaflöde"

    # ------------------------------------------------------------------
    # Baseline-beräkning från Fortnox-data
    # ------------------------------------------------------------------

    def _calc_dso(self, company_code: str) -> dict:
        """DSO: dagar från kundfaktura till betalning (senaste 90d).
        Primär: fortnox_payment JOIN fortnox_invoice via fortnox_id.
        Fallback: ålder på obetalda fakturor (balance > 0)."""
        rows = query_dicts("""
            SELECT
                AVG(fp.payment_date - fi.invoice_date) as avg_dso,
                COUNT(*) as sample_size
            FROM fortnox_invoice fi
            JOIN fortnox_payment fp ON fi.fortnox_id = fp.fortnox_id
            WHERE fi.invoice_date > NOW() - INTERVAL '90 days'
              AND fp.payment_date IS NOT NULL
              AND fp.payment_date > fi.invoice_date
              AND fi.company_code = %s
        """, (company_code,))
        if rows and rows[0]["avg_dso"] is not None:
            return {"dso_days": float(rows[0]["avg_dso"]), "sample": int(rows[0]["sample_size"])}
        # Fallback: genomsnittlig ålder på obetalda fakturor
        rows2 = query_dicts("""
            SELECT AVG(CURRENT_DATE - fi.invoice_date) as avg_age, COUNT(*) as cnt
            FROM fortnox_invoice fi
            WHERE fi.invoice_date > NOW() - INTERVAL '90 days'
              AND fi.balance > 0
              AND fi.company_code = %s
        """, (company_code,))
        if rows2 and rows2[0]["avg_age"] is not None:
            return {"dso_days": float(rows2[0]["avg_age"]), "sample": int(rows2[0]["cnt"])}
        return {"dso_days": 30.0, "sample": 0}

    def _calc_dpo(self, company_code: str) -> dict:
        """DPO: dagar vi håller leverantörsfakturor.
        Primär: betalda fakturor (balance=0) → tid till due_date.
        Fallback: genomsnittlig betalningstid (due_date - invoice_date)."""
        rows = query_dicts("""
            SELECT
                AVG(fsi.due_date - fsi.invoice_date) as avg_dpo,
                COUNT(*) as sample_size
            FROM fortnox_supplier_invoice fsi
            WHERE fsi.invoice_date > NOW() - INTERVAL '90 days'
              AND fsi.due_date IS NOT NULL
              AND fsi.due_date > fsi.invoice_date
              AND fsi.balance = 0
              AND fsi.company_code = %s
        """, (company_code,))
        if rows and rows[0]["avg_dpo"] is not None:
            return {"dpo_days": float(rows[0]["avg_dpo"]), "sample": int(rows[0]["sample_size"])}
        # Fallback: alla leverantörsfakturor, due_date som proxy
        rows2 = query_dicts("""
            SELECT AVG(fsi.due_date - fsi.invoice_date) as avg_terms, COUNT(*) as cnt
            FROM fortnox_supplier_invoice fsi
            WHERE fsi.invoice_date > NOW() - INTERVAL '90 days'
              AND fsi.due_date IS NOT NULL
              AND fsi.company_code = %s
        """, (company_code,))
        if rows2 and rows2[0]["avg_terms"] is not None:
            return {"dpo_days": float(rows2[0]["avg_terms"]), "sample": int(rows2[0]["cnt"])}
        return {"dpo_days": 30.0, "sample": 0}

    def _calc_dio(self, company_code: str) -> dict:
        """DIO: tid från leverantörskostnad till delfakturering (per projekt)."""
        rows = query_dicts("""
            SELECT
                AVG(fi_min.first_inv_date - fsi_min.first_cost_date) as avg_dio,
                COUNT(*) as sample_size
            FROM (
                SELECT project_code, MIN(invoice_date) as first_cost_date
                FROM fortnox_supplier_invoice
                WHERE project_code IS NOT NULL AND project_code != ''
                      AND invoice_date > NOW() - INTERVAL '180 days'
                      AND company_code = %s
                GROUP BY project_code
            ) fsi_min
            JOIN (
                SELECT project_code, MIN(invoice_date) as first_inv_date
                FROM fortnox_invoice
                WHERE project_code IS NOT NULL AND project_code != ''
                      AND invoice_date > NOW() - INTERVAL '180 days'
                      AND company_code = %s
                Group BY project_code
            ) fi_min ON fsi_min.project_code = fi_min.project_code
            WHERE fi_min.first_inv_date > fsi_min.first_cost_date
        """, (company_code, company_code))
        if rows and rows[0]["avg_dio"] is not None:
            return {"dio_days": float(rows[0]["avg_dio"]), "sample": int(rows[0]["sample_size"])}
        return {"dio_days": 30.0, "sample": 0}

    def _revenue_and_costs(self, company_code: str) -> dict:
        """Årsomsättning och kostnader från Fortnox."""
        rows = query_dicts("""
            SELECT
                COALESCE(SUM(fi.total), 0) as annual_revenue,
                COALESCE((SELECT SUM(total) FROM fortnox_supplier_invoice
                          WHERE invoice_date > NOW() - INTERVAL '365 days'
                          AND company_code = %s), 0) as annual_costs
            FROM fortnox_invoice fi
            WHERE fi.invoice_date > NOW() - INTERVAL '365 days'
              AND fi.company_code = %s
        """, (company_code, company_code))
        if rows:
            rev = float(rows[0]["annual_revenue"] or 0)
            costs = float(rows[0]["annual_costs"] or 0)
            return {"annual_revenue": rev, "annual_costs": costs}
        return {"annual_revenue": 0, "annual_costs": 0}

    # ------------------------------------------------------------------
    # Interface implementation
    # ------------------------------------------------------------------

    def fetch_baseline(self, company_code: str = "RM") -> dict:
        dso = self._calc_dso(company_code)
        dpo = self._calc_dpo(company_code)
        dio = self._calc_dio(company_code)
        rev = self._revenue_and_costs(company_code)

        dio_days = dio["dio_days"]
        dso_days = dso["dso_days"]
        dpo_days = dpo["dpo_days"]
        ccc_days = dio_days + dso_days - dpo_days
        annual_rev = rev["annual_revenue"]
        annual_costs = rev["annual_costs"]
        working_capital = (ccc_days / 365) * annual_rev if annual_rev > 0 else 0
        ebitda = annual_rev - annual_costs  # Förenklat

        return {
            "dio_days": round(dio_days, 1),
            "dso_days": round(dso_days, 1),
            "dpo_days": round(dpo_days, 1),
            "ccc_days": round(ccc_days, 1),
            "annual_revenue": round(annual_rev),
            "annual_costs": round(annual_costs),
            "working_capital_tied": round(working_capital),
            "ebitda": round(ebitda),
            "samples": {
                "dio": dio["sample"],
                "dso": dso["sample"],
                "dpo": dpo["sample"],
            }
        }

    def parameters(self, company_code: str = "RM") -> list:
        baseline = self.fetch_baseline(company_code)
        return [
            ParamDef("price_delta_pct", "Prisförändring", "pct", 0,
                     -10, 10, 0.5, "%",
                     "Procentuell förändring av snittfaktura"),
            ParamDef("volume_delta", "Volymförändring", "int", 0,
                     -5, 10, 1, "projekt",
                     "Antal extra (eller färre) projekt per år"),
            ParamDef("cogs_delta_pct", "Kostnadsförändring", "pct", 0,
                     -10, 10, 0.5, "%",
                     "Procentuell förändring av materialkostnad"),
            ParamDef("dso_delta_days", "DSO-förändring", "int", 0,
                     -30, 30, 1, "dagar",
                     f"Dagar snabbare (+) eller långsammare (-) kundinbetalning. Nuvarande: {baseline['dso_days']}d"),
            ParamDef("dpo_delta_days", "DPO-förändring", "int", 0,
                     -30, 30, 1, "dagar",
                     f"Dagar längre (+) eller kortare (-) leverantörskredit. Nuvarande: {baseline['dpo_days']}d"),
        ]

    def compute(self, inputs: dict, company_code: str = "RM") -> SimResult:
        baseline = self.fetch_baseline(company_code)

        # Hämta inputs med defaults
        price_pct = float(inputs.get("price_delta_pct", 0))
        volume_delta = int(inputs.get("volume_delta", 0))
        cogs_pct = float(inputs.get("cogs_delta_pct", 0))
        dso_delta = float(inputs.get("dso_delta_days", 0))
        dpo_delta = float(inputs.get("dpo_delta_days", 0))

        # Beräkna adjusted values
        rev = baseline["annual_revenue"]
        costs = baseline["annual_costs"]

        # Uppskatta antal projekt (grova antaganden om data saknas)
        project_count_rows = query_dicts("""
            SELECT COUNT(DISTINCT project) as cnt FROM fortnox_invoice
            WHERE invoice_date > NOW() - INTERVAL '365 days' AND project IS NOT NULL AND project != ''
              AND company_code = %s
        """, (company_code,))
        n_projects = int(project_count_rows[0]["cnt"]) if project_count_rows and project_count_rows[0]["cnt"] else 10
        rev_per_project = rev / n_projects if n_projects > 0 else 0
        cost_per_project = costs / n_projects if n_projects > 0 else 0

        # Adjusted
        adj_rev = rev * (1 + price_pct / 100) + volume_delta * rev_per_project
        adj_costs = costs * (1 + cogs_pct / 100) + volume_delta * cost_per_project
        adj_ebitda = adj_rev - adj_costs

        adj_dso = baseline["dso_days"] - dso_delta  # Minus = snabbare
        adj_dpo = baseline["dpo_days"] + dpo_delta  # Plus = längre kredit
        adj_ccc = baseline["dio_days"] + adj_dso - adj_dpo
        adj_wc = (adj_ccc / 365) * adj_rev if adj_rev > 0 else 0

        # Deltas
        ccc_change = adj_ccc - baseline["ccc_days"]
        wc_freed = baseline["working_capital_tied"] - adj_wc
        ebitda_change = adj_ebitda - baseline["ebitda"]

        # Warnings
        warnings = []
        for key, label in [("dio", "DIO"), ("dso", "DSO"), ("dpo", "DPO")]:
            n = baseline["samples"][key]
            if n == 0:
                warnings.append(f"{label} använder default-värde (30d) — ingen Fortnox-data hittades")
            elif n < 5:
                warnings.append(f"{label} baseras på {n} datapunkter — låg konfidensgrad")

        # Impact summary
        parts = []
        if abs(wc_freed) > 10000:
            if wc_freed > 0:
                parts.append(f"Frigör {wc_freed/1e6:.1f} MSEK rörelsekapital")
            else:
                parts.append(f"Binder {abs(wc_freed)/1e6:.1f} MSEK ytterligare rörelsekapital")
        if abs(ebitda_change) > 10000:
            if ebitda_change > 0:
                parts.append(f"ökar EBITDA med {ebitda_change/1e3:.0f} tkr")
            else:
                parts.append(f"minskar EBITDA med {abs(ebitda_change)/1e3:.0f} tkr")
        impact = " och ".join(parts) if parts else "Minimal påverkan"

        return SimResult(
            model="ccc",
            timestamp=now_iso(),
            baseline={
                "dio_days": baseline["dio_days"],
                "dso_days": baseline["dso_days"],
                "dpo_days": baseline["dpo_days"],
                "ccc_days": baseline["ccc_days"],
                "annual_revenue": baseline["annual_revenue"],
                "working_capital_tied": baseline["working_capital_tied"],
                "ebitda": baseline["ebitda"],
            },
            adjusted={
                "dso_days": round(adj_dso, 1),
                "dpo_days": round(adj_dpo, 1),
                "ccc_days": round(adj_ccc, 1),
                "annual_revenue": round(adj_rev),
                "working_capital_tied": round(adj_wc),
                "ebitda": round(adj_ebitda),
            },
            delta={
                "ccc_days": {"abs": round(ccc_change, 1), "pct": round(ccc_change / baseline["ccc_days"] * 100, 1) if baseline["ccc_days"] else 0},
                "working_capital_freed": {"abs": round(wc_freed)},
                "ebitda_change": {"abs": round(ebitda_change), "pct": round(ebitda_change / baseline["ebitda"] * 100, 1) if baseline["ebitda"] else 0},
            },
            impact_summary=impact,
            details={
                "n_projects": n_projects,
                "rev_per_project": round(rev_per_project),
            },
            warnings=warnings,
        )

    def monte_carlo(self, inputs: dict, iterations: int = 10000,
                    company_code: str = "RM") -> MCResult:
        baseline = self.fetch_baseline(company_code)

        rev = baseline["annual_revenue"]
        costs = baseline["annual_costs"]
        dio = baseline["dio_days"]
        dso = baseline["dso_days"]
        dpo = baseline["dpo_days"]

        # Distributionsparametrar (kan overridas via inputs)
        dso_std = float(inputs.get("dso_std", max(dso * 0.15, 3)))
        dpo_std = float(inputs.get("dpo_std", max(dpo * 0.10, 2)))
        rev_std = float(inputs.get("rev_std", rev * 0.05))
        cost_std = float(inputs.get("cost_std", costs * 0.05))

        wc_values = []
        for _ in range(iterations):
            sim_dso = max(0, random.gauss(dso, dso_std))
            sim_dpo = max(0, random.gauss(dpo, dpo_std))
            sim_rev = max(0, random.gauss(rev, rev_std))
            sim_ccc = dio + sim_dso - sim_dpo
            sim_wc = (sim_ccc / 365) * sim_rev
            wc_values.append(sim_wc)

        warnings = []
        for key, label in [("dio", "DIO"), ("dso", "DSO"), ("dpo", "DPO")]:
            n = baseline["samples"][key]
            if n < 10:
                warnings.append(f"{label} har {n} datapunkter — Monte Carlo-fördelningen är uppskattad")

        mean_wc = round(sum(wc_values) / len(wc_values))
        baseline_wc = baseline["working_capital_tied"]

        return MCResult(
            model="ccc",
            timestamp=now_iso(),
            iterations=iterations,
            metric="working_capital_tied",
            mean=mean_wc,
            std=round(float(statistics.stdev(wc_values)) if len(wc_values) > 1 else 0),
            p10=round(percentile(wc_values, 10)),
            p25=round(percentile(wc_values, 25)),
            p50=round(percentile(wc_values, 50)),
            p75=round(percentile(wc_values, 75)),
            p90=round(percentile(wc_values, 90)),
            histogram=build_histogram(wc_values, bins=20),
            baseline_value=baseline_wc,
            impact_summary=f"Bundet kapital: P10={percentile(wc_values,10)/1e6:.1f} MSEK, P50={percentile(wc_values,50)/1e6:.1f} MSEK, P90={percentile(wc_values,90)/1e6:.1f} MSEK",
            warnings=warnings,
        )


# Registrera vid import
import statistics
_ccc = CCCModel()
register_model(_ccc)
