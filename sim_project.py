"""
Simuleringsmodell: Projektlönsamhet.
Läser project_profitability + Fortnox-data.
Parametrar: TB1-marginal, overhead-andel, volym, snittpris per projekt.
Monte Carlo: varierar TB1-marginal och antal projekt.
"""

import random
import statistics
from rm_data import query_dicts
from simulation_engine import (
    SimModel, ParamDef, SimResult, MCResult,
    register_model, build_histogram, percentile, now_iso,
)


class ProjectModel(SimModel):
    name = "project"
    display_name = "Projektlönsamhet"
    description = "TB1-analys, break-even och lönsamhetssimulering per projektportfölj"
    category = "lönsamhet"

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------

    def _project_stats(self, company_code: str) -> dict:
        """Hämtar aggregerad statistik från project_profitability."""
        rows = query_dicts("""
            SELECT
                COUNT(*) as project_count,
                COALESCE(SUM(revenue), 0) as total_revenue,
                COALESCE(SUM(supplier_costs), 0) as total_costs,
                COALESCE(SUM(tb1), 0) as total_tb1,
                COALESCE(AVG(CASE WHEN revenue > 0 THEN tb1_margin END), 0) as avg_margin
            FROM project_profitability
            WHERE company_code = %s
        """, (company_code,))
        if rows:
            return {
                "project_count": int(rows[0]["project_count"] or 0),
                "total_revenue": float(rows[0]["total_revenue"] or 0),
                "total_costs": float(rows[0]["total_costs"] or 0),
                "total_tb1": float(rows[0]["total_tb1"] or 0),
                "avg_margin": float(rows[0]["avg_margin"] or 0),
            }
        return {"project_count": 0, "total_revenue": 0, "total_costs": 0,
                "total_tb1": 0, "avg_margin": 0}

    def _project_details(self, company_code: str) -> list:
        """Hämtar per-projekt-data för fördelningsanalys."""
        return query_dicts("""
            SELECT project_number, project_name, revenue, supplier_costs,
                   tb1, tb1_margin, project_group
            FROM project_profitability
            WHERE revenue > 0 OR supplier_costs > 0
            ORDER BY revenue DESC
        """)

    def _overhead(self, company_code: str) -> float:
        """Hämtar overhead-kostnader (projekt 101 eller liknande)."""
        rows = query_dicts("""
            SELECT COALESCE(SUM(supplier_costs), 0) as overhead
            FROM project_profitability
            WHERE project_number = '101'
              AND company_code = %s
        """, (company_code,))
        return float(rows[0]["overhead"]) if rows else 0

    def _active_project_margins(self, company_code: str) -> list:
        """Hämtar TB1-marginaler för aktiva projekt med intäkt."""
        rows = query_dicts("""
            SELECT tb1_margin
            FROM project_profitability
            WHERE revenue > 0 AND project_number != '101'
              AND company_code = %s
        """, (company_code,))
        return [float(r["tb1_margin"]) for r in rows] if rows else []

    # ------------------------------------------------------------------
    # Interface
    # ------------------------------------------------------------------

    def fetch_baseline(self, company_code: str = "RM") -> dict:
        stats = self._project_stats(company_code)
        overhead = self._overhead(company_code)
        margins = self._active_project_margins(company_code)
        details = self._project_details(company_code)

        rev = stats["total_revenue"]
        costs = stats["total_costs"]
        tb1 = stats["total_tb1"]
        n_projects = len([d for d in details if float(d.get("revenue", 0)) > 0])
        rev_per_project = rev / n_projects if n_projects > 0 else 0
        cost_per_project = costs / n_projects if n_projects > 0 else 0

        # Break-even: hur många projekt krävs vid nuvarande snittmarginal?
        avg_tb1_per_project = tb1 / n_projects if n_projects > 0 else 0
        break_even_projects = (
            int(-(-overhead // avg_tb1_per_project))  # ceiling division
            if avg_tb1_per_project > 0 else 0
        )

        # TB2 = TB1 - overhead
        tb2 = tb1 - overhead

        return {
            "project_count": n_projects,
            "total_revenue": round(rev),
            "total_costs": round(costs),
            "total_tb1": round(tb1),
            "avg_tb1_margin": round(stats["avg_margin"], 1),
            "overhead": round(overhead),
            "tb2": round(tb2),
            "rev_per_project": round(rev_per_project),
            "cost_per_project": round(cost_per_project),
            "break_even_projects": break_even_projects,
            "margin_spread": {
                "min": round(min(margins), 1) if margins else 0,
                "max": round(max(margins), 1) if margins else 0,
                "std": round(statistics.stdev(margins), 1) if len(margins) > 1 else 0,
            },
            "samples": {
                "projects": n_projects,
                "with_margin": len(margins),
            }
        }

    def parameters(self, company_code: str = "RM") -> list:
        baseline = self.fetch_baseline(company_code)
        return [
            ParamDef("margin_delta_pct", "TB1-marginalförändring", "pct", 0,
                     -20, 20, 1, "%",
                     f"Förändring av genomsnittlig TB1-marginal. Nuvarande: {baseline['avg_tb1_margin']}%"),
            ParamDef("volume_delta", "Antal projekt +/-", "int", 0,
                     -5, 10, 1, "st",
                     f"Extra eller färre projekt. Nuvarande: {baseline['project_count']}"),
            ParamDef("rev_per_project_delta_pct", "Snittintäkt/projekt +/-", "pct", 0,
                     -30, 30, 5, "%",
                     f"Förändring av snittintäkt per projekt. Nuvarande: {baseline['rev_per_project']/1000:.0f} tkr"),
            ParamDef("overhead_delta_pct", "Overhead-förändring", "pct", 0,
                     -30, 30, 5, "%",
                     f"Förändring av overheadkostnader. Nuvarande: {baseline['overhead']/1000:.0f} tkr"),
        ]

    def compute(self, inputs: dict, company_code: str = "RM") -> SimResult:
        baseline = self.fetch_baseline(company_code)

        margin_delta = float(inputs.get("margin_delta_pct", 0))
        volume_delta = int(inputs.get("volume_delta", 0))
        rev_pct = float(inputs.get("rev_per_project_delta_pct", 0))
        overhead_pct = float(inputs.get("overhead_delta_pct", 0))

        # Baseline-värden
        n = baseline["project_count"]
        rev_pp = baseline["rev_per_project"]
        cost_pp = baseline["cost_per_project"]
        avg_margin = baseline["avg_tb1_margin"]
        overhead = baseline["overhead"]

        # Justerade värden
        adj_n = max(0, n + volume_delta)
        adj_rev_pp = rev_pp * (1 + rev_pct / 100)
        adj_margin = avg_margin + margin_delta
        adj_cost_pp = adj_rev_pp * (1 - adj_margin / 100) if adj_margin < 100 else 0
        adj_overhead = overhead * (1 + overhead_pct / 100)

        adj_rev = adj_n * adj_rev_pp
        adj_costs = adj_n * adj_cost_pp + adj_overhead
        adj_tb1 = adj_rev - (adj_n * adj_cost_pp)
        adj_tb2 = adj_tb1 - adj_overhead

        # Break-even
        avg_tb1_pp = adj_rev_pp - adj_cost_pp
        adj_break_even = (
            int(-(-adj_overhead // avg_tb1_pp))
            if avg_tb1_pp > 0 else 0
        )

        # Delta
        tb2_change = adj_tb2 - baseline["tb2"]

        # Warnings
        warnings = []
        if baseline["samples"]["projects"] < 3:
            warnings.append(f"Bara {baseline['samples']['projects']} projekt med intäkt — låg konfidensgrad")
        if adj_margin < 0:
            warnings.append(f"Justerad TB1-marginal är negativ ({adj_margin:.1f}%) — samtliga projekt går med förlust")
        if adj_break_even > adj_n and adj_n > 0:
            warnings.append(f"Break-even kräver {adj_break_even} projekt men bara {adj_n} planerade — negativt TB2")

        # Impact
        parts = []
        if abs(tb2_change) > 10000:
            direction = "ökar" if tb2_change > 0 else "minskar"
            parts.append(f"TB2 {direction} med {abs(tb2_change)/1e3:.0f} tkr")
        if adj_break_even != baseline["break_even_projects"]:
            parts.append(f"Break-even: {baseline['break_even_projects']} → {adj_break_even} projekt")
        impact = " och ".join(parts) if parts else "Minimal påverkan"

        return SimResult(
            model="project",
            timestamp=now_iso(),
            baseline={
                "project_count": baseline["project_count"],
                "total_revenue": baseline["total_revenue"],
                "total_tb1": baseline["total_tb1"],
                "avg_tb1_margin": baseline["avg_tb1_margin"],
                "overhead": baseline["overhead"],
                "tb2": baseline["tb2"],
                "break_even_projects": baseline["break_even_projects"],
            },
            adjusted={
                "project_count": adj_n,
                "total_revenue": round(adj_rev),
                "total_tb1": round(adj_tb1),
                "avg_tb1_margin": round(adj_margin, 1),
                "overhead": round(adj_overhead),
                "tb2": round(adj_tb2),
                "break_even_projects": adj_break_even,
            },
            delta={
                "tb2": {"abs": round(tb2_change), "pct": round(tb2_change / baseline["tb2"] * 100, 1) if baseline["tb2"] else 0},
                "break_even": {"abs": adj_break_even - baseline["break_even_projects"]},
            },
            impact_summary=impact,
            details={
                "rev_per_project": round(adj_rev_pp),
                "cost_per_project": round(adj_cost_pp),
                "margin_spread": baseline["margin_spread"],
            },
            warnings=warnings,
        )

    def monte_carlo(self, inputs: dict, iterations: int = 10000,
                    company_code: str = "RM") -> MCResult:
        baseline = self.fetch_baseline(company_code)

        n = baseline["project_count"]
        rev_pp = baseline["rev_per_project"]
        avg_margin = baseline["avg_tb1_margin"]
        overhead = baseline["overhead"]
        margin_std = baseline["margin_spread"]["std"] if baseline["margin_spread"]["std"] > 0 else 10

        # Distributions
        margin_std_mc = float(inputs.get("margin_std", margin_std))
        volume_std = float(inputs.get("volume_std", max(n * 0.2, 1)))
        rev_std = float(inputs.get("rev_std", rev_pp * 0.15))

        tb2_values = []
        for _ in range(iterations):
            sim_n = max(1, int(random.gauss(n, volume_std)))
            sim_rev_pp = max(0, random.gauss(rev_pp, rev_std))
            sim_margin = random.gauss(avg_margin, margin_std_mc)
            sim_cost_pp = sim_rev_pp * (1 - sim_margin / 100)
            sim_tb1 = sim_n * (sim_rev_pp - sim_cost_pp)
            sim_tb2 = sim_tb1 - overhead
            tb2_values.append(sim_tb2)

        warnings = []
        if baseline["samples"]["projects"] < 5:
            warnings.append(f"Bara {baseline['samples']['projects']} projekt — MC-fördelningen är grov uppskattning")

        mean_tb2 = round(sum(tb2_values) / len(tb2_values))

        return MCResult(
            model="project",
            timestamp=now_iso(),
            iterations=iterations,
            metric="tb2",
            mean=mean_tb2,
            std=round(float(statistics.stdev(tb2_values)) if len(tb2_values) > 1 else 0),
            p10=round(percentile(tb2_values, 10)),
            p25=round(percentile(tb2_values, 25)),
            p50=round(percentile(tb2_values, 50)),
            p75=round(percentile(tb2_values, 75)),
            p90=round(percentile(tb2_values, 90)),
            histogram=build_histogram(tb2_values, bins=20),
            baseline_value=baseline["tb2"],
            impact_summary=f"TB2: P10={percentile(tb2_values,10)/1e3:.0f} tkr, P50={percentile(tb2_values,50)/1e3:.0f} tkr, P90={percentile(tb2_values,90)/1e3:.0f} tkr",
            warnings=warnings,
        )


# Registrera vid import
_project = ProjectModel()
register_model(_project)
