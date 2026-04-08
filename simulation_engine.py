"""
Simuleringsmotor — RM OS Modul 6.
Generisk motor med pluggbart modell-registry, standardformat och Monte Carlo-stöd.
Read-only: läser data från andra moduler, skriver aldrig tillbaka.
"""

import random
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional

from rm_data import query_dicts, execute


# ---------------------------------------------------------------------------
# Data classes — standardformat
# ---------------------------------------------------------------------------

@dataclass
class ParamDef:
    """Definition av en simuleringsparameter."""
    name: str
    display_name: str
    type: str              # "float", "int", "pct"
    default: float
    min_val: float
    max_val: float
    step: float
    unit: str              # "dagar", "%", "SEK", "st"
    description: str = ""

    def to_dict(self):
        return {
            "name": self.name,
            "display_name": self.display_name,
            "type": self.type,
            "default": self.default,
            "min": self.min_val,
            "max": self.max_val,
            "step": self.step,
            "unit": self.unit,
            "description": self.description,
        }


@dataclass
class SimResult:
    """Resultat av en deterministisk körning."""
    model: str
    timestamp: str
    baseline: dict
    adjusted: dict
    delta: dict
    impact_summary: str
    details: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


@dataclass
class MCResult:
    """Resultat av en Monte Carlo-körning."""
    model: str
    timestamp: str
    iterations: int
    metric: str
    mean: float
    std: float
    p10: float
    p25: float
    p50: float
    p75: float
    p90: float
    histogram: list         # [{bin_start, bin_end, count}, ...]
    baseline_value: float
    impact_summary: str
    warnings: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


# ---------------------------------------------------------------------------
# Base model interface
# ---------------------------------------------------------------------------

class SimModel:
    """Basinterface för alla simuleringsmodeller."""

    name: str = ""
    display_name: str = ""
    description: str = ""
    category: str = ""

    def parameters(self, company_code: str = "RM") -> list:
        """Returnerar lista med ParamDef."""
        raise NotImplementedError

    def fetch_baseline(self, company_code: str = "RM") -> dict:
        """Hämtar aktuella värden från databasen."""
        raise NotImplementedError

    def compute(self, inputs: dict, company_code: str = "RM") -> SimResult:
        """Kör deterministisk beräkning."""
        raise NotImplementedError

    def monte_carlo(self, inputs: dict, iterations: int = 10000,
                    company_code: str = "RM") -> MCResult:
        """Kör stokastisk simulering. Default: override i modell."""
        raise NotImplementedError

    def info(self) -> dict:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "description": self.description,
            "category": self.category,
        }


# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------

_registry: dict[str, SimModel] = {}


def register_model(model: SimModel):
    """Registrera en simuleringsmodell."""
    _registry[model.name] = model


def get_model(name: str) -> Optional[SimModel]:
    return _registry.get(name)


def list_models() -> list[dict]:
    return [m.info() for m in _registry.values()]


# ---------------------------------------------------------------------------
# Persistence — simulation_run
# ---------------------------------------------------------------------------

def save_run(model: str, run_type: str, user_email: str,
             inputs: dict, result: dict, impact_summary: str,
             company_code: str = "RM") -> int:
    """Spara en körning och returnera id."""
    import json
    row_id = execute(
        """INSERT INTO simulation_run
           (company_code, model, run_type, user_email, inputs, result, impact_summary)
           VALUES (%s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (company_code, model, run_type, user_email,
         json.dumps(inputs, default=str),
         json.dumps(result, default=str),
         impact_summary),
        returning=True
    )
    return row_id if isinstance(row_id, int) else 0


def get_history(model: str = None, days: int = 30,
                company_code: str = "RM", limit: int = 50) -> list:
    """Hämta sparade körningar."""
    if model:
        return query_dicts(
            """SELECT id, model, run_type, user_email, inputs, impact_summary,
                      created_at::text
               FROM simulation_run
               WHERE company_code = %s AND model = %s
                     AND created_at > NOW() - INTERVAL '%s days'
               ORDER BY created_at DESC LIMIT %s""",
            (company_code, model, days, limit)
        )
    return query_dicts(
        """SELECT id, model, run_type, user_email, inputs, impact_summary,
                  created_at::text
           FROM simulation_run
           WHERE company_code = %s AND created_at > NOW() - INTERVAL '%s days'
           ORDER BY created_at DESC LIMIT %s""",
        (company_code, days, limit)
    )


def get_runs_by_ids(run_ids: list) -> list:
    """Hämta specifika körningar för jämförelse."""
    if not run_ids:
        return []
    placeholders = ",".join(["%s"] * len(run_ids))
    return query_dicts(
        f"""SELECT id, model, run_type, user_email, inputs, result,
                   impact_summary, created_at::text
            FROM simulation_run
            WHERE id IN ({placeholders})
            ORDER BY created_at""",
        tuple(run_ids)
    )


# ---------------------------------------------------------------------------
# Monte Carlo helpers
# ---------------------------------------------------------------------------

def build_histogram(values: list, bins: int = 20) -> list:
    """Bygg histogram-data från en lista med värden."""
    if not values:
        return []
    mn, mx = min(values), max(values)
    if mn == mx:
        return [{"bin_start": mn, "bin_end": mx, "count": len(values)}]
    width = (mx - mn) / bins
    hist = []
    for i in range(bins):
        lo = mn + i * width
        hi = lo + width
        count = sum(1 for v in values if lo <= v < hi) if i < bins - 1 \
            else sum(1 for v in values if lo <= v <= hi)
        hist.append({"bin_start": round(lo, 2), "bin_end": round(hi, 2), "count": count})
    return hist


def percentile(values: list, p: float) -> float:
    """Beräkna percentil."""
    sorted_v = sorted(values)
    k = (len(sorted_v) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_v) else f
    d = k - f
    return sorted_v[f] + d * (sorted_v[c] - sorted_v[f])


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
