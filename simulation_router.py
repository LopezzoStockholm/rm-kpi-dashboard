"""
Simulering API — RM OS Modul 6.
REST-endpoints för simuleringsmotorn. Read-only mot andra modulers data.
Multi-company: resolves company_code from X-Company-Code header via get_company_code().
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import Optional

from simulation_engine import (
    list_models, get_model, save_run, get_history, get_runs_by_ids,
)

# Importera modeller så de registreras
import sim_ccc  # noqa: F401
import sim_project  # noqa: F401
import sim_capacity  # noqa: F401
import sim_financing  # noqa: F401

router = APIRouter()

# --- Dependency injection ---
_get_current_user = None
_has_perm = None
_require_perm = None
_audit_log = None
_get_company_code = None


def init_simulation_router(get_current_user_fn, has_perm_fn, require_perm_fn, audit_log_fn, get_company_code_fn=None):
    global _get_current_user, _has_perm, _require_perm, _audit_log, _get_company_code
    _get_current_user = get_current_user_fn
    _has_perm = has_perm_fn
    _require_perm = require_perm_fn
    _audit_log = audit_log_fn
    _get_company_code = get_company_code_fn


def _resolve_company(request, user):
    """Resolve company_code from request header or user default."""
    if _get_company_code:
        return _get_company_code(request, user)
    return "RM"


# --- Pydantic models ---

class SimRunRequest(BaseModel):
    inputs: dict = {}


class MCRunRequest(BaseModel):
    inputs: dict = {}
    iterations: int = 10000


# ============================================================================
# /api/simulation/models — lista alla modeller
# ============================================================================

@router.get("/api/simulation/models")
async def sim_list_models(request: Request):
    user = await _get_current_user(request)
    return {"models": list_models()}


# ============================================================================
# /api/simulation/{model}/parameters — parametrar med defaults
# ============================================================================

@router.get("/api/simulation/{model_name}/parameters")
async def sim_parameters(model_name: str, request: Request):
    user = await _get_current_user(request)
    company_code = _resolve_company(request, user)
    model = get_model(model_name)
    if not model:
        raise HTTPException(status_code=404, detail=f"Modell '{model_name}' finns inte")
    try:
        params = model.parameters(company_code)
        baseline = model.fetch_baseline(company_code)
        return {
            "model": model.info(),
            "parameters": [p.to_dict() for p in params],
            "baseline": baseline,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/simulation/{model}/run — deterministisk körning
# ============================================================================

@router.post("/api/simulation/{model_name}/run")
async def sim_run(model_name: str, body: SimRunRequest, request: Request):
    user = await _get_current_user(request)
    company_code = _resolve_company(request, user)
    model = get_model(model_name)
    if not model:
        raise HTTPException(status_code=404, detail=f"Modell '{model_name}' finns inte")
    try:
        result = model.compute(body.inputs, company_code)
        run_id = save_run(
            model=model_name,
            run_type="deterministic",
            user_email=user.get("email", "unknown"),
            inputs=body.inputs,
            result=result.to_dict(),
            impact_summary=result.impact_summary,
            company_code=company_code,
        )
        resp = result.to_dict()
        resp["run_id"] = run_id
        return resp
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/simulation/{model}/monte-carlo — stokastisk körning
# ============================================================================

@router.post("/api/simulation/{model_name}/monte-carlo")
async def sim_monte_carlo(model_name: str, body: MCRunRequest, request: Request):
    user = await _get_current_user(request)
    company_code = _resolve_company(request, user)
    model = get_model(model_name)
    if not model:
        raise HTTPException(status_code=404, detail=f"Modell '{model_name}' finns inte")
    try:
        iterations = min(body.iterations, 50000)
        result = model.monte_carlo(body.inputs, iterations, company_code)
        run_id = save_run(
            model=model_name,
            run_type="monte_carlo",
            user_email=user.get("email", "unknown"),
            inputs={**body.inputs, "iterations": iterations},
            result=result.to_dict(),
            impact_summary=result.impact_summary,
            company_code=company_code,
        )
        resp = result.to_dict()
        resp["run_id"] = run_id
        return resp
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/simulation/history — sparade körningar
# ============================================================================

@router.get("/api/simulation/history")
async def sim_history(request: Request, model: Optional[str] = None, days: int = 30):
    user = await _get_current_user(request)
    _require_perm(user, "simulation.admin")
    company_code = _resolve_company(request, user)
    try:
        runs = get_history(model=model, days=days, company_code=company_code)
        return {"count": len(runs), "runs": runs}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/simulation/{model}/compare — jämför scenarion
# ============================================================================

@router.get("/api/simulation/{model_name}/compare")
async def sim_compare(model_name: str, request: Request, run_ids: str = ""):
    user = await _get_current_user(request)
    _require_perm(user, "simulation.admin")
    if not run_ids:
        raise HTTPException(status_code=400, detail="run_ids krävs (kommaseparerade)")
    try:
        ids = [int(x.strip()) for x in run_ids.split(",") if x.strip()]
        runs = get_runs_by_ids(ids)
        runs = [r for r in runs if r.get("model") == model_name]
        return {"model": model_name, "count": len(runs), "runs": runs}
    except ValueError:
        raise HTTPException(status_code=400, detail="run_ids måste vara heltal")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/simulation/ccc/trend — CCC-tidsserie
# ============================================================================

@router.get("/api/simulation/ccc/trend")
async def ccc_trend(request: Request, days: int = 90):
    user = await _get_current_user(request)
    company_code = _resolve_company(request, user)
    try:
        from rm_data import query_dicts as qd
        rows = qd("""
            SELECT snapshot_date::text, dio_days, dso_days, dpo_days, ccc_days,
                   working_capital, sample_size
            FROM ccc_snapshot
            WHERE company_code = %s AND project_code IS NULL
                  AND snapshot_date > CURRENT_DATE - %s
            ORDER BY snapshot_date
        """, (company_code, days))
        return {"days": days, "count": len(rows), "trend": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# /api/simulation/summary — kompakt widget-data fran alla modeller
# ============================================================================

@router.get("/api/simulation/summary")
async def sim_summary(request: Request):
    user = await _get_current_user(request)
    company_code = _resolve_company(request, user)
    models_info = list_models()
    summaries = []
    for m_info in models_info:
        model = get_model(m_info["name"])
        if not model:
            continue
        try:
            baseline = model.fetch_baseline(company_code)
            entry = {
                "model": m_info["name"],
                "display_name": m_info["display_name"],
                "category": m_info.get("category", ""),
            }
            name = m_info["name"]
            if name == "ccc":
                entry["metrics"] = [
                    {"key": "ccc_days", "label": "CCC", "value": baseline.get("ccc_days"), "fmt": "days"},
                    {"key": "working_capital_tied", "label": "Bundet kapital", "value": baseline.get("working_capital_tied"), "fmt": "money"},
                    {"key": "ebitda", "label": "EBITDA", "value": baseline.get("ebitda"), "fmt": "money"},
                ]
                ccc = baseline.get("ccc_days", 0)
                entry["status"] = "green" if ccc < 30 else "yellow" if ccc < 60 else "red"
            elif name == "project":
                entry["metrics"] = [
                    {"key": "project_count", "label": "Projekt", "value": baseline.get("project_count"), "fmt": "int"},
                    {"key": "avg_tb1_margin", "label": "TB1-marginal", "value": baseline.get("avg_tb1_margin"), "fmt": "pct"},
                    {"key": "tb2", "label": "TB2", "value": baseline.get("tb2"), "fmt": "money"},
                ]
                tb2 = baseline.get("tb2", 0)
                entry["status"] = "green" if tb2 > 0 else "yellow" if tb2 > -500000 else "red"
            elif name == "capacity":
                entry["metrics"] = [
                    {"key": "utilization_pct", "label": "Belaggning", "value": baseline.get("utilization_pct"), "fmt": "pct"},
                    {"key": "active_projects", "label": "Aktiva projekt", "value": baseline.get("active_projects"), "fmt": "int"},
                    {"key": "annual_revenue_capacity", "label": "Kapacitet", "value": baseline.get("annual_revenue_capacity"), "fmt": "money"},
                ]
                util = baseline.get("utilization_pct", 0)
                entry["status"] = "green" if util <= 90 else "yellow" if util <= 120 else "red"
            elif name == "financing":
                entry["metrics"] = [
                    {"key": "ebitda", "label": "EBITDA", "value": baseline.get("ebitda"), "fmt": "money"},
                    {"key": "max_debt", "label": "Max belaning", "value": baseline.get("max_debt"), "fmt": "money"},
                    {"key": "bank_balance", "label": "Banksaldo", "value": baseline.get("bank_balance"), "fmt": "money"},
                ]
                ebitda = baseline.get("ebitda", 0)
                entry["status"] = "green" if ebitda > 500000 else "yellow" if ebitda > 0 else "red"
            else:
                entry["metrics"] = []
                entry["status"] = "gray"
            summaries.append(entry)
        except Exception:
            summaries.append({
                "model": m_info["name"],
                "display_name": m_info["display_name"],
                "metrics": [],
                "status": "gray",
                "error": True,
            })
    return {"models": summaries}
