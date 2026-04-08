#!/usr/bin/env python3
"""
cashflow_alert.py — Kassaflödesagent v3 för RM Entreprenad och Fasad

Analyserar kassaposition + fordringar + skulder + löner + fasta kostnader + månadsmoms.
Tre scenarier (konservativ/bas/optimistisk) per horisont.
Varnar via Teams Ekonomi-kanalen vid risk.

Kör dagligen 07:00 via cron.

v3 ändringar (2026-04-02):
  - Månadsmoms istället för kvartalsmoms (betaldag 12:e varje månad)
  - Moms beräknas per rapportmånad, inte per kvartal
  - Hanterar negativ moms (tillbaka) som positivt kassaflöde

Datakällor (rm_central):
  - bank_balance: Aktuell kassa (Fortnox PSD2 Handelsbanken)
  - fortnox_invoice: Kundfordringar med förfallodatum + label
  - fortnox_supplier_invoice: Leverantörsskulder med förfallodatum + label
  - fortnox_salary_monthly: Historiska lönekostnader per månad
  - next_project_economy: Ej fakturerat arbete (framtida intäkt)
  - recurring_cost: Fasta återkommande kostnader (hyra, försäkring etc)
"""

import json
import subprocess
import sys
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from statistics import mean
import calendar

# ── Import rm_data-modul ──
sys.path.insert(0, '/opt/rm-infra')
from rm_data import query_one, query_all, query_dicts, safe_json_query

# ── Config ──
CONFIG_DIR = Path("/opt/rm-infra")
NOTIFICATION_CONFIG = CONFIG_DIR / "notification-config.json"
STATE_FILE = CONFIG_DIR / "cashflow_alert_state.json"
LOG_PREFIX = "[cashflow_alert]"

# Thresholds
CRITICAL_THRESHOLD = 0
WARNING_THRESHOLD = 200_000
WATCH_THRESHOLD = 500_000
DAYS_FORECAST = [14, 30, 60]

CENTRAL_DB = "rm_central"

# ── Label-viktning ──
RECV_LABEL_WEIGHTS = {
    "Parkerad": 0.0,
    "Tvist": 0.0,
    "Bevakas": 0.5,
}
RECV_OVERDUE_NO_LABEL = 0.25
RECV_CURRENT_NO_LABEL = 1.0

PAY_LABEL_WEIGHTS = {
    "Parkerad": 0.0,
    "Tvist": 0.0,
    "Bevakas": 0.5,
}

# ── Scenario-multiplikatorer ──
SCENARIO_MULTIPLIERS = {
    "conservative": {"inflow": 0.70, "outflow": 1.10},
    "base":         {"inflow": 1.00, "outflow": 1.00},
    "optimistic":   {"inflow": 1.15, "outflow": 0.90},
}

# ── Löne-schema ──
SALARY_PAY_DAY = 25
EMPLOYER_TAX_DAY = 12

# ── Moms-schema ──
# Månadsmoms: betalas 12:e månaden efter rapportmånad
# (t.ex. moms för mars betalas 12 april)
VAT_PAY_DAY = 12


def log(msg):
    print(f"{LOG_PREFIX} {datetime.now().isoformat()} {msg}")


# ── Datahämtning ──

def get_bank_balance():
    """Hämta aktuell banksaldo från bank_balance-tabell."""
    rows = query_all("""
        SELECT balance, balance_date, booked_balance
        FROM bank_balance
        WHERE company_code = 'RM'
        ORDER BY balance_date DESC
        LIMIT 1
    """)
    if not rows:
        return None, None, None
    balance, balance_date, booked_balance = rows[0]
    return float(balance) if balance else None, balance_date, float(booked_balance) if booked_balance else None


def get_receivables_weighted():
    """Kundfordringar per period med label-viktning."""
    rows = query_dicts("""
        SELECT
            CASE
                WHEN due_date < CURRENT_DATE THEN 'overdue'
                WHEN due_date <= CURRENT_DATE + 14 THEN 'd14'
                WHEN due_date <= CURRENT_DATE + 30 THEN 'd30'
                WHEN due_date <= CURRENT_DATE + 60 THEN 'd60'
                ELSE 'd60plus'
            END as period,
            COALESCE(label, '') as label,
            COUNT(*) as cnt,
            COALESCE(SUM(balance), 0) as total
        FROM fortnox_invoice
        WHERE balance > 0 AND status != 'cancelled'
        GROUP BY 1, 2
    """)

    result = {
        "raw": {"overdue": 0, "d14": 0, "d30": 0, "d60": 0, "d60plus": 0},
        "weighted": {"overdue": 0, "d14": 0, "d30": 0, "d60": 0, "d60plus": 0},
        "counts": {"overdue": 0, "d14": 0, "d30": 0, "d60": 0, "d60plus": 0},
    }

    for r in rows:
        period = r["period"]
        label = r["label"].strip() if r["label"] else ""
        total = float(r["total"]) if r["total"] else 0
        cnt = int(r["cnt"]) if r["cnt"] else 0

        result["raw"][period] = result["raw"].get(period, 0) + total
        result["counts"][period] = result["counts"].get(period, 0) + cnt

        if label in RECV_LABEL_WEIGHTS:
            weight = RECV_LABEL_WEIGHTS[label]
        elif period == "overdue":
            weight = RECV_OVERDUE_NO_LABEL
        else:
            weight = RECV_CURRENT_NO_LABEL

        result["weighted"][period] = result["weighted"].get(period, 0) + total * weight

    return result


def get_payables_weighted():
    """Leverantörsskulder per period med label-viktning."""
    rows = query_dicts("""
        SELECT
            CASE
                WHEN due_date < CURRENT_DATE THEN 'overdue'
                WHEN due_date <= CURRENT_DATE + 14 THEN 'd14'
                WHEN due_date <= CURRENT_DATE + 30 THEN 'd30'
                WHEN due_date <= CURRENT_DATE + 60 THEN 'd60'
                ELSE 'd60plus'
            END as period,
            COALESCE(label, '') as label,
            COUNT(*) as cnt,
            COALESCE(SUM(balance), 0) as total
        FROM fortnox_supplier_invoice
        WHERE balance > 0 AND status != 'cancelled'
        GROUP BY 1, 2
    """)

    result = {
        "raw": {"overdue": 0, "d14": 0, "d30": 0, "d60": 0, "d60plus": 0},
        "weighted": {"overdue": 0, "d14": 0, "d30": 0, "d60": 0, "d60plus": 0},
        "counts": {"overdue": 0, "d14": 0, "d30": 0, "d60": 0, "d60plus": 0},
    }

    for r in rows:
        period = r["period"]
        label = r["label"].strip() if r["label"] else ""
        total = float(r["total"]) if r["total"] else 0
        cnt = int(r["cnt"]) if r["cnt"] else 0

        result["raw"][period] = result["raw"].get(period, 0) + total
        result["counts"][period] = result["counts"].get(period, 0) + cnt

        if label in PAY_LABEL_WEIGHTS:
            weight = PAY_LABEL_WEIGHTS[label]
        else:
            weight = 1.0

        result["weighted"][period] = result["weighted"].get(period, 0) + total * weight

    return result


def get_not_invoiced():
    """Ej fakturerat arbete (framtida intäkt) från Next."""
    val = query_one("""
        SELECT COALESCE(SUM(earned_revenue_not_invoiced), 0)
        FROM next_project_economy
        WHERE company_code = 'RM'
    """)
    return float(val) if val else 0.0


def get_salary_forecast():
    """Historiska lönekostnader per månad för forecast."""
    rows = query_dicts("""
        SELECT year_month, net_payout, employer_tax, personal_tax,
               (net_payout + employer_tax + personal_tax) as total_cost
        FROM fortnox_salary_monthly
        WHERE company_code = 'RM'
        ORDER BY year_month DESC
        LIMIT 12
    """)
    if not rows:
        return {"net_payout": 0, "employer_tax": 0, "personal_tax": 0}
    
    total_cost = sum(float(r["total_cost"]) if r["total_cost"] else 0 for r in rows)
    months = len(rows)
    avg_monthly = total_cost / months if months > 0 else 0
    
    return {
        "net_payout": avg_monthly * 0.75,
        "employer_tax": avg_monthly * 0.20,
        "personal_tax": avg_monthly * 0.05,
    }


def get_recurring_costs():
    """Fasta återkommande kostnader (hyra, försäkring etc)."""
    rows = query_dicts("""
        SELECT description, monthly_amount, pay_day
        FROM recurring_cost
        WHERE active = true AND company_code = 'RM'
        ORDER BY pay_day
    """)
    
    result = {"total": 0, "by_day": {}}
    for r in rows:
        amount = float(r["monthly_amount"]) if r["monthly_amount"] else 0
        pay_day = int(r["pay_day"]) if r["pay_day"] else 1
        
        result["total"] += amount
        if pay_day not in result["by_day"]:
            result["by_day"][pay_day] = []
        result["by_day"][pay_day].append({
            "description": r["description"],
            "amount": amount
        })
    
    return result


def get_ata_pipeline():
    """ÄTA-pipeline: arbetskostnader per projekt som inte är fakturerade."""
    rows = query_dicts("""
        SELECT
            ata_number, project_name,
            COALESCE(SUM(labor_cost), 0) as labor_cost,
            COALESCE(SUM(material_cost), 0) as material_cost,
            (COALESCE(SUM(labor_cost), 0) + COALESCE(SUM(material_cost), 0)) as total_cost,
            COALESCE(AVG(completion_percent), 0) as completion_percent
        FROM ata_task
        WHERE is_invoiced = FALSE AND company_code = 'RM'
        GROUP BY ata_number, project_name
        ORDER BY ata_number
    """)
    
    result = []
    for r in rows:
        result.append({
            "ata_number": r["ata_number"],
            "project_name": r["project_name"],
            "labor_cost": float(r["labor_cost"]) if r["labor_cost"] else 0,
            "material_cost": float(r["material_cost"]) if r["material_cost"] else 0,
            "total_cost": float(r["total_cost"]) if r["total_cost"] else 0,
            "completion_percent": float(r["completion_percent"]) if r["completion_percent"] else 0,
        })
    
    return result


def get_vat_forecast():
    """VAT-forecast för de nästa 90 dagarna baserat på rapportmånader."""
    # Hämta moms per rapportmånad för de senaste 6 månaderna
    sql = """
        SELECT
            year_month,
            COALESCE(SUM(vat_amount), 0) as vat_amount,
            COALESCE(SUM(deductible_vat), 0) as deductible_vat,
            (COALESCE(SUM(vat_amount), 0) - COALESCE(SUM(deductible_vat), 0)) as net_vat
        FROM fortnox_vat_report
        WHERE company_code = 'RM'
        GROUP BY year_month
        ORDER BY year_month DESC
        LIMIT 6
    """
    rows = query_dicts(sql)
    
    if not rows:
        return {"forecast_90d": 0, "average_monthly": 0}
    
    total_vat = sum(float(r["net_vat"]) if r["net_vat"] else 0 for r in rows)
    months = len(rows)
    avg_monthly = total_vat / months if months > 0 else 0
    
    return {
        "forecast_90d": avg_monthly * 3,
        "average_monthly": avg_monthly,
    }


def get_overdue_details():
    """Detaljerade förfallna fordringar och skulder."""
    recv_rows = query_dicts("""
        SELECT customer_name, total, balance, due_date,
               DATEDIFF(day, due_date, CURRENT_DATE) as days_overdue,
               label
        FROM fortnox_invoice
        WHERE balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled'
        ORDER BY due_date
        LIMIT 20
    """)
    
    pay_rows = query_dicts("""
        SELECT supplier_name, total, balance, due_date,
               DATEDIFF(day, due_date, CURRENT_DATE) as days_overdue,
               label
        FROM fortnox_supplier_invoice
        WHERE balance > 0 AND due_date < CURRENT_DATE AND status != 'cancelled'
        ORDER BY due_date
        LIMIT 20
    """)
    
    return {
        "overdue_receivables": [
            {
                "customer": r["customer_name"],
                "balance": float(r["balance"]) if r["balance"] else 0,
                "due_date": r["due_date"],
                "days_overdue": int(r["days_overdue"]) if r["days_overdue"] else 0,
                "label": r["label"] if r["label"] else ""
            }
            for r in recv_rows
        ],
        "overdue_payables": [
            {
                "supplier": r["supplier_name"],
                "balance": float(r["balance"]) if r["balance"] else 0,
                "due_date": r["due_date"],
                "days_overdue": int(r["days_overdue"]) if r["days_overdue"] else 0,
                "label": r["label"] if r["label"] else ""
            }
            for r in pay_rows
        ],
    }


# ── Prognoser & Scenarier ──

def forecast_inflow(days_ahead, scenario="base"):
    """Prognostiserat kassainflöde för nästa N dagar."""
    recv = get_receivables_weighted()
    multiplier = SCENARIO_MULTIPLIERS[scenario]["inflow"]
    
    # Vägt medelvärde av nästa 30 dagars förväntade inbetalningar
    # Baserat på period-viktning
    base_inflow = recv["weighted"]["d14"] + recv["weighted"]["d30"] + recv["weighted"]["d60"]
    
    if days_ahead <= 14:
        return recv["weighted"]["d14"] * multiplier
    elif days_ahead <= 30:
        return (recv["weighted"]["d14"] + recv["weighted"]["d30"]) * multiplier
    else:
        return (recv["weighted"]["d14"] + recv["weighted"]["d30"] + recv["weighted"]["d60"]) * multiplier


def forecast_outflow(days_ahead, scenario="base"):
    """Prognostiserat kassautflöde för nästa N dagar."""
    pay = get_payables_weighted()
    salary = get_salary_forecast()
    recurring = get_recurring_costs()
    multiplier = SCENARIO_MULTIPLIERS[scenario]["outflow"]
    
    # Löner: månadligt belopp för nästa löneperiod
    salary_cost = (salary["net_payout"] + salary["employer_tax"] + salary["personal_tax"]) / 30 * days_ahead
    
    # Leverantörsskulder: vägt medelvärde
    payables_cost = pay["weighted"]["d14"] + pay["weighted"]["d30"] + pay["weighted"]["d60"]
    if days_ahead <= 14:
        payables_cost = pay["weighted"]["d14"]
    elif days_ahead <= 30:
        payables_cost = pay["weighted"]["d14"] + pay["weighted"]["d30"]
    
    # Återkommande kostnader: daglig andel
    recurring_cost = recurring["total"] / 30 * days_ahead
    
    total = (salary_cost + payables_cost + recurring_cost) * multiplier
    return total


def forecast_cash_position(days_ahead, scenario="base"):
    """Prognostiserad kassaposition efter N dagar."""
    balance, _, _ = get_bank_balance()
    if balance is None:
        return None
    
    inflow = forecast_inflow(days_ahead, scenario)
    outflow = forecast_outflow(days_ahead, scenario)
    
    return balance + inflow - outflow


# ── Notifikations-modul ──

def send_push_notification(title, body, urgency="normal"):
    """Skicka push-notis via push_notify.py."""
    try:
        result = subprocess.run(
            ["/opt/rm-infra/push-venv/bin/python3", "/opt/rm-infra/push_notify.py",
             str(title), str(body), "cashflow", str(urgency)],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            log(f"Push notification sent: {result.stdout.strip()}")
        else:
            log(f"Push notification failed: {result.stderr.strip()}")
    except Exception as e:
        log(f"Push notification error: {e}")


def send_teams_notification(title, body, risk_level="info"):
    """Skicka notis till Teams Ekonomi-kanalen."""
    config_file = NOTIFICATION_CONFIG
    if not config_file.exists():
        log(f"Config file not found: {config_file}")
        return
    
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
        
        webhook_url = config.get("teams_webhook_ekonomi")
        if not webhook_url:
            log("Teams webhook URL not configured")
            return
        
        color_map = {
            "critical": "ff0000",
            "warning": "ff9900",
            "watch": "ffcc00",
            "info": "0078d4",
        }
        color = color_map.get(risk_level, "0078d4")
        
        payload = {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": color,
            "summary": title,
            "sections": [
                {
                    "activityTitle": title,
                    "activitySubtitle": f"Risk Level: {risk_level.upper()}",
                    "text": body,
                    "facts": [
                        {
                            "name": "Time",
                            "value": datetime.now().isoformat()
                        }
                    ]
                }
            ]
        }
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code == 200:
            log(f"Teams notification sent: {title}")
        else:
            log(f"Teams notification failed: {response.status_code} {response.text}")
    
    except Exception as e:
        log(f"Teams notification error: {e}")


# ── State Management ──

def load_state():
    """Ladda tidigare notifikations-state."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_state(state):
    """Spara notifikations-state."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log(f"Failed to save state: {e}")


# ── Huvudanalys ──

def analyze_cashflow():
    """Huvudanalys: hämta data, beräkna scenarier, skicka notifikationer."""
    log("Starting cashflow analysis...")
    
    try:
        # Hämta data
        balance, balance_date, booked = get_bank_balance()
        receivables = get_receivables_weighted()
        payables = get_payables_weighted()
        salary = get_salary_forecast()
        recurring = get_recurring_costs()
        vat = get_vat_forecast()
        not_invoiced = get_not_invoiced()
        ata = get_ata_pipeline()
        overdue = get_overdue_details()
        
        if balance is None:
            log("ERROR: Could not fetch bank balance")
            return
        
        # Prognoser för olika horisonter
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "balance": balance,
            "balance_date": balance_date,
            "booked": booked,
        }
        
        for days in DAYS_FORECAST:
            analysis[f"forecast_{days}d"] = {}
            for scenario in ["conservative", "base", "optimistic"]:
                pos = forecast_cash_position(days, scenario)
                analysis[f"forecast_{days}d"][scenario] = pos
        
        # Detaljerade data
        analysis["receivables"] = receivables
        analysis["payables"] = payables
        analysis["salary"] = salary
        analysis["recurring"] = recurring
        analysis["vat"] = vat
        analysis["not_invoiced"] = not_invoiced
        analysis["ata_pipeline"] = ata
        analysis["overdue"] = overdue
        
        # Bestäm risknivå
        critical_forecast = analysis["forecast_14d"]["conservative"]
        warning_forecast = analysis["forecast_30d"]["conservative"]
        
        risk_level = "info"
        if critical_forecast <= CRITICAL_THRESHOLD:
            risk_level = "critical"
        elif critical_forecast <= WARNING_THRESHOLD:
            risk_level = "warning"
        elif warning_forecast <= WATCH_THRESHOLD:
            risk_level = "watch"
        
        # Notifikation
        state = load_state()
        last_risk = state.get("last_risk_level", "info")
        
        if risk_level != last_risk:
            title = f"Kassaflöde: {risk_level.upper()}"
            body = f"Aktuell saldo: {balance:,.0f} SEK\n"
            body += f"Prognos 14d (konservativ): {critical_forecast:,.0f} SEK\n"
            body += f"Prognos 30d (konservativ): {warning_forecast:,.0f} SEK"
            
            send_teams_notification(title, body, risk_level)
            if risk_level in ["critical", "warning"]:
                send_push_notification(title, body)
            
            state["last_risk_level"] = risk_level
            state["last_alert_time"] = datetime.now().isoformat()
            save_state(state)
        
        log(f"Analysis complete. Risk level: {risk_level}")
        
        # Spara hela analysen för dashboard
        dashboard_file = CONFIG_DIR / "cashflow_latest.json"
        try:
            with open(dashboard_file, "w") as f:
                json.dump(analysis, f, indent=2, default=str)
        except Exception as e:
            log(f"Failed to save dashboard data: {e}")
    
    except Exception as e:
        log(f"Analysis error: {e}")
        import traceback
        traceback.print_exc()
        send_teams_notification("Kassaflödesagent: FEL", f"Analys misslyckades: {e}", "critical")


# ── Entry Point ──

if __name__ == "__main__":
    analyze_cashflow()
