#!/usr/bin/env python3
"""
sweep_nudges.py — Genererar nudge_queue-rader.

Typer:
  at_risk         — 2+ veckor rod i rad (kors var 15 min)
  weekly_commit   — mandagar: pinga owners som inte committats (cron man 08:00)
  weekly_reminder — torsdagar: paminnelse att rapportera (cron tor 14:00)
  weekly_summary  — fredagar: veckosammanfattning (cron fre 16:00)

Anvandning:
  python3 sweep_nudges.py                    # default: at_risk
  python3 sweep_nudges.py --type=weekly_commit
  python3 sweep_nudges.py --type=all         # kor alla typer
"""
import argparse
import sys
sys.path.insert(0, '/opt/rm-infra')
from rm_data import query_one

SWEEP_FUNCTIONS = {
    'at_risk': 'sweep_at_risk_nudges',
    'weekly_commit': 'sweep_weekly_commit',
    'weekly_reminder': 'sweep_weekly_reminder',
    'weekly_summary': 'sweep_weekly_summary',
}

def run_sweep(sweep_type: str):
    fn = SWEEP_FUNCTIONS.get(sweep_type)
    if not fn:
        print(f"Okand sweep-typ: {sweep_type}. Tillgangliga: {list(SWEEP_FUNCTIONS.keys())}")
        sys.exit(1)
    result = query_one(f"SELECT {fn}()")
    count = list(result.values())[0] if isinstance(result, dict) else result
    print(f"{fn}() -> {count} nya nudges")
    return count

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', default='at_risk', help='Sweep-typ: at_risk, weekly_commit, weekly_reminder, weekly_summary, all')
    args = parser.parse_args()

    if args.type == 'all':
        total = 0
        for t in SWEEP_FUNCTIONS:
            total += run_sweep(t)
        print(f"Totalt: {total} nya nudges")
    else:
        run_sweep(args.type)
