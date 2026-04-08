#!/usr/bin/env python3
"""sweep_blocker_escalation.py — Eskalerar överförfallna blockers i Task Hub.

Körs via cron var 15:e minut. Hittar aktiva blockers med passerat follow_up_date,
markerar dem som eskalerade och skapar in-app notiser.
"""
import sys
sys.path.insert(0, '/opt/rm-infra')
from rm_data import execute
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('blocker_escalation')

# execute() committar — query_one() gör det INTE (rollback vid conn.close)
count = execute("SELECT sweep_blocker_escalation()", returning=True)
log.info(f"sweep_blocker_escalation() => {count} blockers eskalerade")
