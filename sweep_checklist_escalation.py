#!/usr/bin/env python3
"""sweep_checklist_escalation.py — Eskalerar dagliga checklista-poster som inte bockats av.

Körs via cron var 15:e minut. Tre eskaleringsnivåer:
  Nivå 1 (kl 11): pending item → notis till backup_email
  Nivå 2 (kl 14): fortfarande pending → notis till manager_email
  Nivå 3 (kl 16): fortfarande pending → notis till VD

Skapar rader i escalation_event (dedup via UNIQUE) och nudge_queue.
Befintlig notification_dispatcher.py hanterar sändning.
"""
import sys
sys.path.insert(0, '/opt/rm-infra')

import logging
from datetime import datetime

from rm_data import query_dicts, execute

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('checklist_escalation')

VD_EMAIL = 'daniel.lopez@rmef.se'

# Eskaleringsnivåer: (nivå, timme, kanal)
LEVELS = [
    (1, 11, 'dashboard'),
    (2, 14, 'email'),
    (3, 16, 'email'),
]


def get_pending_items(today_str):
    """Hämta alla pending checklista-poster för idag med template-info."""
    return query_dicts(
        """SELECT cl.id AS log_id, cl.template_id, cl.current_escalation_level,
                  rt.title, rt.assignee_email, rt.backup_email,
                  pu.manager_email
           FROM daily_checklist_log cl
           JOIN recurring_template rt ON rt.id = cl.template_id
           LEFT JOIN portal_user pu ON pu.email = rt.assignee_email AND pu.active = true
           WHERE cl.check_date = %s
             AND cl.status = 'pending'""",
        (today_str,)
    )


def resolve_recipient(item, level):
    """Returnera mottagarens email för given eskaleringsnivå."""
    if level == 1:
        return item.get('backup_email')
    elif level == 2:
        return item.get('manager_email')
    elif level == 3:
        return VD_EMAIL
    return None


def create_escalation(item, level, recipient, channel):
    """Skapa escalation_event + nudge_queue-rad. Returnerar True om skapad."""
    log_id = item['log_id']
    template_id = item['template_id']
    title = item['title']
    assignee = item['assignee_email'] or 'okänd'

    # Dedupe: UNIQUE (checklist_log_id, escalation_level)
    existing = query_dicts(
        "SELECT id FROM escalation_event WHERE checklist_log_id = %s AND escalation_level = %s",
        (log_id, level)
    )
    if existing:
        return False

    level_labels = {1: 'backup', 2: 'chef', 3: 'VD'}
    level_label = level_labels.get(level, str(level))

    # Skapa nudge i nudge_queue
    msg = f"Eskalering nivå {level} ({level_label}): \"{title}\" ej utförd av {assignee}"
    dedupe_key = f"checklist_esc|{log_id}|{level}"

    nudge_id = execute(
        """INSERT INTO nudge_queue (channel, owner, key_activity_id, iso_week, nudge_type, message, status, dedupe_key)
           VALUES (%s, %s, NULL, %s, %s, %s, 'pending', %s)
           ON CONFLICT (dedupe_key) DO NOTHING
           RETURNING id""",
        (channel, recipient, datetime.now().strftime('%G-W%V'), f'checklist_escalation_L{level}', msg, dedupe_key),
        returning=True
    )

    # Skapa escalation_event
    execute(
        """INSERT INTO escalation_event (checklist_log_id, template_id, escalation_level, recipient_email, channel, nudge_id)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT (checklist_log_id, escalation_level) DO NOTHING""",
        (log_id, template_id, level, recipient, channel, nudge_id)
    )

    # Uppdatera current_escalation_level på checklist-posten
    execute(
        "UPDATE daily_checklist_log SET current_escalation_level = %s, escalated_at = NOW() WHERE id = %s AND current_escalation_level < %s",
        (level, log_id, level)
    )

    # Skapa in-app notis direkt (utöver nudge_queue för extern kanal)
    execute(
        """INSERT INTO dashboard_notification (owner, trigger_event, title, body, link, severity)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (recipient,
         f'checklist_escalation_L{level}',
         f"Eskalering: {title}",
         f"Daglig rutin ej utförd av {assignee}. Nivå {level} ({level_label}).",
         '/tasks',
         'warning' if level < 3 else 'critical')
    )

    return True


def sweep():
    now = datetime.now()
    current_hour = now.hour
    today_str = now.strftime('%Y-%m-%d')

    items = get_pending_items(today_str)
    if not items:
        log.info(f"Inga pending checklista-poster för {today_str}")
        return 0

    total_created = 0

    for item in items:
        for level, trigger_hour, channel in LEVELS:
            # Skippa om inte rätt tid ännu
            if current_hour < trigger_hour:
                continue

            # Skippa om redan eskalerat till denna eller högre nivå
            current_level = item.get('current_escalation_level') or 0
            if current_level >= level:
                continue

            recipient = resolve_recipient(item, level)
            if not recipient:
                log.warning(f"Ingen mottagare för log_id={item['log_id']} nivå {level} — skippar")
                continue

            # Skippa om mottagaren är samma som ansvarig (undvik self-escalation)
            if recipient == item.get('assignee_email'):
                continue

            created = create_escalation(item, level, recipient, channel)
            if created:
                total_created += 1
                log.info(f"Eskalering L{level} för \"{item['title']}\" → {recipient}")

    return total_created


if __name__ == '__main__':
    count = sweep()
    log.info(f"Sweep klar: {count} eskaleringshändelser skapade")
