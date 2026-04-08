#!/usr/bin/env python3
"""
push_notify.py — Push notification helper för RM-agenter

Skickar Web Push-notiser till alla registrerade enheter.
Används av cashflow_alert.py och andra agenter.

Kräver: /opt/rm-infra/push-venv/bin/python3 (pywebpush installerad)

Användning:
    from push_notify import send_push
    send_push("Titel", "Brödtext", tag="cashflow", urgency="high")

Eller standalone:
    /opt/rm-infra/push-venv/bin/python3 push_notify.py "Titel" "Brödtext"
"""

import json
import sys
from pathlib import Path
from datetime import datetime

CONFIG_DIR = Path("/opt/rm-infra")
VAPID_CONFIG = CONFIG_DIR / "vapid-config.json"
SUBSCRIPTIONS_FILE = CONFIG_DIR / "push_subscriptions.json"
LOG_PREFIX = "[push_notify]"


def log(msg):
    print(f"{LOG_PREFIX} {datetime.now().isoformat()} {msg}")


def load_subscriptions():
    """Ladda alla push-prenumerationer."""
    if not SUBSCRIPTIONS_FILE.exists():
        return []
    try:
        data = json.loads(SUBSCRIPTIONS_FILE.read_text())
        if isinstance(data, list):
            return data
        return data.get("subscriptions", [])
    except Exception as e:
        log(f"ERROR loading subscriptions: {e}")
        return []


def load_vapid():
    """Ladda VAPID-nycklar."""
    if not VAPID_CONFIG.exists():
        log("ERROR: VAPID config saknas")
        return None, None, None
    config = json.loads(VAPID_CONFIG.read_text())
    return (
        config.get("private_key_pem"),
        config.get("public_key"),
        config.get("subject", "mailto:daniel@boenosverige.se")
    )


def send_push(title, body, tag=None, urgency="normal", url=None, icon=None):
    """
    Skicka push-notis till alla registrerade enheter.

    Args:
        title: Notistitel
        body: Notisbrödtext
        tag: Grupperings-tag (t.ex. "cashflow", "crm")
        urgency: "very-low", "low", "normal", "high"
        url: URL att öppna vid klick
        icon: Ikon-URL

    Returns:
        dict med sent/failed/total
    """
    try:
        from pywebpush import webpush, WebPushException
    except ImportError:
        log("ERROR: pywebpush ej installerad. Kör: /opt/rm-infra/push-venv/bin/pip install pywebpush")
        return {"sent": 0, "failed": 0, "total": 0, "error": "pywebpush not installed"}

    subscriptions = load_subscriptions()
    if not subscriptions:
        log("Inga push-prenumerationer registrerade")
        return {"sent": 0, "failed": 0, "total": 0}

    private_key_pem, public_key, subject = load_vapid()
    if not private_key_pem:
        return {"sent": 0, "failed": 0, "total": 0, "error": "VAPID config missing"}

    payload = {
        "title": title,
        "body": body,
        "tag": tag or "rm-notification",
        "url": url or "https://dashboard.rmef.se",
        "icon": icon or "/icon-192.png",
        "timestamp": datetime.now().isoformat(),
    }

    sent = 0
    failed = 0
    expired = []

    for i, sub in enumerate(subscriptions):
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps(payload),
                vapid_private_key=private_key_pem,
                vapid_claims={"sub": subject},
                headers={"Urgency": urgency},
                timeout=10
            )
            sent += 1
            log(f"Push sent to subscription {i}")
        except WebPushException as e:
            # 410 Gone eller 404 = prenumeration utgången
            if hasattr(e, 'response') and e.response and e.response.status_code in (404, 410):
                expired.append(i)
                log(f"Subscription {i} expired, removing")
            else:
                failed += 1
                log(f"Push failed for subscription {i}: {e}")
        except Exception as e:
            failed += 1
            log(f"Push error for subscription {i}: {e}")

    # Ta bort utgångna prenumerationer
    if expired:
        remaining = [s for j, s in enumerate(subscriptions) if j not in expired]
        try:
            SUBSCRIPTIONS_FILE.write_text(json.dumps(remaining, indent=2))
            log(f"Removed {len(expired)} expired subscriptions")
        except Exception as e:
            log(f"ERROR removing expired subs: {e}")

    result = {"sent": sent, "failed": failed, "total": len(subscriptions)}
    log(f"Push complete: {sent} sent, {failed} failed, {len(expired)} expired")
    return result


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: push_notify.py <title> <body> [tag] [urgency]")
        sys.exit(1)

    title = sys.argv[1]
    body = sys.argv[2]
    tag = sys.argv[3] if len(sys.argv) > 3 else None
    urgency = sys.argv[4] if len(sys.argv) > 4 else "normal"

    result = send_push(title, body, tag=tag, urgency=urgency)
    print(json.dumps(result, indent=2))
