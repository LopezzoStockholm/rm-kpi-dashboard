#!/usr/bin/env python3
"""
wa_template_sync.py — synkar whatsapp_template-tabellen mot Meta Graph.

Körs via cron var 15e minut. Upsertar templates fran WABA och uppdaterar status.
"""
import json, sys, requests
sys.path.insert(0, "/opt/rm-infra")
from rm_data import execute, query_dicts

WABA_ID = "1274230297375767"

def main():
    with open("/opt/rm-infra/whatsapp-config.json") as f:
        cfg = json.load(f)
    token = cfg["access_token"]

    url = f"https://graph.facebook.com/v20.0/{WABA_ID}/message_templates"
    params = {"fields": "name,status,language,category,components", "limit": 100}
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                     params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])

    seen = []
    for t in data:
        # Rakna parametrar i BODY
        param_count = 0
        for c in t.get("components", []):
            if c.get("type") == "BODY":
                txt = c.get("text", "")
                # {{1}}, {{2}}, ...
                import re
                nums = re.findall(r"\{\{(\d+)\}\}", txt)
                if nums:
                    param_count = max(int(n) for n in nums)
        execute("""
            INSERT INTO whatsapp_template
              (name, language, category, status, param_count, updated_at)
            VALUES (%s,%s,%s,%s,%s, now())
            ON CONFLICT (name) DO UPDATE
              SET status=EXCLUDED.status,
                  category=EXCLUDED.category,
                  language=EXCLUDED.language,
                  param_count=EXCLUDED.param_count,
                  updated_at=now()
        """, (t["name"], t["language"], t["category"], t["status"], param_count))
        seen.append(f"{t['name']} [{t['status']}]")

    print(f"Synced {len(seen)} templates: {', '.join(seen)}")

if __name__ == "__main__":
    main()
