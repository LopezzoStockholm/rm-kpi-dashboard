#!/usr/bin/env python3
"""Deal scoring — beräknar prioriteringspoäng för varje deal i pipeline.
Körs av generate_dashboard.py, sparar till rm_central.deal_score.

Modell v2 (2026-03-31):
  Värde       0-30p  — relativt max-värde i pipeline
  Stage       0-25p  — var i säljprocessen
  Affärstyp   0-20p  — kvalitet på lead (upp från 15p, marginal borttagen)
  Datahygien  0-25p  — completeness (upp från 15p)
  TOTALT      100p
"""

import subprocess, json, sys
from datetime import datetime

def psql(sql, db='rm_central'):
    r = subprocess.run(['docker', 'exec', '-i', 'rm-postgres', 'psql', '-U', 'rmadmin', '-d', db, '-t', '-A'],
        input=sql, capture_output=True, text=True)
    if r.stderr and 'ERROR' in r.stderr:
        print(f"SQL ERROR: {r.stderr.strip()}", file=sys.stderr)
        return None
    return r.stdout.strip()

# 1. Skapa tabell om den inte finns
psql("""
CREATE TABLE IF NOT EXISTS deal_score (
    twenty_id text PRIMARY KEY,
    deal_name text,
    score integer,
    score_value integer,
    score_stage integer,
    score_type integer,
    score_hygiene integer,
    score_margin integer,
    action text,
    updated_at timestamp DEFAULT now()
);
""")

# 2. Hämta alla deals
raw = psql("""
SELECT json_agg(row_to_json(t)) FROM (
    SELECT twenty_id, name, stage, deal_type, estimated_value,
           customer_name, lead_source, contract_type, region,
           margin, probability, next_project_no
    FROM pipeline_deal 
    WHERE company_code='RM' 
    AND stage NOT IN ('fakturerat','forlorad')
) t
""")

deals = json.loads(raw) if raw else []
if not deals:
    print("Inga deals att scora")
    sys.exit(0)

# 3. Beräkna max-värde för normalisering
max_value = max(d['estimated_value'] or 0 for d in deals) or 1

# 4. Scoring
scored = []
for d in deals:
    val = d['estimated_value'] or 0
    stage = (d['stage'] or '').lower()
    dtype = (d['deal_type'] or '').lower()

    # Värde (0-30p) — relativt dyraste dealen
    s_value = round((val / max_value) * 30) if max_value > 0 else 0

    # Stage (0-25p) — säljmognad
    stage_scores = {
        'forhandling':   25,
        'kontrakterat':  22,
        'leverans':      20,
        'offert_skickad': 15,
        'kalkyl':         5,
        'inkommit':       2,
        'forlorad':       0,
    }
    s_stage = stage_scores.get(stage, 0)

    # Affärstyp (0-20p, upp från 15p)
    type_scores = {'styrd': 20, 'intern': 16, 'varm': 12, 'service': 6, 'kall': 3}
    s_type = type_scores.get(dtype, 0)

    # Datahygien (0-25p, upp från 15p)
    # Max penalty = -25 om allt saknas
    hygiene_penalty = 0
    if not d.get('customer_name'):                                              hygiene_penalty -= 8
    if not d.get('lead_source'):                                                hygiene_penalty -= 7
    if not d.get('contract_type') and stage in ('kontrakterat', 'leverans'):   hygiene_penalty -= 5
    if val == 0 and stage in ('forhandling', 'kontrakterat', 'leverans'):       hygiene_penalty -= 7
    s_hygiene = max(0, 25 + hygiene_penalty)

    # Marginal — borttaget som inmatningsfält, alltid 0
    s_margin = 0

    total = s_value + s_stage + s_type + s_hygiene

    # Generera action-rekommendationer
    actions = []
    if val == 0 and stage in ('forhandling', 'kontrakterat', 'leverans'):
        actions.append('Ange uppskattat värde')
    if not d.get('customer_name'):
        actions.append('Koppla företag')
    if not d.get('lead_source'):
        actions.append('Ange leadkälla')
    if not d.get('contract_type') and stage in ('kontrakterat', 'leverans'):
        actions.append('Ange avtalstyp')
    if stage == 'kalkyl' and val > 0:
        actions.append('Skicka offert eller flytta framåt')

    action = '; '.join(actions) if actions else ''

    scored.append({
        'twenty_id': d['twenty_id'],
        'name': d['name'],
        'score': total,
        's_value': s_value,
        's_stage': s_stage,
        's_type': s_type,
        's_hygiene': s_hygiene,
        's_margin': s_margin,
        'action': action
    })

# 5. Uppdatera databas
psql("DELETE FROM deal_score;")

for s in scored:
    name_escaped = s['name'].replace("'", "''")
    action_escaped = s['action'].replace("'", "''")
    psql(f"""
    INSERT INTO deal_score (twenty_id, deal_name, score, score_value, score_stage, score_type, score_hygiene, score_margin, action, updated_at)
    VALUES ('{s['twenty_id']}', '{name_escaped}', {s['score']}, {s['s_value']}, {s['s_stage']}, {s['s_type']}, {s['s_hygiene']}, {s['s_margin']}, '{action_escaped}', now());
    """)

# 6. Output
scored.sort(key=lambda x: x['score'], reverse=True)
print(f"Scorat {len(scored)} deals. Top 10:")
for s in scored[:10]:
    print(f"  {s['score']:3d}p  {s['name'][:35]:35s}  {s['action'][:50]}")
