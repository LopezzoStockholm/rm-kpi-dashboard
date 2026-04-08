#!/usr/bin/env python3
"""Deal Tracker v4 — loggar stage-changes, won och lost deals."""
import subprocess, os
from datetime import datetime

WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"

def q(sql, db="rm_central"):
    tmp = "/tmp/_dt_q.sql"
    with open(tmp, "w") as f:
        f.write(sql)
    r = subprocess.run(
        f"cat {tmp} | docker exec -i rm-postgres psql -U rmadmin -d {db} -t -A -F '\t'",
        shell=True, capture_output=True, text=True)
    return r.stdout.strip()

def run(sql, db="rm_central"):
    tmp = "/tmp/_dt_r.sql"
    with open(tmp, "w") as f:
        f.write(sql)
    r = subprocess.run(
        f"cat {tmp} | docker exec -i rm-postgres psql -U rmadmin -d {db}",
        shell=True, capture_output=True, text=True)
    if r.returncode != 0 and 'ERROR' in r.stderr:
        print(f"SQL ERROR: {r.stderr[:200]}")

now = datetime.now()
print(f"=== DEAL TRACKER v4 — {now.strftime('%Y-%m-%d %H:%M')} ===")

run("""CREATE TABLE IF NOT EXISTS deal_snapshot (
    twenty_id TEXT PRIMARY KEY,
    deal_name TEXT, stage TEXT, deal_type TEXT,
    estimated_value NUMERIC(14,2), company_name TEXT,
    lead_source TEXT, contract_type TEXT, region TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);""")

# Use COALESCE with NULL-safe cast for enum fields
current_sql = f"""SELECT o.id, o.name, o.stage,
    COALESCE(o.affarstyp::text, ''),
    COALESCE(o."uppskattatVardeAmountMicros",0)/1000000,
    COALESCE(c.name, ''),
    COALESCE(o."leadSource"::text, ''),
    COALESCE(o."contractType"::text, ''),
    COALESCE(o.region::text, '')
FROM {WS}.opportunity o
LEFT JOIN {WS}.company c ON o."companyId"=c.id
WHERE o."deletedAt" IS NULL;"""

current_raw = q(current_sql, db="twenty")

current = {}
for line in current_raw.split("\n"):
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) >= 5:
        current[parts[0]] = {
            "name": parts[1], "stage": parts[2],
            "type": parts[3] if len(parts) > 3 else "",
            "value": float(parts[4]) if len(parts) > 4 else 0,
            "company": parts[5] if len(parts) > 5 else "",
            "lead_source": parts[6] if len(parts) > 6 else "",
            "contract_type": parts[7] if len(parts) > 7 else "",
            "region": parts[8] if len(parts) > 8 else ""
        }

print(f"  {len(current)} deals from Twenty")

prev_raw = q("SELECT twenty_id, deal_name, stage, deal_type, estimated_value, company_name FROM deal_snapshot;")
prev = {}
for line in prev_raw.split("\n"):
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) >= 6:
        prev[parts[0]] = {"name": parts[1], "stage": parts[2], "type": parts[3],
                          "value": float(parts[4]), "company": parts[5]}

print(f"  {len(prev)} in previous snapshot")

changes = 0
for tid, cur in current.items():
    if tid in prev:
        old = prev[tid]
        if old["stage"] != cur["stage"]:
            event = "STAGE_CHANGE"
            if cur["stage"] in ["KONTRAKTERAT","LEVERANS","FAKTURERAT"] and old["stage"] in ["INKOMMIT","KALKYL","OFFERT_SKICKAD","FORHANDLING"]:
                event = "WON"
            dn = cur["name"].replace("'", "''")
            cn = cur["company"].replace("'", "''")
            run(f"""INSERT INTO deal_history (twenty_id, deal_name, company_name, deal_type, event_type,
                from_stage, to_stage, estimated_value, lead_source, contract_type, region)
                VALUES ('{tid}', '{dn}', '{cn}', '{cur["type"]}', '{event}',
                '{old["stage"]}', '{cur["stage"]}', {cur["value"]},
                '{cur["lead_source"]}', '{cur["contract_type"]}', '{cur["region"]}');""")
            changes += 1
            print(f"  {event}: {cur['name']} ({old['stage']} -> {cur['stage']})")

for tid, old in prev.items():
    if tid not in current:
        dn = old["name"].replace("'", "''")
        cn = old["company"].replace("'", "''")
        run(f"""INSERT INTO deal_history (twenty_id, deal_name, company_name, deal_type, event_type,
            from_stage, to_stage, estimated_value) VALUES ('{tid}', '{dn}', '{cn}', '{old["type"]}',
            'LOST', '{old["stage"]}', 'DELETED', {old["value"]});""")
        changes += 1
        print(f"  LOST: {old['name']}")

# Update snapshot
run("DELETE FROM deal_snapshot;")
inserts = []
for tid, cur in current.items():
    dn = cur["name"].replace("'", "''")
    cn = cur["company"].replace("'", "''")
    inserts.append(f"('{tid}','{dn}','{cur['stage']}','{cur['type']}',{cur['value']},'{cn}','{cur['lead_source']}','{cur['contract_type']}','{cur['region']}')")

if inserts:
    batch = "INSERT INTO deal_snapshot (twenty_id, deal_name, stage, deal_type, estimated_value, company_name, lead_source, contract_type, region) VALUES " + ",\n".join(inserts) + ";"
    run(batch)

print(f"\n{changes} changes, {len(current)} deals in snapshot")
