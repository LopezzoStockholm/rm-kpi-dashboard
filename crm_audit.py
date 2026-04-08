#!/usr/bin/env python3
"""
CRM Audit Agent v2 — körs dagligen via cron.
Kontrollerar datahygien, stage-gates (inkl nya fält), inaktiva deals.
Skriver rapport till rm_central.crm_audit.
"""
import subprocess, json, sys
from datetime import datetime, timedelta

WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"

def q(sql, db="twenty"):
    r = subprocess.run(
        ["docker","exec","rm-postgres","psql","-U","rmadmin","-d",db,"-t","-A","-F","\t","-c",sql],
        capture_output=True, text=True)
    return r.stdout.strip()

def run(sql, db="twenty"):
    subprocess.run(
        ["docker","exec","rm-postgres","psql","-U","rmadmin","-d",db,"-c",sql],
        capture_output=True, text=True)

now = datetime.now()
print(f"=== CRM AUDIT v2 — {now.strftime('%Y-%m-%d %H:%M')} ===\n")

issues = []

# 1. DATAHYGIEN
print("1. DATAHYGIEN")

no_type = q(f'SELECT name, stage FROM {WS}.opportunity WHERE "deletedAt" IS NULL AND affarstyp IS NULL')
if no_type:
    for line in no_type.split("\n"):
        if line:
            parts = line.split("\t")
            issues.append({"type": "DATAHYGIEN", "severity": "HIGH", "deal": parts[0], "msg": f"Saknar affarstyp (stage: {parts[1]})"})
    print(f"  X {len([l for l in no_type.split(chr(10)) if l])} deals utan affarstyp")
else:
    print("  OK Alla deals har affarstyp")

no_co = q(f'SELECT name, stage FROM {WS}.opportunity WHERE "deletedAt" IS NULL AND "companyId" IS NULL')
if no_co:
    for line in no_co.split("\n"):
        if line:
            parts = line.split("\t")
            issues.append({"type": "DATAHYGIEN", "severity": "MEDIUM", "deal": parts[0], "msg": f"Saknar foretag (stage: {parts[1]})"})
    print(f"  X {len([l for l in no_co.split(chr(10)) if l])} deals utan foretag")
else:
    print("  OK Alla deals har foretag")

no_val = q(f"""SELECT name, stage FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL
    AND stage IN ('FORHANDLING','KONTRAKTERAT','LEVERANS','OFFERT_SKICKAD')
    AND ("uppskattatVardeAmountMicros" IS NULL OR "uppskattatVardeAmountMicros" = 0)""")
if no_val:
    for line in no_val.split("\n"):
        if line:
            parts = line.split("\t")
            issues.append({"type": "STAGE_GATE", "severity": "HIGH", "deal": parts[0], "msg": f"Stage {parts[1]} utan uppskattat varde"})
    print(f"  X {len([l for l in no_val.split(chr(10)) if l])} deals i forhandling+ utan varde")
else:
    print("  OK Alla deals i forhandling+ har varde")

no_leadsrc = q(f"""SELECT COUNT(*) FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND ("leadSource" IS NULL OR "leadSource" = '')""")
if no_leadsrc and no_leadsrc.strip() != '0':
    issues.append({"type": "DATAHYGIEN", "severity": "LOW", "deal": f"{no_leadsrc.strip()} deals", "msg": "Saknar leadkalla"})
    print(f"  ! {no_leadsrc.strip()} deals utan leadkalla")
else:
    print("  OK Alla deals har leadkalla")

# 2. STAGE-GATES
print("\n2. STAGE-GATES")

no_contact_kontrakt = q(f"""SELECT name FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage IN ('KONTRAKTERAT','LEVERANS')
    AND "pointOfContactId" IS NULL""")
if no_contact_kontrakt:
    for line in no_contact_kontrakt.split("\n"):
        if line:
            issues.append({"type": "STAGE_GATE", "severity": "HIGH", "deal": line, "msg": "Kontrakterat utan kontaktperson"})
    print(f"  X {len([l for l in no_contact_kontrakt.split(chr(10)) if l])} kontrakterade deals utan kontakt")
else:
    print("  OK Alla kontrakterade deals har kontaktperson")

no_contact_forh = q(f"""SELECT name FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage = 'FORHANDLING'
    AND "pointOfContactId" IS NULL""")
if no_contact_forh:
    count = len([l for l in no_contact_forh.split("\n") if l])
    print(f"  ! {count} deals i forhandling utan kontakt")
    for line in no_contact_forh.split("\n"):
        if line:
            issues.append({"type": "STAGE_GATE", "severity": "MEDIUM", "deal": line, "msg": "Forhandling utan kontaktperson"})
else:
    print("  OK Alla deals i forhandling har kontaktperson")

no_contact_offert = q(f"""SELECT name FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage = 'OFFERT_SKICKAD'
    AND "pointOfContactId" IS NULL""")
if no_contact_offert:
    for line in no_contact_offert.split("\n"):
        if line:
            issues.append({"type": "STAGE_GATE", "severity": "MEDIUM", "deal": line, "msg": "Offert skickad utan kontaktperson"})
    print(f"  ! {len([l for l in no_contact_offert.split(chr(10)) if l])} offert-deals utan kontakt")
else:
    print("  OK Alla offert-deals har kontaktperson")

overdue_close = q(f"""SELECT name, stage, "closeDate"::date FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND "closeDate" < NOW()
    AND stage NOT IN ('LEVERANS','FAKTURERAT')""")
if overdue_close:
    count = len([l for l in overdue_close.split("\n") if l])
    print(f"  ! {count} deals med passerad closeDate")
    for line in overdue_close.split("\n"):
        if line:
            parts = line.split("\t")
            issues.append({"type": "STAGE_GATE", "severity": "MEDIUM", "deal": parts[0], "msg": f"CloseDate {parts[2]} passerad, stage {parts[1]}"})
else:
    print("  OK Inga deals med passerad closeDate")

no_next = q(f"""SELECT name, stage FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage IN ('KONTRAKTERAT','LEVERANS')
    AND ("nextProjectNo" IS NULL OR "nextProjectNo" = '')""")
if no_next:
    for line in no_next.split("\n"):
        if line:
            parts = line.split("\t")
            issues.append({"type": "STAGE_GATE", "severity": "HIGH", "deal": parts[0], "msg": "Kontrakterat utan Next-projektnummer"})
    print(f"  X {len([l for l in no_next.split(chr(10)) if l])} kontrakterade deals utan Next-projektnr")
else:
    print("  OK Alla kontrakterade deals har Next-projektnr")

no_ctype = q(f"""SELECT name, stage FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage IN ('KONTRAKTERAT','LEVERANS','FAKTURERAT')
    AND ("contractType" IS NULL OR "contractType" = '')""")
if no_ctype:
    for line in no_ctype.split("\n"):
        if line:
            parts = line.split("\t")
            issues.append({"type": "STAGE_GATE", "severity": "MEDIUM", "deal": parts[0], "msg": f"Stage {parts[1]} utan avtalstyp"})
    print(f"  ! {len([l for l in no_ctype.split(chr(10)) if l])} kontrakterade+ deals utan avtalstyp")
else:
    print("  OK Alla kontrakterade+ deals har avtalstyp")

# 3. INAKTIVITET
print("\n3. INAKTIVITET")

stale_14 = q(f"""SELECT name, stage, "updatedAt"::date FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage NOT IN ('KALKYL','FAKTURERAT')
    AND "updatedAt" < NOW() - INTERVAL '14 days'
    ORDER BY "updatedAt" """)
if stale_14:
    count = len([l for l in stale_14.split("\n") if l])
    print(f"  ! {count} deals utan uppdatering pa 14+ dagar:")
    for line in stale_14.split("\n"):
        if line:
            parts = line.split("\t")
            days = (now - datetime.strptime(parts[2], "%Y-%m-%d")).days
            issues.append({"type": "INAKTIV", "severity": "MEDIUM", "deal": parts[0], "msg": f"{days} dagar utan uppdatering (stage: {parts[1]})"})
            print(f"    {parts[0]:35} {parts[1]:16} {days}d sedan")
else:
    print("  OK Alla aktiva deals uppdaterade senaste 14 dagarna")

stale_kalkyl = q(f"""SELECT name, "updatedAt"::date FROM {WS}.opportunity
    WHERE "deletedAt" IS NULL AND stage = 'KALKYL'
    AND "updatedAt" < NOW() - INTERVAL '30 days'
    ORDER BY "updatedAt" """)
if stale_kalkyl:
    count = len([l for l in stale_kalkyl.split("\n") if l])
    print(f"  ! {count} kalkyl-deals utan uppdatering pa 30+ dagar")
    for line in stale_kalkyl.split("\n"):
        if line:
            parts = line.split("\t")
            days = (now - datetime.strptime(parts[1], "%Y-%m-%d")).days
            issues.append({"type": "INAKTIV", "severity": "LOW", "deal": parts[0], "msg": f"Kalkyl inaktiv {days}d"})
else:
    print("  OK Alla kalkyl-deals aktiva senaste 30 dagarna")

# 4. SAMMANFATTNING
high = len([i for i in issues if i["severity"] == "HIGH"])
medium = len([i for i in issues if i["severity"] == "MEDIUM"])
low = len([i for i in issues if i["severity"] == "LOW"])
total = len(issues)

print(f"\n{'='*50}")
print(f"  SAMMANFATTNING: {total} issues")
print(f"  HIGH: {high} | MEDIUM: {medium} | LOW: {low}")
print(f"{'='*50}")

audit_json = json.dumps({"timestamp": now.isoformat(), "issues": issues, "summary": {"total": total, "high": high, "medium": medium, "low": low}})

run("""CREATE TABLE IF NOT EXISTS crm_audit (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    total_issues INT,
    high_issues INT,
    medium_issues INT,
    low_issues INT,
    details JSONB
)""", db="rm_central")

run(f"INSERT INTO crm_audit (total_issues, high_issues, medium_issues, low_issues, details) VALUES ({total}, {high}, {medium}, {low}, '{audit_json.replace(chr(39), chr(39)+chr(39))}')", db="rm_central")

print(f"\nAudit sparad i rm_central.crm_audit")
