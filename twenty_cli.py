#!/usr/bin/env python3
"""
Twenty CRM CLI — Direkt databasåtkomst till Twenty CRM
Används av Claude som operativt verktyg för RM Entreprenad och Fasad.

Workspace: workspace_13e0qz9uia3v9w5dx0mk6etm5
DB: twenty (PostgreSQL via Docker rm-postgres, user rmadmin)

Kommandon:
  deals           Lista alla deals (opportunities)
  deal <id>       Visa en specifik deal med detaljer
  deal-update <id> <fält> <värde>  Uppdatera ett fält på en deal
  deal-create <json>  Skapa ny deal
  deal-stage <id> <stage>  Ändra stage på en deal

  companies       Lista alla bolag
  company <id>    Visa ett specifikt bolag
  company-create <json>  Skapa nytt bolag

  contacts        Lista alla kontakter
  contact <id>    Visa en specifik kontakt
  contact-create <json>  Skapa ny kontakt

  tasks           Lista alla tasks
  task <id>       Visa en specifik task
  task-create <json>  Skapa ny task
  task-done <id>  Markera task som klar

  notes           Lista senaste noteringar
  note-create <json>  Skapa ny notering

  search <term>   Sök i deals, bolag och kontakter
  stats           Visa pipeline-statistik
"""

import sys
import json
import subprocess
import uuid
from datetime import datetime

WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"
OWNER_ID = "7b4db90c-59cf-45b4-a0bd-c73465a7afad"  # Daniel Lopez

STAGES = ["INKOMMIT", "KALKYL", "OFFERT_SKICKAD", "FORHANDLING", "KONTRAKTERAT", "LEVERANS", "FAKTURERAT"]
DEAL_TYPES = ["INTERN", "VARM", "STYRD", "SERVICE", "KALL"]

def psql(query, db="twenty"):
    """Kör SQL mot Twenty-databasen."""
    cmd = [
        "docker", "exec", "rm-postgres",
        "psql", "-U", "rmadmin", "-d", db,
        "-t", "-A", "-F", "|", "-c", query
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"DB-fel: {r.stderr.strip()}", file=sys.stderr)
        return []
    rows = [line for line in r.stdout.strip().split("\n") if line]
    return rows

def psql_exec(query, db="twenty"):
    """Kör SQL utan resultat (INSERT/UPDATE)."""
    cmd = [
        "docker", "exec", "rm-postgres",
        "psql", "-U", "rmadmin", "-d", db, "-c", query
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"DB-fel: {r.stderr.strip()}", file=sys.stderr)
        return False
    return True

def esc(val):
    """Escape SQL-sträng."""
    if val is None:
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def fmt_amount(micros):
    """Formatera belopp från micros till SEK."""
    if not micros or micros == "":
        return "0 kr"
    v = int(float(micros)) / 1_000_000
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f} Mkr"
    elif v >= 1_000:
        return f"{v/1_000:.0f} tkr"
    else:
        return f"{v:.0f} kr"

# === DEALS ===

def cmd_deals(args):
    """Lista alla aktiva deals."""
    stage_filter = ""
    type_filter = ""
    for i, a in enumerate(args):
        if a == "--stage" and i+1 < len(args):
            stage_filter = f"AND stage = '{args[i+1].upper()}'"
        if a == "--type" and i+1 < len(args):
            type_filter = f"AND affarstyp = '{args[i+1].upper()}'"

    q = f'''SELECT o.id, o.name, o.stage::text, COALESCE(o.affarstyp::text,''),
            COALESCE(o."uppskattatVardeAmountMicros",0),
            COALESCE(c.name,'—'),
            o."createdAt"::date
            FROM {WS}.opportunity o
            LEFT JOIN {WS}.company c ON o."companyId" = c.id
            WHERE o."deletedAt" IS NULL {stage_filter} {type_filter}
            ORDER BY COALESCE(o."uppskattatVardeAmountMicros",0) DESC'''
    rows = psql(q)

    print(f"{'Namn':<45} {'Steg':<16} {'Typ':<10} {'Värde':>12} {'Bolag':<25}")
    print("-" * 115)
    for row in rows:
        parts = row.split("|")
        if len(parts) >= 7:
            name = parts[1][:43]
            stage = parts[2]
            dtype = parts[3]
            value = fmt_amount(parts[4])
            company = parts[5][:23]
            print(f"{name:<45} {stage:<16} {dtype:<10} {value:>12} {company:<25}")
    print(f"\nTotalt: {len(rows)} deals")

def cmd_deal(args):
    """Visa en specifik deal."""
    if not args:
        print("Ange deal-id eller sökterm")
        return

    term = args[0]
    if len(term) < 36:
        # Sök på namn
        q = f'''SELECT o.id, o.name, o.stage::text, COALESCE(o.affarstyp::text,''),
                COALESCE(o."uppskattatVardeAmountMicros",0),
                COALESCE(o."kalkyleratVardeAmountMicros",0),
                COALESCE(o."amountAmountMicros",0),
                o."closeDate", o."createdAt",
                COALESCE(c.name,'—'), c.id,
                COALESCE(p."nameFirstName" || ' ' || p."nameLastName", '—')
                FROM {WS}.opportunity o
                LEFT JOIN {WS}.company c ON o."companyId" = c.id
                LEFT JOIN {WS}.person p ON o."pointOfContactId" = p.id
                WHERE o."deletedAt" IS NULL AND o.name ILIKE '%{term.replace("'","''")}%'
                LIMIT 5'''
    else:
        q = f'''SELECT o.id, o.name, o.stage::text, COALESCE(o.affarstyp::text,''),
                COALESCE(o."uppskattatVardeAmountMicros",0),
                COALESCE(o."kalkyleratVardeAmountMicros",0),
                COALESCE(o."amountAmountMicros",0),
                o."closeDate", o."createdAt",
                COALESCE(c.name,'—'), c.id,
                COALESCE(p."nameFirstName" || ' ' || p."nameLastName", '—')
                FROM {WS}.opportunity o
                LEFT JOIN {WS}.company c ON o."companyId" = c.id
                LEFT JOIN {WS}.person p ON o."pointOfContactId" = p.id
                WHERE o.id = '{term}' '''

    rows = psql(q)
    for row in rows:
        p = row.split("|")
        if len(p) >= 12:
            print(f"ID:            {p[0]}")
            print(f"Namn:          {p[1]}")
            print(f"Steg:          {p[2]}")
            print(f"Affärstyp:     {p[3]}")
            print(f"Uppskattat:    {fmt_amount(p[4])}")
            print(f"Kalkylerat:    {fmt_amount(p[5])}")
            print(f"Belopp:        {fmt_amount(p[6])}")
            print(f"Stängningsdatum: {p[7] if p[7] else '—'}")
            print(f"Skapad:        {p[8]}")
            print(f"Bolag:         {p[9]}")
            print(f"Kontakt:       {p[11]}")

            # Hämta tasks
            tasks = psql(f'''SELECT t.title, t.status, t."dueAt"::date
                            FROM {WS}.task t
                            JOIN {WS}."taskTarget" tt ON tt."taskId" = t.id
                            WHERE tt."targetOpportunityId" = '{p[0]}'
                            AND t."deletedAt" IS NULL
                            ORDER BY t."dueAt" NULLS LAST''')
            if tasks:
                print(f"\nUppgifter ({len(tasks)}):")
                for t in tasks:
                    tp = t.split("|")
                    print(f"  [{tp[1]}] {tp[0]}  {tp[2] if tp[2] else ''}")

            # Hämta noteringar
            notes = psql(f'''SELECT n.title, n."bodyV2Markdown", n."createdAt"::date
                            FROM {WS}.note n
                            JOIN {WS}."noteTarget" nt ON nt."noteId" = n.id
                            WHERE nt."targetOpportunityId" = '{p[0]}'
                            AND n."deletedAt" IS NULL
                            ORDER BY n."createdAt" DESC LIMIT 5''')
            if notes:
                print(f"\nNoteringar ({len(notes)}):")
                for n in notes:
                    np = n.split("|")
                    body = (np[1] or "")[:100]
                    print(f"  [{np[2]}] {np[0] or '—'}: {body}")
            print()

def cmd_deal_update(args):
    """Uppdatera fält på en deal. Syntax: deal-update <id> <fält> <värde>"""
    if len(args) < 3:
        print("Syntax: deal-update <id> <fält> <värde>")
        return

    deal_id = args[0]
    field = args[1]
    value = " ".join(args[2:])

    field_map = {
        "name": "name",
        "stage": "stage",
        "affarstyp": "affarstyp",
        "uppskattat": '"uppskattatVardeAmountMicros"',
        "kalkylerat": '"kalkyleratVardeAmountMicros"',
        "belopp": '"amountAmountMicros"',
        "closedate": '"closeDate"',
    }

    db_field = field_map.get(field.lower())
    if not db_field:
        print(f"Okänt fält: {field}. Tillgängliga: {', '.join(field_map.keys())}")
        return

    if field.lower() == "stage":
        value = value.upper()
        if value not in STAGES:
            print(f"Ogiltig stage: {value}. Tillgängliga: {', '.join(STAGES)}")
            return
        sql_val = f"'{value}'"
    elif field.lower() == "affarstyp":
        value = value.upper()
        if value not in DEAL_TYPES:
            print(f"Ogiltig affärstyp: {value}. Tillgängliga: {', '.join(DEAL_TYPES)}")
            return
        sql_val = f"'{value}'"
    elif field.lower() in ("uppskattat", "kalkylerat", "belopp"):
        # Konvertera SEK till micros
        try:
            micros = int(float(value) * 1_000_000)
            sql_val = str(micros)
        except ValueError:
            print(f"Ogiltigt belopp: {value}")
            return
    elif field.lower() == "closedate":
        sql_val = f"'{value}'"
    else:
        sql_val = esc(value)

    q = f'''UPDATE {WS}.opportunity
            SET {db_field} = {sql_val}, "updatedAt" = NOW()
            WHERE id = '{deal_id}' AND "deletedAt" IS NULL'''

    if psql_exec(q):
        print(f"Uppdaterat {field} = {value} på deal {deal_id}")
    else:
        print("Uppdatering misslyckades")

def cmd_deal_stage(args):
    """Ändra stage på deal."""
    if len(args) < 2:
        print("Syntax: deal-stage <id> <stage>")
        print(f"Tillgängliga: {', '.join(STAGES)}")
        return
    cmd_deal_update([args[0], "stage", args[1]])

def cmd_deal_create(args):
    """Skapa ny deal. Syntax: deal-create '{"name":"X","stage":"INKOMMIT","affarstyp":"KALL","uppskattat":500000,"company_id":"uuid"}'"""
    if not args:
        print('Syntax: deal-create \'{"name":"X","stage":"INKOMMIT","affarstyp":"KALL","uppskattat":500000}\'')
        return

    try:
        data = json.loads(" ".join(args))
    except json.JSONDecodeError as e:
        print(f"Ogiltigt JSON: {e}")
        return

    new_id = str(uuid.uuid4())
    name = esc(data.get("name", "Ny deal"))
    stage = data.get("stage", "INKOMMIT").upper()
    affarstyp = data.get("affarstyp", "KALL").upper()
    uppskattat = int(float(data.get("uppskattat", 0)) * 1_000_000)
    kalkylerat = int(float(data.get("kalkylerat", 0)) * 1_000_000)
    company_id = data.get("company_id", "NULL")
    contact_id = data.get("contact_id", "NULL")

    company_sql = f"'{company_id}'" if company_id != "NULL" else "NULL"
    contact_sql = f"'{contact_id}'" if contact_id != "NULL" else "NULL"

    q = f'''INSERT INTO {WS}.opportunity
            (id, "createdAt", "updatedAt", name, stage, affarstyp,
             "uppskattatVardeAmountMicros", "uppskattatVardeCurrencyCode",
             "kalkyleratVardeAmountMicros", "kalkyleratVardeCurrencyCode",
             "companyId", "pointOfContactId", "ownerId", position)
            VALUES (
                '{new_id}', NOW(), NOW(), {name}, '{stage}', '{affarstyp}',
                {uppskattat}, 'SEK', {kalkylerat}, 'SEK',
                {company_sql}, {contact_sql}, '{data.get("assigneeId", OWNER_ID)}', 0
            )'''

    if psql_exec(q):
        print(f"Deal skapad: {new_id}")
        print(f"  Namn: {data.get('name')}")
        print(f"  Steg: {stage}")
        print(f"  Typ:  {affarstyp}")
        print(f"  Uppskattat: {fmt_amount(uppskattat)}")
    else:
        print("Kunde inte skapa deal")

# === COMPANIES ===

def cmd_companies(args):
    """Lista alla bolag."""
    q = f'''SELECT c.id, c.name,
            COALESCE(c."addressAddressCity",''),
            COALESCE(c."domainNamePrimaryLinkUrl",''),
            (SELECT COUNT(*) FROM {WS}.opportunity o WHERE o."companyId"=c.id AND o."deletedAt" IS NULL),
            (SELECT COUNT(*) FROM {WS}.person p WHERE p."companyId"=c.id AND p."deletedAt" IS NULL)
            FROM {WS}.company c
            WHERE c."deletedAt" IS NULL
            ORDER BY c.name'''
    rows = psql(q)

    print(f"{'Bolag':<40} {'Stad':<20} {'Deals':>6} {'Kontakter':>10}")
    print("-" * 80)
    for row in rows:
        p = row.split("|")
        if len(p) >= 6:
            print(f"{p[1][:38]:<40} {p[2][:18]:<20} {p[4]:>6} {p[5]:>10}")
    print(f"\nTotalt: {len(rows)} bolag")

def cmd_company(args):
    """Visa bolag med detaljer."""
    if not args:
        print("Ange bolag-id eller sökterm")
        return

    term = args[0]
    if len(term) < 36:
        where = f"c.name ILIKE '%{term.replace(chr(39),chr(39)+chr(39))}%'"
    else:
        where = f"c.id = '{term}'"

    q = f'''SELECT c.id, c.name,
            COALESCE(c."addressAddressStreet1",''), COALESCE(c."addressAddressCity",''),
            COALESCE(c."addressAddressPostcode",''),
            COALESCE(c."domainNamePrimaryLinkUrl",''),
            c."createdAt"::date
            FROM {WS}.company c
            WHERE c."deletedAt" IS NULL AND {where}'''
    rows = psql(q)

    for row in rows:
        p = row.split("|")
        if len(p) >= 7:
            print(f"ID:      {p[0]}")
            print(f"Bolag:   {p[1]}")
            print(f"Adress:  {p[2]}, {p[4]} {p[3]}")
            print(f"Webb:    {p[5] or '—'}")
            print(f"Skapad:  {p[6]}")

            # Kontakter
            contacts = psql(f'''SELECT "nameFirstName", "nameLastName",
                              COALESCE("jobTitle",''), COALESCE("emailsPrimaryEmail",''),
                              COALESCE("phonesPrimaryPhoneNumber",'')
                              FROM {WS}.person
                              WHERE "companyId" = '{p[0]}' AND "deletedAt" IS NULL''')
            if contacts:
                print(f"\nKontakter ({len(contacts)}):")
                for c in contacts:
                    cp = c.split("|")
                    print(f"  {cp[0]} {cp[1]} — {cp[2]} | {cp[3]} | {cp[4]}")

            # Deals
            deals = psql(f'''SELECT name, stage, COALESCE(affarstyp::text,''),
                           COALESCE("uppskattatVardeAmountMicros",0)
                           FROM {WS}.opportunity
                           WHERE "companyId" = '{p[0]}' AND "deletedAt" IS NULL
                           ORDER BY COALESCE("uppskattatVardeAmountMicros",0) DESC''')
            if deals:
                print(f"\nDeals ({len(deals)}):")
                for d in deals:
                    dp = d.split("|")
                    print(f"  {dp[0]:<35} {dp[1]:<16} {dp[2]:<10} {fmt_amount(dp[3]):>12}")
            print()

def cmd_company_create(args):
    """Skapa nytt bolag."""
    if not args:
        print('Syntax: company-create \'{"name":"X","city":"Stockholm","street":"Gatan 1"}\'')
        return
    try:
        data = json.loads(" ".join(args))
    except json.JSONDecodeError as e:
        print(f"Ogiltigt JSON: {e}")
        return

    new_id = str(uuid.uuid4())
    q = f'''INSERT INTO {WS}.company
            (id, "createdAt", "updatedAt", name,
             "addressAddressCity", "addressAddressStreet1", "addressAddressPostcode",
             "domainNamePrimaryLinkUrl", position)
            VALUES (
                '{new_id}', NOW(), NOW(), {esc(data.get('name',''))},
                {esc(data.get('city'))}, {esc(data.get('street'))}, {esc(data.get('postcode'))},
                {esc(data.get('domain'))}, 0
            )'''
    if psql_exec(q):
        print(f"Bolag skapat: {new_id} — {data.get('name')}")
    else:
        print("Kunde inte skapa bolag")

# === CONTACTS ===

def cmd_contacts(args):
    """Lista alla kontakter."""
    q = f'''SELECT p.id, p."nameFirstName", p."nameLastName",
            COALESCE(p."jobTitle",''), COALESCE(p."emailsPrimaryEmail",''),
            COALESCE(p."phonesPrimaryPhoneNumber",''),
            COALESCE(c.name,'—')
            FROM {WS}.person p
            LEFT JOIN {WS}.company c ON p."companyId" = c.id
            WHERE p."deletedAt" IS NULL
            ORDER BY p."nameLastName", p."nameFirstName"'''
    rows = psql(q)

    print(f"{'Namn':<30} {'Titel':<25} {'E-post':<30} {'Bolag':<25}")
    print("-" * 115)
    for row in rows:
        p = row.split("|")
        if len(p) >= 7:
            name = f"{p[1]} {p[2]}"
            print(f"{name[:28]:<30} {p[3][:23]:<25} {p[4][:28]:<30} {p[6][:23]:<25}")
    print(f"\nTotalt: {len(rows)} kontakter")

def cmd_contact(args):
    """Visa kontakt."""
    if not args:
        print("Ange kontakt-id eller sökterm")
        return
    term = args[0]
    if len(term) < 36:
        where = f'''(p."nameFirstName" ILIKE '%{term.replace(chr(39),chr(39)+chr(39))}%'
                     OR p."nameLastName" ILIKE '%{term.replace(chr(39),chr(39)+chr(39))}%')'''
    else:
        where = f"p.id = '{term}'"

    q = f'''SELECT p.id, p."nameFirstName", p."nameLastName",
            COALESCE(p."jobTitle",''), COALESCE(p."emailsPrimaryEmail",''),
            COALESCE(p."phonesPrimaryPhoneNumber",''), COALESCE(p.city,''),
            COALESCE(c.name,'—'), p."createdAt"::date
            FROM {WS}.person p
            LEFT JOIN {WS}.company c ON p."companyId" = c.id
            WHERE p."deletedAt" IS NULL AND {where}'''
    rows = psql(q)
    for row in rows:
        p = row.split("|")
        if len(p) >= 9:
            print(f"ID:      {p[0]}")
            print(f"Namn:    {p[1]} {p[2]}")
            print(f"Titel:   {p[3]}")
            print(f"E-post:  {p[4]}")
            print(f"Telefon: {p[5]}")
            print(f"Stad:    {p[6]}")
            print(f"Bolag:   {p[7]}")
            print(f"Skapad:  {p[8]}")
            print()

def cmd_contact_create(args):
    """Skapa ny kontakt."""
    if not args:
        print('Syntax: contact-create \'{"first":"Kalle","last":"Svensson","email":"kalle@test.se","phone":"07012345","title":"VD","company_id":"uuid"}\'')
        return
    try:
        data = json.loads(" ".join(args))
    except json.JSONDecodeError as e:
        print(f"Ogiltigt JSON: {e}")
        return

    new_id = str(uuid.uuid4())
    company_sql = f"'{data['company_id']}'" if data.get('company_id') else "NULL"

    q = f'''INSERT INTO {WS}.person
            (id, "createdAt", "updatedAt", "nameFirstName", "nameLastName",
             "emailsPrimaryEmail", "phonesPrimaryPhoneNumber", "jobTitle",
             "companyId", city, position)
            VALUES (
                '{new_id}', NOW(), NOW(), {esc(data.get('first',''))}, {esc(data.get('last',''))},
                {esc(data.get('email'))}, {esc(data.get('phone'))}, {esc(data.get('title'))},
                {company_sql}, {esc(data.get('city'))}, 0
            )'''
    if psql_exec(q):
        print(f"Kontakt skapad: {new_id} — {data.get('first','')} {data.get('last','')}")
    else:
        print("Kunde inte skapa kontakt")

# === TASKS ===

def cmd_tasks(args):
    """Lista aktiva tasks."""
    status_filter = "AND t.status != 'DONE'" if "--all" not in args else ""

    q = f'''SELECT t.id, t.title, t.status, t."dueAt"::date,
            COALESCE(
                (SELECT string_agg(COALESCE(o.name, c.name, p."nameFirstName" || ' ' || p."nameLastName"), ', ')
                 FROM {WS}."taskTarget" tt
                 LEFT JOIN {WS}.opportunity o ON tt."targetOpportunityId" = o.id
                 LEFT JOIN {WS}.company c ON tt."targetCompanyId" = c.id
                 LEFT JOIN {WS}.person p ON tt."targetPersonId" = p.id
                 WHERE tt."taskId" = t.id AND tt."deletedAt" IS NULL
                ), '—'
            )
            FROM {WS}.task t
            WHERE t."deletedAt" IS NULL {status_filter}
            ORDER BY t."dueAt" NULLS LAST, t."createdAt" DESC'''
    rows = psql(q)

    print(f"{'Status':<8} {'Förfaller':<12} {'Uppgift':<50} {'Kopplad till':<30}")
    print("-" * 105)
    for row in rows:
        p = row.split("|")
        if len(p) >= 5:
            status = p[2]
            due = p[3] if p[3] else "—"
            print(f"{status:<8} {due:<12} {p[1][:48]:<50} {p[4][:28]:<30}")
    print(f"\nTotalt: {len(rows)} uppgifter")

def cmd_task_create(args):
    """Skapa ny task."""
    if not args:
        print('Syntax: task-create \'{"title":"Gör X","due":"2026-04-01","deal_id":"uuid","company_id":"uuid"}\'')
        return
    try:
        data = json.loads(" ".join(args))
    except json.JSONDecodeError as e:
        print(f"Ogiltigt JSON: {e}")
        return

    task_id = str(uuid.uuid4())
    due_sql = f"'{data['due']}'" if data.get('due') else "NULL"

    q = f'''INSERT INTO {WS}.task
            (id, "createdAt", "updatedAt", title, status, "dueAt", "assigneeId", position)
            VALUES (
                '{task_id}', NOW(), NOW(), {esc(data.get('title',''))}, 'TODO', {due_sql}, '{data.get("assigneeId", OWNER_ID)}', 0
            )'''
    if not psql_exec(q):
        print("Kunde inte skapa task")
        return

    # Koppla till deal/bolag/kontakt
    for target_type, field in [("deal_id", "targetOpportunityId"),
                                ("company_id", "targetCompanyId"),
                                ("contact_id", "targetPersonId")]:
        if data.get(target_type):
            tt_id = str(uuid.uuid4())
            tq = f'''INSERT INTO {WS}."taskTarget"
                     (id, "createdAt", "updatedAt", "taskId", "{field}", position)
                     VALUES ('{tt_id}', NOW(), NOW(), '{task_id}', '{data[target_type]}', 0)'''
            psql_exec(tq)

    print(f"Task skapad: {task_id} — {data.get('title')}")

def cmd_task_done(args):
    """Markera task som klar."""
    if not args:
        print("Ange task-id")
        return
    q = f'''UPDATE {WS}.task SET status = 'DONE', "updatedAt" = NOW()
            WHERE id = '{args[0]}' AND "deletedAt" IS NULL'''
    if psql_exec(q):
        print(f"Task {args[0]} markerad som DONE")

# === NOTES ===

def cmd_notes(args):
    """Lista senaste noteringar."""
    limit = 20
    q = f'''SELECT n.id, COALESCE(n.title,'—'),
            LEFT(COALESCE(n."bodyV2Markdown",''),100),
            n."createdAt"::date,
            COALESCE(
                (SELECT string_agg(COALESCE(o.name, c.name, p."nameFirstName"), ', ')
                 FROM {WS}."noteTarget" nt
                 LEFT JOIN {WS}.opportunity o ON nt."targetOpportunityId" = o.id
                 LEFT JOIN {WS}.company c ON nt."targetCompanyId" = c.id
                 LEFT JOIN {WS}.person p ON nt."targetPersonId" = p.id
                 WHERE nt."noteId" = n.id AND nt."deletedAt" IS NULL
                ), '—'
            )
            FROM {WS}.note n
            WHERE n."deletedAt" IS NULL
            ORDER BY n."createdAt" DESC
            LIMIT {limit}'''
    rows = psql(q)

    for row in rows:
        p = row.split("|")
        if len(p) >= 5:
            print(f"[{p[3]}] {p[1]} → {p[4]}")
            if p[2]:
                print(f"         {p[2][:80]}")

def cmd_note_create(args):
    """Skapa ny notering."""
    if not args:
        print('Syntax: note-create \'{"title":"Anteckning","body":"Texten...","deal_id":"uuid"}\'')
        return
    try:
        data = json.loads(" ".join(args))
    except json.JSONDecodeError as e:
        print(f"Ogiltigt JSON: {e}")
        return

    note_id = str(uuid.uuid4())
    q = f'''INSERT INTO {WS}.note
            (id, "createdAt", "updatedAt", title, "bodyV2Markdown", position)
            VALUES (
                '{note_id}', NOW(), NOW(), {esc(data.get('title',''))},
                {esc(data.get('body',''))}, 0
            )'''
    if not psql_exec(q):
        print("Kunde inte skapa notering")
        return

    for target_type, field in [("deal_id", "targetOpportunityId"),
                                ("company_id", "targetCompanyId"),
                                ("contact_id", "targetPersonId")]:
        if data.get(target_type):
            nt_id = str(uuid.uuid4())
            nq = f'''INSERT INTO {WS}."noteTarget"
                     (id, "createdAt", "updatedAt", "noteId", "{field}", position)
                     VALUES ('{nt_id}', NOW(), NOW(), '{note_id}', '{data[target_type]}', 0)'''
            psql_exec(nq)

    print(f"Notering skapad: {note_id} — {data.get('title','')}")

# === SEARCH ===

def cmd_search(args):
    """Sök i deals, bolag och kontakter."""
    if not args:
        print("Ange sökterm")
        return
    term = " ".join(args).replace("'", "''")

    # Sök deals
    deals = psql(f'''SELECT id, name, stage::text FROM {WS}.opportunity
                     WHERE "deletedAt" IS NULL AND name ILIKE '%{term}%' LIMIT 10''')
    if deals:
        print(f"DEALS ({len(deals)}):")
        for d in deals:
            p = d.split("|")
            print(f"  {p[1]:<50} [{p[2]}]  id:{p[0][:8]}...")

    # Sök bolag
    companies = psql(f'''SELECT id, name FROM {WS}.company
                        WHERE "deletedAt" IS NULL AND name ILIKE '%{term}%' LIMIT 10''')
    if companies:
        print(f"\nBOLAG ({len(companies)}):")
        for c in companies:
            p = c.split("|")
            print(f"  {p[1]:<50} id:{p[0][:8]}...")

    # Sök kontakter
    contacts = psql(f'''SELECT id, "nameFirstName", "nameLastName" FROM {WS}.person
                        WHERE "deletedAt" IS NULL
                        AND ("nameFirstName" ILIKE '%{term}%' OR "nameLastName" ILIKE '%{term}%')
                        LIMIT 10''')
    if contacts:
        print(f"\nKONTAKTER ({len(contacts)}):")
        for c in contacts:
            p = c.split("|")
            print(f"  {p[1]} {p[2]:<40} id:{p[0][:8]}...")

    if not deals and not companies and not contacts:
        print(f"Inga träffar för '{term}'")

# === STATS ===

def cmd_stats(args):
    """Pipeline-statistik."""
    # Per stage
    stages = psql(f'''SELECT stage::text, COUNT(*),
                      SUM(COALESCE("uppskattatVardeAmountMicros",0))
                      FROM {WS}.opportunity
                      WHERE "deletedAt" IS NULL
                      GROUP BY stage ORDER BY stage''')

    print("PIPELINE PER STEG:")
    print(f"{'Steg':<20} {'Antal':>6} {'Värde':>15}")
    print("-" * 45)
    total_count = 0
    total_value = 0
    for row in stages:
        p = row.split("|")
        if len(p) >= 3:
            count = int(p[1])
            value = int(float(p[2]))
            total_count += count
            total_value += value
            print(f"{p[0]:<20} {count:>6} {fmt_amount(p[2]):>15}")
    print(f"{'TOTALT':<20} {total_count:>6} {fmt_amount(total_value):>15}")

    # Per typ
    print("\nPIPELINE PER TYP:")
    types = psql(f'''SELECT COALESCE(affarstyp::text,'Ej satt'), COUNT(*),
                     SUM(COALESCE("uppskattatVardeAmountMicros",0))
                     FROM {WS}.opportunity
                     WHERE "deletedAt" IS NULL
                     GROUP BY affarstyp::text ORDER BY affarstyp::text''')
    print(f"{'Typ':<20} {'Antal':>6} {'Värde':>15}")
    print("-" * 45)
    for row in types:
        p = row.split("|")
        if len(p) >= 3:
            print(f"{p[0]:<20} {int(p[1]):>6} {fmt_amount(p[2]):>15}")

# === MAIN ===

COMMANDS = {
    "deals": cmd_deals,
    "deal": cmd_deal,
    "deal-update": cmd_deal_update,
    "deal-stage": cmd_deal_stage,
    "deal-create": cmd_deal_create,
    "companies": cmd_companies,
    "company": cmd_company,
    "company-create": cmd_company_create,
    "contacts": cmd_contacts,
    "contact": cmd_contact,
    "contact-create": cmd_contact_create,
    "tasks": cmd_tasks,
    "task-create": cmd_task_create,
    "task-done": cmd_task_done,
    "notes": cmd_notes,
    "note-create": cmd_note_create,
    "search": cmd_search,
    "stats": cmd_stats,
}

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help", "help"):
        print("Twenty CRM CLI — RM Entreprenad och Fasad")
        print(f"\nKommandon: {', '.join(sorted(COMMANDS.keys()))}")
        print("\nExempel:")
        print("  twenty_cli.py deals")
        print("  twenty_cli.py deal Grimvägen")
        print("  twenty_cli.py deal-stage <id> KONTRAKTERAT")
        print("  twenty_cli.py search Balder")
        print("  twenty_cli.py stats")
        print('  twenty_cli.py deal-create \'{"name":"Nytt projekt","uppskattat":2000000,"affarstyp":"VARM"}\'')
        print('  twenty_cli.py task-create \'{"title":"Ring kund","due":"2026-04-01","deal_id":"uuid"}\'')
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd not in COMMANDS:
        print(f"Okänt kommando: {cmd}")
        print(f"Tillgängliga: {', '.join(sorted(COMMANDS.keys()))}")
        sys.exit(1)

    COMMANDS[cmd](sys.argv[2:])

if __name__ == "__main__":
    main()
