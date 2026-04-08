#!/usr/bin/env python3
"""
RM Prospekteringsagent v2
=========================
Hämtar bygglov från Combify GraphQL API och skapar leads i Twenty CRM.

Datakällor:
1. Combify — bygglov i Stockholms län (godkända, nybyggnation + ändring)
2. Combify — markanvisningar (tenders)
3. Combify — detaljplaner (development_plans)

Agenten:
- Söker nya bygglov/projekt i RM:s upptagningsområde
- Filtrerar på relevanta kategorier
- Skapar leads i Twenty CRM med affärstyp "KALL" och steg "INKOMMIT"
- Använder Cognito refresh_token för att förnya JWT automatiskt
"""

import subprocess
import json
import hashlib
import sys
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import URLError
import ssl

# ─── Config ────────────────────────────────────────────────
WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"
LOG_FILE = "/var/log/rm-prospector.log"
TOKEN_FILE = "/opt/rm-infra/combify-tokens.json"

# Combify AppSync
COMBIFY_ENDPOINT = "https://wktutp4vpbezrbwtwea4c6qd7m.appsync-api.eu-west-1.amazonaws.com/graphql"
COMBIFY_CLIENT_ID = "4spuf8r6iel759h8li3g1tbcvi"
COMBIFY_REGION = "eu-west-1"

# Stockholms läns kommuner — municipality_ids
STOCKHOLM_MUNICIPALITY_IDS = [
    "0114", "0115", "0117", "0120", "0123", "0125", "0126", "0127",
    "0128", "0136", "0138", "0139", "0140", "0160", "0162", "0163",
    "0180", "0181", "0182", "0183", "0184", "0186", "0187", "0188",
    "0191", "0192",
]

# Relevanta labels för RM:s verksamhet
RELEVANT_LABELS = [
    "residential", "infrastructure_technical", "industrial",
    "office", "community_function", "educational",
    "sports_recreation", "healthcare", "retail",
]

# ─── Logging ───────────────────────────────────────────────
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts}: {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass

# ─── Token management ─────────────────────────────────────
def load_tokens():
    """Load Combify tokens from file."""
    try:
        with open(TOKEN_FILE, "r") as f:
            return json.load(f)
    except:
        log("VARNING: Kunde inte läsa token-fil. Kör med sparad id_token.")
        return {}

def save_tokens(tokens):
    """Save updated tokens to file."""
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump(tokens, f)
    except Exception as e:
        log(f"VARNING: Kunde inte spara tokens: {e}")

def refresh_id_token(tokens):
    """Use refresh_token to get a new id_token via Cognito."""
    try:
        ctx = ssl.create_default_context()
        body = json.dumps({
            "AuthFlow": "REFRESH_TOKEN_AUTH",
            "ClientId": COMBIFY_CLIENT_ID,
            "AuthParameters": {
                "REFRESH_TOKEN": tokens.get("refresh_token", "")
            }
        }).encode()

        req = Request(
            f"https://cognito-idp.{COMBIFY_REGION}.amazonaws.com/",
            data=body,
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
            }
        )
        resp = urlopen(req, context=ctx, timeout=10)
        data = json.loads(resp.read())
        new_id_token = data["AuthenticationResult"]["IdToken"]
        tokens["id_token"] = new_id_token
        save_tokens(tokens)
        log("Combify token förnyad")
        return new_id_token
    except Exception as e:
        log(f"FEL: Kunde inte förnya token: {e}")
        return tokens.get("id_token", "")

def get_valid_token():
    """Get a valid Combify id_token, refreshing if needed."""
    tokens = load_tokens()
    id_token = tokens.get("id_token", "")

    if not id_token:
        return refresh_id_token(tokens)

    # Check if token is expired (JWT has exp claim in payload)
    try:
        import base64
        payload = id_token.split(".")[1]
        # Add padding
        payload += "=" * (4 - len(payload) % 4)
        claims = json.loads(base64.b64decode(payload))
        exp = claims.get("exp", 0)
        if datetime.utcnow().timestamp() > exp - 300:  # 5 min margin
            log("Token utgången, förnyar...")
            return refresh_id_token(tokens)
    except:
        pass

    return id_token

# ─── Combify GraphQL ──────────────────────────────────────
def combify_search(query, variables, token=None):
    """Execute a GraphQL search against Combify API."""
    if token is None:
        token = get_valid_token()

    ctx = ssl.create_default_context()
    body = json.dumps({"query": query, "variables": variables}).encode()

    req = Request(
        COMBIFY_ENDPOINT,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": token,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://map.combify.com",
            "Referer": "https://map.combify.com/",
            "Accept": "application/json, text/plain, */*",
        }
    )

    try:
        resp = urlopen(req, context=ctx, timeout=30)
        data = json.loads(resp.read())

        if "errors" in data and data.get("data") is None:
            # Token might be expired, try refresh
            log(f"API-fel: {data['errors'][0].get('message', 'unknown')}")
            if "Unauthorized" in str(data["errors"]):
                token = refresh_id_token(load_tokens())
                req = Request(
                    COMBIFY_ENDPOINT,
                    data=body,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": token,
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Origin": "https://map.combify.com",
                        "Referer": "https://map.combify.com/",
                        "Accept": "application/json, text/plain, */*",
                    }
                )
                resp = urlopen(req, context=ctx, timeout=30)
                data = json.loads(resp.read())

        return data
    except Exception as e:
        log(f"FEL vid Combify-anrop: {e}")
        return {"data": {"search": []}}

# ─── Database helpers ──────────────────────────────────────
def psql(query, db="twenty"):
    cmd = ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-t", "-A", "-c", query]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.stdout.strip()

def psql_exec(query, db="twenty"):
    cmd = ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-c", query]
    subprocess.run(cmd, capture_output=True, text=True)

def lead_exists(name_hash):
    result = psql(f"""
        SELECT COUNT(*) FROM {WS}.opportunity
        WHERE name LIKE '%[{name_hash}]%' AND "deletedAt" IS NULL
    """)
    return int(result or 0) > 0

def create_lead(name, source, estimated_value=0, kommun=""):
    name_hash = hashlib.md5(name.encode()).hexdigest()[:8]
    if lead_exists(name_hash):
        return False

    safe_name = name.replace("'", "''")
    display_name = f"{safe_name} [{name_hash}]"
    owner_id = psql(f'SELECT id FROM {WS}."workspaceMember" LIMIT 1')

    psql_exec(f"""
        INSERT INTO {WS}.opportunity (
            id, name, stage, affarstyp,
            "uppskattatVardeAmountMicros", "uppskattatVardeCurrencyCode",
            "kalkyleratVardeAmountMicros", "kalkyleratVardeCurrencyCode",
            "createdAt", "updatedAt", "createdBySource", "createdByName",
            position, "ownerId"
        ) VALUES (
            gen_random_uuid(),
            '{display_name}',
            'INKOMMIT',
            'KALL',
            {int(estimated_value * 1_000_000)}, 'SEK',
            0, 'SEK',
            NOW(), NOW(), 'API', 'RM Prospekteringsagent',
            0, '{owner_id}'
        )
    """)
    return True

# ─── Estimate value based on area and type ─────────────────
def estimate_value(area_m2, procedure, labels):
    """Uppskatta projektvärde baserat på yta och typ."""
    if not area_m2 or area_m2 == 0:
        return 500_000  # Minimumuppskattning

    # SEK per kvm baserat på typ
    rates = {
        "new_building": 25000,
        "addition": 18000,
        "modification": 12000,
        "demolition": 3000,
    }
    base_rate = rates.get(procedure, 15000)

    # Justera för typ av byggnad
    if any(l in labels for l in ["residential", "office"]):
        base_rate *= 1.2
    elif any(l in labels for l in ["industrial", "storage_warehouse"]):
        base_rate *= 0.7

    value = area_m2 * base_rate

    # RM:s typiska projektspann: 500K - 50M
    return max(500_000, min(value, 50_000_000))

# ─── Source 1: Combify Bygglov ────────────────────────────
PERMIT_QUERY = """
query search($input: SearchFiltersInput!) {
  search(input: $input) {
    ... on PermitSearchResponse {
      items {
        permit_id
        sublocality
        block_unit
        municipality_name
        municipality_id
        decision_date
        permit_type
        status
        procedure
        center_point
        updated_at
        sub_categories { area_m2 budget_sek }
        labels { label }
      }
      next_token
      total
    }
  }
}
"""

def fetch_combify_permits():
    """Hämta nya bygglov från Combify för Stockholmsregionen."""
    leads = []

    # Hämta godkända bygglov, nyast först
    variables = {
        "input": {
            "Permit": {
                "search": True,
                "permit_type": ["building_permit"],
                "status": ["approved"],
                "procedure": ["new_building", "addition", "modification"],
                "sort": {"key": "date", "order": "desc"},
            }
        }
    }

    data = combify_search(PERMIT_QUERY, variables)

    search_result = data.get("data", {}).get("search", [])
    if not search_result:
        log("Inga bygglov från Combify")
        return leads

    # search returns a list of response types
    permits_response = None
    for item in search_result:
        if isinstance(item, dict) and "items" in item:
            permits_response = item
            break

    if not permits_response:
        log("Ingen PermitSearchResponse i svaret")
        return leads

    items = permits_response.get("items", [])
    total = permits_response.get("total", 0)
    log(f"Combify: {total} totala bygglov, bearbetar {len(items)} st")

    # Filtrera på Stockholm-kommuner och relevanta labels
    stockholm_names = {
        "Stockholm", "Solna", "Sundbyberg", "Järfälla", "Upplands-Bro",
        "Sigtuna", "Vallentuna", "Norrtälje", "Täby", "Danderyd",
        "Lidingö", "Nacka", "Haninge", "Huddinge", "Botkyrka",
        "Södertälje", "Nykvarn", "Salem", "Ekerö", "Värmdö",
        "Tyresö", "Nynäshamn", "Upplands Väsby", "Vaxholm",
    }

    for permit in items:
        kommun = permit.get("municipality_name", "")
        if kommun not in stockholm_names:
            continue

        labels = [l.get("label", "") for l in (permit.get("labels") or [])]

        # Filtrera bort ointressanta typer (uthus, staket, carport etc)
        if labels and not any(l in RELEVANT_LABELS for l in labels):
            continue

        # Beräkna namn
        sublocality = permit.get("sublocality", "")
        block_unit = permit.get("block_unit", "")
        procedure = permit.get("procedure", "")
        decision_date = permit.get("decision_date", "")

        procedure_sv = {
            "new_building": "Nybyggnad",
            "addition": "Tillbyggnad",
            "modification": "Ändring",
            "demolition": "Rivning",
        }.get(procedure, procedure)

        label_sv = {
            "residential": "Bostad",
            "industrial": "Industri",
            "office": "Kontor",
            "retail": "Handel",
            "educational": "Utbildning",
            "healthcare": "Vård",
            "infrastructure_technical": "Infrastruktur",
            "community_function": "Samhällsfunktion",
            "sports_recreation": "Sport/Fritid",
        }

        main_label = ""
        for l in labels:
            if l in label_sv:
                main_label = label_sv[l]
                break
        if not main_label:
            main_label = "Byggprojekt"

        name = f"{procedure_sv} {main_label}"
        if sublocality:
            name += f" Kv {sublocality.title()}"
        if block_unit:
            name += f" {block_unit}"
        name += f" — {kommun}"

        # Uppskatta värde
        area = 0
        budget = 0
        sub_cats = permit.get("sub_categories") or {}
        if isinstance(sub_cats, dict):
            area = sub_cats.get("area_m2") or 0
            budget = sub_cats.get("budget_sek") or 0

        if budget and budget > 0:
            est_value = budget
        else:
            est_value = estimate_value(area, procedure, labels)

        leads.append({
            "name": name,
            "value": est_value,
            "kommun": kommun.lower(),
            "source_id": permit.get("permit_id", ""),
            "decision_date": decision_date,
        })

    log(f"Combify bygglov: {len(leads)} relevanta i Stockholmsregionen")
    return leads

# ─── Source 2: Combify Markanvisningar ────────────────────
TENDER_QUERY = """
query search($input: SearchFiltersInput!) {
  search(input: $input) {
    ... on TenderSearchResponse {
      items {
        project_id
        name
        municipality_name
        municipality_id
        procedure
        state
        planned_date
        announced_date
        decided_date
        updated_at
        center_point
        sub_categories {
          usage { residential commercial hospitality educational }
          units
        }
      }
      next_token
      total
    }
  }
}
"""

def fetch_combify_tenders():
    """Hämta markanvisningar från Combify."""
    leads = []

    variables = {
        "input": {
            "Tender": {
                "search": True,
            }
        }
    }

    data = combify_search(TENDER_QUERY, variables)
    search_result = data.get("data", {}).get("search", [])

    if not search_result:
        log("Inga markanvisningar från Combify")
        return leads

    tender_response = None
    for item in search_result:
        if isinstance(item, dict) and "items" in item:
            tender_response = item
            break

    if not tender_response:
        return leads

    items = tender_response.get("items", [])
    log(f"Combify markanvisningar: {len(items)} st")

    stockholm_names = {
        "Stockholm", "Solna", "Sundbyberg", "Järfälla", "Upplands-Bro",
        "Sigtuna", "Vallentuna", "Norrtälje", "Täby", "Danderyd",
        "Lidingö", "Nacka", "Haninge", "Huddinge", "Botkyrka",
        "Södertälje", "Nykvarn", "Salem", "Ekerö", "Värmdö",
        "Tyresö", "Nynäshamn", "Upplands Väsby", "Vaxholm",
    }

    for tender in items:
        kommun = tender.get("municipality_name", "")
        if kommun not in stockholm_names:
            continue

        name = tender.get("name", "Okänd markanvisning")
        state = tender.get("state", "")

        lead_name = f"Markanvisning: {name} — {kommun}"

        # Markanvisningar typiskt 5-50M
        sub_cats = tender.get("sub_categories") or {}
        units = 0
        if isinstance(sub_cats, dict):
            units = sub_cats.get("units") or 0

        est_value = max(5_000_000, units * 3_000_000) if units else 10_000_000
        est_value = min(est_value, 50_000_000)

        leads.append({
            "name": lead_name,
            "value": est_value,
            "kommun": kommun.lower(),
            "source_id": tender.get("project_id", ""),
        })

    log(f"Combify markanvisningar: {len(leads)} relevanta i Stockholmsregionen")
    return leads

# ─── Main ──────────────────────────────────────────────────
def run():
    log("=" * 60)
    log("RM Prospekteringsagent v2 — Combify-integration")
    log("=" * 60)

    total_new = 0

    for source_name, source_fn in [
        ("Combify Bygglov", fetch_combify_permits),
        ("Combify Markanvisningar", fetch_combify_tenders),
    ]:
        try:
            leads = source_fn()
            for lead in leads:
                created = create_lead(
                    name=lead.get("name", "Okänd"),
                    source=source_name,
                    estimated_value=lead.get("value", 0),
                    kommun=lead.get("kommun", ""),
                )
                if created:
                    total_new += 1
                    log(f"  NY LEAD: {lead['name']} ({source_name})")
        except Exception as e:
            log(f"FEL i {source_name}: {e}")
            import traceback
            traceback.print_exc()

    log(f"Klart — {total_new} nya leads skapade")

    # Trigga synk och dashboard
    subprocess.run(["/bin/bash", "/opt/rm-infra/sync_twenty.sh"], capture_output=True)
    log("Dashboard uppdaterad")

if __name__ == "__main__":
    run()
