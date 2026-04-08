"""
message_parser.py — Central AI-parser for message → Planner task.

Takes raw text from any source (Teams, WhatsApp, etc.) and extracts:
- assignee (who should do it)
- title (what needs to be done)
- project/bucket (where to put it)
- due_date (when it's due)

Uses fuzzy matching against known team members, companies, and Planner buckets.
No external AI API — fast, deterministic, local parsing.
"""

import json
import re
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from thefuzz import fuzz

# ── Config paths ──
CONFIG_DIR = Path("/opt/rm-infra")
TEAMS_CONFIG = CONFIG_DIR / "teams-config.json"
PLANNER_CONFIG = CONFIG_DIR / "planner-config.json"

# ── Load reference data ──
def load_users():
    """Load user map: {user_id: {name, firstName, mail}}"""
    data = json.loads(TEAMS_CONFIG.read_text())
    return data.get("users", {})

WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"

def load_companies(db_conn=None):
    """Load company names from Twenty CRM (via db_conn if provided)."""
    if db_conn:
        cur = db_conn.cursor()
        cur.execute(f'SELECT id, name FROM {WS}.company WHERE "deletedAt" IS NULL')
        return {str(row[0]): row[1] for row in cur.fetchall()}
    return load_companies_via_psql()

def load_companies_via_psql():
    """Load company names via docker exec psql."""
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", "twenty",
         "-t", "-A", "-c", f'SELECT id, name FROM {WS}.company WHERE "deletedAt" IS NULL'],
        capture_output=True, text=True
    )
    companies = {}
    for line in r.stdout.strip().split("\n"):
        if "|" in line:
            parts = line.split("|", 1)
            companies[parts[0]] = parts[1]
    return companies

def load_buckets():
    """Load Planner buckets via Graph API."""
    import requests
    config = json.loads(PLANNER_CONFIG.read_text())
    token = _get_token(config)
    headers = {"Authorization": f"Bearer {token}"}

    buckets = {}
    for plan_id, plan_name in config["plans"].items():
        resp = requests.get(
            f"https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/buckets?$select=id,name",
            headers=headers
        ).json()
        for b in resp.get("value", []):
            buckets[b["id"]] = {"name": b["name"], "plan_id": plan_id, "plan_name": plan_name}
    return buckets

def _get_token(config):
    import requests
    r = requests.post(
        f"https://login.microsoftonline.com/{config['tenant_id']}/oauth2/v2.0/token",
        data={
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials"
        }
    )
    return r.json()["access_token"]


# ── Name parsing ──
# Common Swedish nicknames → formal first name
NICKNAMES = {
    "janne": "Jan",
    "jansen": "Jan",
    "janna": "Jan",
    "putte": "Patrik",
    "pudde": "Patrik",
    "lansen": "Daniel",
    "alex": "Alexandra",
    "mansen": "Mattias",
    "macke": "Mattias",
    "micke": "Mikael",
}

def _is_addressee_position(text_lower, match_start):
    """Check if a name at this position is the person being addressed (assignee).

    Addressee indicators:
    - Name at start of text (e.g., "Erik, kan du...")
    - Name after sentence boundary followed by task verb
    - Name NOT preceded by prepositions (till, med, från, hos, för, åt, via, på)
    """
    # Object position: preceded by prepositions → NOT the assignee
    pre = text_lower[max(0, match_start - 15):match_start].strip()
    if re.search(r'\b(till|med|från|hos|för|åt|via|på|om)\s*$', pre):
        return False

    # At text start → very likely the addressee
    if match_start <= 2:
        return True

    # After sentence boundary
    pre_ctx = text_lower[max(0, match_start - 5):match_start]
    if re.search(r'[\.\!\?]\s*$', pre_ctx):
        return True

    return None  # Neutral — neither clearly addressee nor clearly object


def match_assignee(text, users):
    """Find the best matching team member in the text.

    Priority: addressee position (name at start or after sentence boundary)
    beats any name mentioned in object position (after till/med/från etc.).

    Returns (user_id, name, score) or None.
    """
    text_lower = text.lower()
    candidates = []  # (uid, full_name, base_score, position, is_addressee)

    # First pass: check nicknames
    for nick, formal in NICKNAMES.items():
        pattern = r'\b' + re.escape(nick) + r'\b'
        m = re.search(pattern, text_lower)
        if m:
            for uid, u in users.items():
                if (u.get("firstName") or "").strip() == formal:
                    addr = _is_addressee_position(text_lower, m.start())
                    candidates.append((uid, u.get("name", formal), 100, m.start(), addr))
                    break

    for uid, u in users.items():
        first = (u.get("firstName") or "").strip()
        full = (u.get("name") or "").strip()

        if not first or first.lower() == "none":
            continue

        # Exact first name match (word boundary)
        pattern = r'\b' + re.escape(first.lower()) + r'\b'
        m = re.search(pattern, text_lower)
        if m:
            addr = _is_addressee_position(text_lower, m.start())
            candidates.append((uid, full, 100, m.start(), addr))
            continue

        # Fuzzy match on full name
        ratio = fuzz.partial_ratio(full.lower(), text_lower)
        if ratio >= 80:
            candidates.append((uid, full, ratio, 999, None))

    if not candidates:
        return None

    # Sort: addressee=True first, then False=excluded, then by position (earlier wins)
    def sort_key(c):
        uid, name, score, pos, is_addr = c
        if is_addr is False:
            return (2, pos, -score)   # Object position — lowest priority
        elif is_addr is True:
            return (0, pos, -score)   # Addressee — highest priority
        else:
            return (1, pos, -score)   # Neutral — middle priority

    candidates.sort(key=sort_key)
    best = candidates[0]

    # If best candidate is in object position (is_addr=False) and there are no
    # addressee candidates, still use it but with reduced confidence
    if best[4] is False:
        return (best[0], best[1], max(best[2] - 20, 60))

    return (best[0], best[1], best[2])


# ── Project/bucket matching ──
def match_project(text, companies, buckets):
    """Match text against company names and Planner bucket names.

    Returns (bucket_id, project_name, score) or None.
    """
    text_lower = text.lower()
    best_company = None

    # Match against Twenty company names
    for cid, cname in companies.items():
        ratio = fuzz.token_set_ratio(cname.lower(), text_lower)
        if ratio >= 75 and (best_company is None or ratio > best_company[2]):
            best_company = (cid, cname, ratio)

    # Match against Planner bucket names (non-generic ones)
    generic_buckets = {"to do", "pågår", "pågående", "klart", "blockerat",
                       "in progress", "completed", "blocked", "up next", "backlog",
                       "att göra", "beslut", "väntar svar", "uppföljning",
                       "väntar på extern", "denna vecka", "denna manad",
                       "arbetar med nu", "granska", "att skapa", "att uppdatera",
                       "kvalitetssäkrad och klar", "actions från veckomöte"}

    best_bucket = None
    for bid, binfo in buckets.items():
        bname = binfo["name"].strip().lower()
        if bname in generic_buckets:
            continue
        ratio = fuzz.token_set_ratio(bname, text_lower)
        if ratio >= 75 and (best_bucket is None or ratio > best_bucket[2]):
            best_bucket = (bid, binfo["name"], ratio, binfo["plan_id"])

    # Prefer company match if found (links to Twenty)
    if best_company and best_company[2] >= 80:
        # Find a bucket that matches the company name
        for bid, binfo in buckets.items():
            if fuzz.token_set_ratio(best_company[1].lower(), binfo["name"].strip().lower()) >= 75:
                return (bid, best_company[1], best_company[2])
        # No bucket match — use default plan bucket
        return (None, best_company[1], best_company[2])

    if best_bucket:
        return (best_bucket[0], best_bucket[1], best_bucket[2])

    return None


# ── Deadline parsing ──
WEEKDAYS_SV = {
    "måndag": 0, "mån": 0, "tisdag": 1, "tis": 1,
    "onsdag": 2, "ons": 2, "torsdag": 3, "tor": 3, "tors": 3,
    "fredag": 4, "fre": 4, "lördag": 5, "lör": 5,
    "söndag": 6, "sön": 6
}

def parse_deadline(text):
    """Extract deadline from natural Swedish text.

    Handles: idag, imorgon, fredag, nästa vecka, v15, 15/4, 15 april, etc.
    Returns date or None.
    """
    text_lower = text.lower()
    today = datetime.now().date()

    # "idag" / "today"
    if re.search(r'\bidag\b|\btoday\b', text_lower):
        return today

    # "imorgon" / "tomorrow"
    if re.search(r'\bimorgon\b|\btomorrow\b', text_lower):
        return today + timedelta(days=1)

    # "nästa vecka"
    if re.search(r'\bnästa vecka\b', text_lower):
        days_ahead = 7 - today.weekday()
        return today + timedelta(days=days_ahead)

    # Weekday names: "fredag", "på torsdag"
    for day_name, day_num in WEEKDAYS_SV.items():
        if re.search(r'\b' + day_name + r'\b', text_lower):
            days_ahead = (day_num - today.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7  # Next occurrence
            return today + timedelta(days=days_ahead)

    # "v15", "vecka 15"
    m = re.search(r'\bv(?:ecka)?\s*(\d{1,2})\b', text_lower)
    if m:
        week_num = int(m.group(1))
        year = today.year
        target = datetime.strptime(f"{year}-W{week_num:02d}-1", "%Y-W%W-%w").date()
        if target < today:
            target = datetime.strptime(f"{year+1}-W{week_num:02d}-1", "%Y-W%W-%w").date()
        return target

    # "15/4", "15-4", "15 april"
    months_sv = {
        "januari": 1, "jan": 1, "februari": 2, "feb": 2,
        "mars": 3, "mar": 3, "april": 4, "apr": 4,
        "maj": 5, "juni": 6, "jun": 6, "juli": 7, "jul": 7,
        "augusti": 8, "aug": 8, "september": 9, "sep": 9,
        "oktober": 10, "okt": 10, "november": 11, "nov": 11,
        "december": 12, "dec": 12
    }

    # "15 april" or "15 apr"
    for mname, mnum in months_sv.items():
        m = re.search(r'(\d{1,2})\s+' + mname + r'\b', text_lower)
        if m:
            day = int(m.group(1))
            try:
                d = today.replace(month=mnum, day=day)
                if d < today:
                    d = d.replace(year=d.year + 1)
                return d
            except ValueError:
                pass

    # "15/4" or "15-4"
    m = re.search(r'(\d{1,2})[/\-](\d{1,2})', text_lower)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            try:
                d = today.replace(month=month, day=day)
                if d < today:
                    d = d.replace(year=d.year + 1)
                return d
            except ValueError:
                pass

    return None


# ── Title extraction ──
def extract_title(text, assignee_name=None, project_name=None):
    """Extract the task title by removing assignee, project, and deadline noise.

    Keeps the core action description.
    """
    title = text.strip()

    # Remove @mentions
    title = re.sub(r'@\w+', '', title)

    # Remove assignee name
    if assignee_name:
        for name_part in assignee_name.split():
            title = re.sub(r'\b' + re.escape(name_part) + r'\b', '', title, flags=re.IGNORECASE)

    # Remove project name
    if project_name:
        for part in project_name.split():
            if len(part) > 2:
                title = re.sub(r'\b' + re.escape(part) + r'\b', '', title, flags=re.IGNORECASE)

    # Remove common prefixes
    title = re.sub(r'^(kan du|kan ni|vänligen|snälla|please|fixa|lös|gör)\s+', '', title, flags=re.IGNORECASE)
    # Remove "på/till/med" at start after name/project removal
    title = re.sub(r'^(på|till|med|för|om|i)\s+', '', title, flags=re.IGNORECASE)

    # Remove deadline phrases
    title = re.sub(r'\b(innan|före|senast|deadline|till)\s+(fredag|måndag|tisdag|onsdag|torsdag|lördag|söndag|idag|imorgon|nästa vecka|v\d+)\b', '', title, flags=re.IGNORECASE)

    # Clean up trailing prepositions/punctuation
    title = re.sub(r'\s+', ' ', title).strip()
    title = re.sub(r'^[\s,\-:]+|[\s,\-:]+$', '', title)
    title = re.sub(r'\s+(på|till|med|för|om|i)\s*[,\.\?!]*\s*$', '', title, flags=re.IGNORECASE)
    title = re.sub(r'\s*[,\.\?!]+\s*$', '', title)

    # Capitalize first letter
    if title:
        title = title[0].upper() + title[1:]

    return title if len(title) > 3 else None


# ── Main parse function ──
def parse_message(text, sender_name=None, sender_id=None, source="teams", db_conn=None, companies=None):
    """Parse a message into a structured task.

    Returns dict with: assignee_id, assignee_name, title, project, bucket_id, due_date, confidence
    Or None if the message doesn't look like a task.
    """
    if not text or len(text.strip()) < 5:
        return None

    users = load_users()
    if companies is None:
        companies = load_companies(db_conn)
    buckets = load_buckets()

    # 1. Find assignee
    assignee = match_assignee(text, users)
    assignee_id = assignee[0] if assignee else None
    assignee_name = assignee[1] if assignee else None

    # 2. Find project/bucket
    project = match_project(text, companies, buckets)
    project_name = project[1] if project else None
    bucket_id = project[0] if project else None

    # 3. Find deadline
    due_date = parse_deadline(text)

    # 4. Extract title
    title = extract_title(text, assignee_name, project_name)

    if not title:
        return None

    # 5. Calculate confidence
    signals = 0
    if assignee:
        signals += 1
    if project:
        signals += 1
    if due_date:
        signals += 1

    # Task-like verbs boost confidence
    task_verbs = r'\b(fixa|lös|gör|kolla|skicka|ring|boka|uppdatera|ändra|kontakta|följ upp|ordna|hantera|ta|säkerställ|se till|se över|checka|maila|fakturera|beställ|slutför|stäm av|sammanställ|analysera|granska|verifiera|dokumentera|förbereda|planera|prioritera)\b'
    if re.search(task_verbs, text.lower()):
        signals += 1

    # Imperative/request patterns
    request_patterns = r'\b(kan du|kan ni|behöver|måste|ska|borde|se till att|vänligen|skulle du|skulle ni|vill du|vill ni)\b'
    if re.search(request_patterns, text.lower()):
        signals += 1

    # Need at least 2 signals to be a task
    if signals < 2:
        return None

    confidence = min(1.0, signals * 0.25)

    return {
        "assignee_id": assignee_id,
        "assignee_name": assignee_name,
        "title": title,
        "project": project_name,
        "bucket_id": bucket_id,
        "due_date": due_date,
        "confidence": confidence,
        "signals": signals
    }




# ── Plan routing ──
def determine_plan(parsed, text, source="whatsapp", channel_team_id=None):
    """Determine the correct Planner plan based on assignee role and message keywords.

    Priority order:
    1. Channel-based routing (Teams messages from specific team channels)
    2. Keyword-based override (ekonomi/faktura → Ekonomi plan, even if assignee is Drift)
    3. Assignee's default plan (based on role/department)
    4. Default plan (Actionlista / All Company)

    Returns plan_id string.
    """
    config = json.loads(PLANNER_CONFIG.read_text())
    user_mapping = config.get("user_plan_mapping", {})
    keyword_mapping = config.get("keyword_plan_mapping", {})
    default_plan = config.get("default_plan", "lPbsJZjTI0-b3z3qwEC7pJgABJgp")

    # 1. Teams channel → team mapping (if message came from Teams)
    if source == "teams" and channel_team_id:
        teams_data = json.loads(TEAMS_CONFIG.read_text())
        team_to_plan = {
            "76bc2d3c-e8d6-472b-a719-2c76386e8d9a": "YD99zEGdpkK8No7ihLpEUJgAEcD-",   # RM Ekonomi & Styrning
            "614a47b2-d8f8-43e5-bf77-3b00113f6add": "eQsKfDDyjkG-b8dSB-dpGJgAANY_",   # RM Drift & Projekt
            "cd10752c-86b0-4591-b1fb-ca6c1e793172": "_R91WOHYEkGm1yvJlzX4nZgAHTTt",   # Fastighetsutveckling
            "f3628431-a566-45c5-9621-e0a821cb3c5f": "0bbH2r3GQUabli75RNyHXJgABupG",   # Bolagsstyrning
        }
        if channel_team_id in team_to_plan:
            return team_to_plan[channel_team_id]

    # 2. Keyword override — scan message for department-specific keywords
    text_lower = text.lower()
    keyword_hits = {}  # plan_id → count of keyword hits
    for keyword, plan_id in keyword_mapping.items():
        # Prefix match: "faktura" matches "fakturera", "fakturering", "fakturor"
        # "styrelse" matches "styrelseprotokoll", "styrelsemöte"
        # "koncern" matches "koncernen", "koncernens"
        if re.search(r'\b' + re.escape(keyword), text_lower):
            keyword_hits[plan_id] = keyword_hits.get(plan_id, 0) + 1

    if keyword_hits:
        # Use the plan with most keyword hits
        best_plan = max(keyword_hits, key=keyword_hits.get)
        return best_plan

    # 3. Assignee's default plan
    assignee_id = parsed.get("assignee_id")
    if assignee_id and assignee_id in user_mapping:
        return user_mapping[assignee_id]

    # 4. Default
    return default_plan


# ── Planner task creation ──
def create_planner_task(parsed, plan_id=None, bucket_id=None):
    """Create a Planner task from parsed message data.

    Returns planner_task_id or None.
    """
    import requests

    config = json.loads(PLANNER_CONFIG.read_text())
    token = _get_token(config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Determine plan and bucket
    if not plan_id:
        # Route to correct plan based on assignee role + keywords
        plan_id = determine_plan(parsed, parsed.get("_original_text", ""), source=parsed.get("_source", "whatsapp"))

    if not bucket_id and parsed.get("bucket_id"):
        bucket_id = parsed["bucket_id"]

    if not bucket_id:
        # Use "Pågående" bucket in the target plan
        buckets_resp = requests.get(
            f"https://graph.microsoft.com/v1.0/planner/plans/{plan_id}/buckets",
            headers=headers
        ).json()
        for b in buckets_resp.get("value", []):
            if b["name"].strip().lower() in ["pågående", "pågår", "to do", "att göra", "in progress"]:
                bucket_id = b["id"]
                break
        if not bucket_id and buckets_resp.get("value"):
            bucket_id = buckets_resp["value"][0]["id"]

    body = {
        "planId": plan_id,
        "title": parsed["title"],
    }

    if bucket_id:
        body["bucketId"] = bucket_id

    if parsed.get("assignee_id"):
        body["assignments"] = {
            parsed["assignee_id"]: {
                "@odata.type": "#microsoft.graph.plannerAssignment",
                "orderHint": " !"
            }
        }

    if parsed.get("due_date"):
        body["dueDateTime"] = parsed["due_date"].isoformat() + "T00:00:00Z"

    resp = requests.post(
        "https://graph.microsoft.com/v1.0/planner/tasks",
        headers=headers,
        json=body
    )

    if resp.status_code in [200, 201]:
        return resp.json().get("id")
    else:
        print(f"  ERROR creating Planner task: {resp.status_code} {resp.text}")
        return None


def update_planner_task_description(task_id, description):
    """Update a Planner task's description (details resource).

    Requires GET to fetch ETag, then PATCH to update.
    This adds sender info and original message text to the task body.
    """
    import requests

    config = json.loads(PLANNER_CONFIG.read_text())
    token = _get_token(config)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # GET current details to obtain ETag
    details_url = f"https://graph.microsoft.com/v1.0/planner/tasks/{task_id}/details"
    get_resp = requests.get(details_url, headers=headers)
    if get_resp.status_code != 200:
        print(f"  WARNING: Could not get task details for description update: {get_resp.status_code}")
        return False

    etag = get_resp.headers.get("ETag", get_resp.json().get("@odata.etag", ""))

    # PATCH with description
    patch_headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "If-Match": etag
    }
    patch_resp = requests.patch(
        details_url,
        headers=patch_headers,
        json={
            "description": description,
            "previewType": "description"
        }
    )

    if patch_resp.status_code in [200, 204]:
        print(f"  Task description updated with sender info")
        return True
    else:
        print(f"  WARNING: Could not update task description: {patch_resp.status_code} {patch_resp.text[:200]}")
        return False


if __name__ == "__main__":
    # Test mode
    test_messages = [
        "Janna kan du lösa A på Rocmore innan fredag?",
        "Mattias: kolla upp status på Signalisten, deadline nästa vecka",
        "Erik fixa offerten till Grimmvägen senast torsdag",
        "Bra jobbat alla, trevlig helg!",  # Should NOT be a task
        "Någon som vill ha kaffe?",  # Should NOT be a task
    ]

    users = load_users()
    print("Known users:", [u["firstName"] for u in users.values() if u.get("firstName")])
    print()

    for msg in test_messages:
        result = parse_message(msg)
        if result:
            print(f"✓ '{msg}'")
            print(f"  → assignee: {result['assignee_name']}, title: {result['title']}, "
                  f"project: {result['project']}, due: {result['due_date']}, "
                  f"confidence: {result['confidence']:.0%}")
        else:
            print(f"✗ '{msg}' → Not a task")
        print()
