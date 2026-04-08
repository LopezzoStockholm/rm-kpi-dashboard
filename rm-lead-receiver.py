#!/usr/bin/env python3
"""
RM Lead Receiver API
====================
Tar emot leads från browser-relay och skriver till Twenty CRM.
Kör som enkel HTTP-server på port 8081.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import subprocess
import hashlib
from datetime import datetime

WS = "workspace_13e0qz9uia3v9w5dx0mk6etm5"
LOG_FILE = "/var/log/rm-prospector.log"
API_KEY = "rm-combify-2026"  # Enkel autentisering

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts}: {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except:
        pass

def psql(query, db="twenty"):
    cmd = ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-t", "-A", "-c", query]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.stdout.strip()

def psql_exec(query, db="twenty"):
    cmd = ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-c", query]
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.returncode == 0

def lead_exists(name_hash):
    result = psql(f"""
        SELECT COUNT(*) FROM {WS}.opportunity
        WHERE name LIKE '%[{name_hash}]%' AND "deletedAt" IS NULL
    """)
    return int(result or 0) > 0

def create_lead(name, estimated_value=0):
    name_hash = hashlib.md5(name.encode()).hexdigest()[:8]
    if lead_exists(name_hash):
        return False, "duplicate"

    safe_name = name.replace("'", "''")
    display_name = f"{safe_name} [{name_hash}]"
    owner_id = psql(f'SELECT id FROM {WS}."workspaceMember" LIMIT 1')

    ok = psql_exec(f"""
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
            NOW(), NOW(), 'API', 'RM Prospekteringsagent (Combify)',
            0, '{owner_id}'
        )
    """)
    return ok, "created"


class LeadHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        """CORS preflight"""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Api-Key")
        self.end_headers()

    def do_POST(self):
        if self.path != "/api/leads":
            self.send_error(404)
            return

        # Check API key
        api_key = self.headers.get("X-Api-Key", "")
        if api_key != API_KEY:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid API key"}).encode())
            return

        # Read body
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)

        try:
            data = json.loads(body)
        except:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
            return

        leads = data.get("leads", [])
        log(f"Mottog {len(leads)} leads från browser-relay")

        created = 0
        duplicates = 0
        errors = 0

        for lead in leads:
            name = lead.get("name", "")
            value = lead.get("value", 0)
            if not name:
                continue
            try:
                ok, status = create_lead(name, value)
                if status == "created" and ok:
                    created += 1
                    log(f"  NY LEAD: {name}")
                elif status == "duplicate":
                    duplicates += 1
                else:
                    errors += 1
            except Exception as e:
                errors += 1
                log(f"  FEL: {name}: {e}")

        # Trigger sync
        if created > 0:
            subprocess.run(["/bin/bash", "/opt/rm-infra/sync_twenty.sh"], capture_output=True)
            log(f"Synk körd — {created} nya leads")

        result = {
            "received": len(leads),
            "created": created,
            "duplicates": duplicates,
            "errors": errors,
        }
        log(f"Resultat: {json.dumps(result)}")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())

    def do_GET(self):
        if self.path == "/api/status":
            count = psql(f'SELECT COUNT(*) FROM {WS}.opportunity WHERE "deletedAt" IS NULL')
            total_value = psql(f'SELECT COALESCE(SUM("uppskattatVardeAmountMicros"), 0) FROM {WS}.opportunity WHERE "deletedAt" IS NULL')
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({
                "deals": int(count or 0),
                "total_value_sek": int(total_value or 0) / 1_000_000,
                "timestamp": datetime.now().isoformat(),
            }).encode())
        elif self.path == "/relay" or self.path == "/relay.html":
            self._serve_file("/opt/rm-infra/combify-relay.html", "text/html")
        elif self.path.startswith("/relay.js"):
            self._serve_file("/opt/rm-infra/relay.js", "application/javascript")
        elif self.path == "/receive" or self.path == "/receive.html":
            self._serve_file("/opt/rm-infra/receive.html", "text/html")
        else:
            self.send_error(404)

    def _serve_file(self, filepath, content_type):
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_error(404)

    def log_message(self, format, *args):
        pass  # Suppress default HTTP logging


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 8081), LeadHandler)
    log("Lead Receiver API startar på port 8081")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
