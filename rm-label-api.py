#!/usr/bin/env python3
"""
RM Label API — sätter etiketter på leverantörs- och kundfakturor.
Kör som HTTP-server på port 8082.

Bidirektionell synk:
- Sätter label i vår DB
- Skriver/tar bort #Label-tagg i Fortnox Comments-fältet
- Synk-skriptet läser sedan Comments och sätter label tillbaka

Banksaldo:
- POST /api/banksaldo — sätter aktuellt banksaldo
- GET  /api/banksaldo — hämtar aktuellt banksaldo
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json, subprocess, re, urllib.request, urllib.error
from datetime import datetime

VALID_LABELS = ["Parkerad", "Bevakas", "Tvist", ""]
API_KEY = "rm-label-2026"
FORTNOX_CONFIG = "/opt/rm-infra/fortnox-config.json"
LABEL_TAGS = ["#Parkerad", "#Bevakas", "#Tvist"]

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts}: {msg}")

def psql(query, db="rm_central"):
    r = subprocess.run(
        ["docker", "exec", "rm-postgres", "psql", "-U", "rmadmin", "-d", db, "-t", "-A", "-c", query],
        capture_output=True, text=True
    )
    return r.stdout.strip()

def escape(val):
    if val is None or val == "":
        return "NULL"
    return "'" + str(val).replace("'", "''") + "'"

def get_fortnox_token():
    try:
        with open(FORTNOX_CONFIG) as f:
            return json.load(f).get("access_token", "")
    except Exception:
        return ""

def fortnox_update_invoice_comment(fortnox_id, label, invoice_type="supplier"):
    """Uppdatera Fortnox Comments-fält med label-tagg. Funkar för båda faktura-typer."""
    token = get_fortnox_token()
    if not token:
        log(f"Fortnox token saknas — hoppar över Fortnox-skrivning")
        return False

    if invoice_type == "supplier":
        api_path = f"supplierinvoices/{fortnox_id}"
        wrapper = "SupplierInvoice"
    else:
        api_path = f"invoices/{fortnox_id}"
        wrapper = "Invoice"

    # Hämta befintliga Comments
    try:
        req = urllib.request.Request(
            f"https://api.fortnox.se/3/{api_path}",
            headers={"Authorization": f"Bearer {token}"}
        )
        resp = urllib.request.urlopen(req, timeout=10)
        inv = json.loads(resp.read()).get(wrapper, {})
        current_comments = inv.get("Comments", "") or ""
    except Exception as e:
        log(f"Kunde inte hämta Fortnox {invoice_type}-faktura {fortnox_id}: {e}")
        return False

    # Rensa befintliga label-taggar
    clean = current_comments
    for tag in LABEL_TAGS:
        clean = clean.replace(" " + tag, "").replace(tag, "")
    clean = clean.strip()

    # Lägg till ny tagg om label sätts
    if label:
        new_comments = (clean + " #" + label).strip() if clean else "#" + label
    else:
        new_comments = clean

    # Skriv tillbaka till Fortnox
    try:
        payload = json.dumps({wrapper: {"Comments": new_comments}}).encode()
        req = urllib.request.Request(
            f"https://api.fortnox.se/3/{api_path}",
            data=payload,
            method="PUT",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
        )
        urllib.request.urlopen(req, timeout=10)
        log(f"Fortnox: {invoice_type}-faktura {fortnox_id} Comments -> '{new_comments}'")
        return True
    except urllib.error.HTTPError as e:
        log(f"Fortnox PUT fel {fortnox_id}: {e.code} {e.read().decode()[:100]}")
        return False
    except Exception as e:
        log(f"Fortnox PUT undantag {fortnox_id}: {e}")
        return False


class Handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Api-Key")
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/banksaldo":
            api_key = self.headers.get("X-Api-Key", "")
            if api_key != API_KEY:
                self.respond(401, {"error": "Unauthorized"})
                return
            self.handle_get_banksaldo()
        else:
            self.respond(404, {"error": "Not found"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length)) if length else {}

            api_key = self.headers.get("X-Api-Key", "") or body.get("api_key", "")
            if api_key != API_KEY:
                self.respond(401, {"error": "Unauthorized"})
                return

            if self.path == "/api/label":
                self.handle_label(body)
            elif self.path == "/api/labels":
                self.handle_get_labels(body)
            elif self.path == "/api/banksaldo":
                self.handle_set_banksaldo(body)
            else:
                self.respond(404, {"error": "Not found"})
        except Exception as e:
            log(f"Error: {e}")
            self.respond(500, {"error": str(e)})

    def handle_label(self, body):
        invoice_type = body.get("type", "supplier")
        fortnox_id = body.get("fortnox_id", "")
        label = body.get("label", "")
        company = body.get("company_code", "RM")

        if not fortnox_id:
            self.respond(400, {"error": "fortnox_id required"})
            return
        if label not in VALID_LABELS:
            self.respond(400, {"error": f"Invalid label. Valid: {VALID_LABELS}"})
            return

        table = "fortnox_supplier_invoice" if invoice_type == "supplier" else "fortnox_invoice"
        label_val = escape(label) if label else "NULL"

        sql = f"UPDATE {table} SET label = {label_val} WHERE company_code = '{company}' AND fortnox_id = {escape(fortnox_id)} RETURNING fortnox_id;"
        result = psql(sql)

        if not result:
            self.respond(404, {"error": f"Invoice {fortnox_id} not found in {table}"})
            return

        log(f"DB: label '{label}' satt pa {invoice_type}-faktura {fortnox_id}")

        # Skriv tagg till Fortnox Comments (både kund- och leverantörsfakturor)
        fortnox_synced = fortnox_update_invoice_comment(fortnox_id, label, invoice_type)

        self.respond(200, {
            "ok": True,
            "fortnox_id": fortnox_id,
            "label": label,
            "type": invoice_type,
            "fortnox_synced": fortnox_synced
        })

    def handle_get_labels(self, body):
        company = body.get("company_code", "RM")
        invoice_type = body.get("type", "all")

        results = {}

        if invoice_type in ("all", "supplier"):
            sql = f"""SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, supplier_name as name, due_date::text, balance::numeric(14,0), label, 'supplier' as type
                FROM fortnox_supplier_invoice
                WHERE company_code='{company}' AND label IS NOT NULL AND label != ''
                ORDER BY label, due_date
            ) t;"""
            result = psql(sql)
            results['supplier'] = json.loads(result) if result and result != "" else []

        if invoice_type in ("all", "customer"):
            sql = f"""SELECT json_agg(row_to_json(t)) FROM (
                SELECT fortnox_id, customer_name as name, due_date::text, balance::numeric(14,0), label, 'customer' as type
                FROM fortnox_invoice
                WHERE company_code='{company}' AND label IS NOT NULL AND label != ''
                ORDER BY label, due_date
            ) t;"""
            result = psql(sql)
            results['customer'] = json.loads(result) if result and result != "" else []

        self.respond(200, results)

    def handle_set_banksaldo(self, body):
        company = body.get("company_code", "RM")
        balance = body.get("balance")
        balance_date = body.get("date", datetime.now().strftime("%Y-%m-%d"))

        if balance is None:
            self.respond(400, {"error": "balance required"})
            return

        try:
            balance = float(balance)
        except (ValueError, TypeError):
            self.respond(400, {"error": "balance must be numeric"})
            return

        sql = f"""INSERT INTO bank_balance (company_code, manual_balance, manual_balance_date, updated_at, updated_by)
            VALUES ('{company}', {balance}, '{balance_date}', NOW(), 'manual')
            ON CONFLICT (company_code) DO UPDATE SET
                manual_balance = {balance},
                manual_balance_date = '{balance_date}',
                updated_at = NOW()
            RETURNING manual_balance::numeric(14,2), manual_balance_date::text;"""
        result = psql(sql).split("\n")[0]  # Bara första raden (RETURNING)

        if result and "|" in result:
            parts = result.split("|")
            log(f"Banksaldo satt: {parts[0]} kr per {parts[1]} for {company}")
            self.respond(200, {
                "ok": True,
                "balance": float(parts[0]),
                "date": parts[1],
                "company_code": company
            })
        else:
            self.respond(500, {"error": "Failed to update bank balance"})

    def handle_get_banksaldo(self):
        company = "RM"
        sql = f"SELECT balance::numeric(14,2), balance_date::text, updated_at::text FROM bank_balance WHERE company_code='{company}';"
        result = psql(sql)
        if result:
            parts = result.split("|")
            self.respond(200, {
                "balance": float(parts[0]),
                "date": parts[1],
                "updated_at": parts[2],
                "company_code": company
            })
        else:
            self.respond(200, {"balance": None, "message": "Inget banksaldo satt"})

    def respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, fmt, *args):
        pass

if __name__ == "__main__":
    port = 8082
    log(f"RM Label API starting on port {port}")
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
