"""
rm_data.py — Delad databasmodul för RM-infrastrukturen.

Ersätter alla separata psql()-funktioner med parameteriserade frågor via psycopg2.
Alla filer ska importera denna modul istället för att köra docker exec psql.

Användning:
    from rm_data import query_one, query_all, execute, safe_json_query
"""

import psycopg2
import psycopg2.extras
import json
import os
import logging

log = logging.getLogger('rm_data')

# --- Konfiguration ---
_DB_CONFIG = {
    'dbname': os.environ.get('RM_DB_NAME', 'rm_central'),
    'user': os.environ.get('RM_DB_USER', 'rmadmin'),
    'password': os.environ.get('RM_DB_PASSWORD', 'Rm4x7KoncernDB2026stack'),
    'host': os.environ.get('RM_DB_HOST', '127.0.0.1'),
    'port': int(os.environ.get('RM_DB_PORT', 5432)),
}


def get_conn(db=None):
    """Returnera en ny databasanslutning. Anroparen ansvarar för att stänga den."""
    cfg = dict(_DB_CONFIG)
    if db:
        cfg['dbname'] = db
    return psycopg2.connect(**cfg)


def query_one(sql, params=None, db=None):
    """Kör en SQL-fråga och returnera första radens första kolumn, eller None.

    Exakt samma semantik som den gamla psql()-funktionen men parameteriserad.

    Exempel:
        count = query_one("SELECT count(*) FROM fortnox_invoice WHERE company_code=%s", ('RM',))
    """
    conn = None
    try:
        conn = get_conn(db)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        log.error(f"query_one error: {e}\nSQL: {sql}\nParams: {params}")
        return None
    finally:
        if conn:
            conn.close()


def query_all(sql, params=None, db=None):
    """Kör en SQL-fråga och returnera alla rader som lista av tuples.

    Exempel:
        rows = query_all("SELECT id, name FROM project WHERE status=%s", ('active',))
    """
    conn = None
    try:
        conn = get_conn(db)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    except Exception as e:
        log.error(f"query_all error: {e}\nSQL: {sql}\nParams: {params}")
        return []
    finally:
        if conn:
            conn.close()


def query_dicts(sql, params=None, db=None):
    """Kör en SQL-fråga och returnera alla rader som lista av dicts.

    Exempel:
        items = query_dicts("SELECT id, name FROM project WHERE status=%s", ('active',))
        # [{'id': 1, 'name': 'Grimvägen'}, ...]
    """
    conn = None
    try:
        conn = get_conn(db)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        log.error(f"query_dicts error: {e}\nSQL: {sql}\nParams: {params}")
        return []
    finally:
        if conn:
            conn.close()


def execute(sql, params=None, db=None, returning=False):
    """Kör INSERT/UPDATE/DELETE. Returnera RETURNING-värdet om returning=True.

    Exempel:
        execute("UPDATE ata_register SET status=%s WHERE id=%s", ('approved', 42))
        new_id = execute("INSERT INTO ... RETURNING id", (...,), returning=True)
    """
    conn = None
    try:
        conn = get_conn(db)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = None
            if returning and cur.description:
                row = cur.fetchone()
                result = row[0] if row else None
            conn.commit()
            return result
    except Exception as e:
        log.error(f"execute error: {e}\nSQL: {sql}\nParams: {params}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def safe_json_query(sql, params=None, db=None):
    """Kör en json_agg()-fråga och returnera en Python-lista.

    Hanterar alla edge cases: None, tom sträng, 'null', tom array.

    Exempel:
        items = safe_json_query("SELECT json_agg(row_to_json(t)) FROM (...) t", ('RM',))
    """
    raw = query_one(sql, params, db)
    if raw is None:
        return []
    if isinstance(raw, str):
        if not raw or raw.strip() in ('', 'null'):
            return []
        try:
            result = json.loads(raw)
            return result if isinstance(result, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
    if isinstance(raw, list):
        return raw
    return []


# --- Bakåtkompatibilitet ---
# Dessa funktioner emulerar det gamla psql()-gränssnittet via docker exec
# för filer som ännu inte migrerats fullt.

def psql_compat(query_str, db='rm_central'):
    """Bakåtkompatibel wrapper — kör SQL och returnera rå sträng (som gamla psql()).

    VARNING: Använd INTE denna för ny kod. Använd query_one/query_all/query_dicts istället.
    Finns bara för att minimera ändringar i filer som inte migrerats ännu.
    """
    conn = None
    try:
        conn = get_conn(db)
        with conn.cursor() as cur:
            cur.execute(query_str)
            row = cur.fetchone()
            if row is None:
                return ''
            val = row[0]
            if val is None:
                return ''
            if isinstance(val, (dict, list)):
                return json.dumps(val, ensure_ascii=False, default=str)
            return str(val)
    except Exception as e:
        log.error(f"psql_compat error: {e}\nSQL: {query_str}")
        return ''
    finally:
        if conn:
            conn.close()
