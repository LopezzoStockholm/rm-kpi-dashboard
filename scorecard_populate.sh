#!/bin/bash
# scorecard_populate.sh — Auto-populate scorecard entries from source queries OR key_activity commitments
# Runs: weekly (Monday 06:00) and monthly (1st of month 06:00)
# Location: /opt/rm-infra/scorecard_populate.sh

set -euo pipefail

DB_CONTAINER="rm-postgres"
DB_USER="rmadmin"
DB_NAME="rm_central"
LOG="/var/log/scorecard_populate.log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $1" >> "$LOG"; }
psql_q() { docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" "$@"; }

log "=== Scorecard populate started ==="

WEEK_PERIOD=$(date '+%G-W%V')
MONTH_PERIOD=$(date '+%Y-%m')

TARGETS=$(psql_q -t -A -F '|' -c "
    SELECT id, frequency, COALESCE(replace(source_query, '|', '/'),''), COALESCE(key_activity_id::text,'')
    FROM scorecard_target
    WHERE auto_populate = true AND active = true
    ORDER BY id
")

if [ -z "$TARGETS" ]; then
    log "No auto-populate targets found"
    exit 0
fi

SUCCESS=0
ERRORS=0

while IFS='|' read -r target_id frequency source_query key_activity_id; do
    [ -z "$target_id" ] && continue

    if [ "$frequency" = "weekly" ]; then
        PERIOD="$WEEK_PERIOD"
    elif [ "$frequency" = "monthly" ]; then
        PERIOD="$MONTH_PERIOD"
    else
        log "SKIP target $target_id: unknown frequency '$frequency'"
        continue
    fi

    key_activity_id=$(echo "$key_activity_id" | tr -d ' ')

    # Prioritera key_activity_id: summera actual_count i meeting_commitment för perioden
    if [ -n "$key_activity_id" ]; then
        if [ "$frequency" = "weekly" ]; then
            QUERY="SELECT COALESCE(SUM(actual_count),0) FROM meeting_commitment WHERE key_activity_id=$key_activity_id AND iso_week='$PERIOD'"
        else
            # månad: summera alla veckor som startar/ingår i månaden (matcha YYYY-Www där veckans måndag faller i månaden)
            QUERY="SELECT COALESCE(SUM(actual_count),0) FROM meeting_commitment WHERE key_activity_id=$key_activity_id AND to_char(to_date(iso_week,'IYYY\"-W\"IW'),'YYYY-MM')='$PERIOD'"
        fi
        ACTUAL=$(psql_q -t -c "$QUERY" 2>/dev/null | tr -d ' ')
    elif [ -n "$source_query" ]; then
        ACTUAL=$(psql_q -t -c "$source_query" 2>/dev/null | tr -d ' ')
    else
        log "SKIP target $target_id: neither key_activity_id nor source_query"
        continue
    fi

    if [ -z "$ACTUAL" ]; then
        log "ERROR target $target_id: query returned empty"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    UPSERT="INSERT INTO scorecard_entry (target_id, period, actual_value, entered_by)
            VALUES ($target_id, '$PERIOD', $ACTUAL, 'auto')
            ON CONFLICT (target_id, period)
            DO UPDATE SET actual_value = $ACTUAL, entered_by = 'auto'"

    if psql_q -c "$UPSERT" > /dev/null 2>&1; then
        log "OK target $target_id period=$PERIOD value=$ACTUAL (key_activity=${key_activity_id:-none})"
        SUCCESS=$((SUCCESS + 1))
    else
        log "ERROR target $target_id: upsert failed"
        ERRORS=$((ERRORS + 1))
    fi
done <<< "$TARGETS"

log "=== Done: $SUCCESS ok, $ERRORS errors ==="
