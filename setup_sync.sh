#!/bin/bash
set -e

cat > /opt/rm-infra/sync_twenty.sh << 'SYNC'
#!/bin/bash
# Twenty CRM <-> PostgreSQL sync v3
# Laeser deals med affarstyp + uppskattat varde fran Twenty DB
# Skriver till pipeline_deal i rm_central

WS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -c "
SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'workspace_%' LIMIT 1" 2>/dev/null)

if [ -z "$WS" ]; then echo "$(date): No workspace"; exit 1; fi

# Hämta deals med alla fält
DEALS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -F '|' -c "
SELECT o.id, o.name, o.stage,
  COALESCE(o.\"affarstyp\", 'KALL'),
  COALESCE(o.\"uppskattatVardeAmountMicros\", 0),
  COALESCE(o.\"kalkyleratVardeAmountMicros\", 0),
  o.\"companyId\"
FROM ${WS}.opportunity o
WHERE o.\"deletedAt\" IS NULL
" 2>/dev/null)

if [ -z "$DEALS" ]; then
  echo "$(date): No deals in $WS"
  exit 0
fi

# Synka till rm_central
docker exec rm-postgres psql -U rmadmin -d rm_central -c "DELETE FROM pipeline_deal WHERE company_code = 'RM';" 2>/dev/null

echo "$DEALS" | while IFS='|' read -r id name stage dtype est_micros calc_micros company_id; do
  if [ -n "$name" ]; then
    safe_name=$(echo "$name" | sed "s/'/''/g")
    stage_lower=$(echo "$stage" | tr '[:upper:]' '[:lower:]')
    dtype_lower=$(echo "$dtype" | tr '[:upper:]' '[:lower:]')
    est=$((est_micros / 1000000))
    calc=$((calc_micros / 1000000))
    
    # Hämta hitrate från matris
    hr=$(docker exec rm-postgres psql -U rmadmin -d rm_central -t -A -c "
      SELECT COALESCE((SELECT hitrate FROM hitrate_matrix WHERE deal_type='$dtype_lower' AND stage='$stage_lower'), 25)" 2>/dev/null)
    
    docker exec rm-postgres psql -U rmadmin -d rm_central -c "
      INSERT INTO pipeline_deal (company_code, twenty_id, name, stage, deal_type, estimated_value, calculated_value, hit_rate)
      VALUES ('RM', '$id', '$safe_name', '$stage_lower', '$dtype_lower', $est, $calc, $hr);" 2>/dev/null
  fi
done

COUNT=$(docker exec rm-postgres psql -U rmadmin -d rm_central -t -A -c "SELECT COUNT(*) FROM pipeline_deal WHERE company_code='RM'" 2>/dev/null)
echo "$(date): Synced $COUNT deals from Twenty ($WS)"
SYNC

chmod +x /opt/rm-infra/sync_twenty.sh

# Installera cron
(crontab -l 2>/dev/null | grep -v sync_twenty; echo "*/15 * * * * /opt/rm-infra/sync_twenty.sh >> /var/log/rm_sync.log 2>&1") | crontab -

# Kör nu
/opt/rm-infra/sync_twenty.sh

echo "SYNC V3 COMPLETE"
