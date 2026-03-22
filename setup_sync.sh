#!/bin/bash
set -e

cat > /opt/rm-infra/sync_twenty.sh << 'SYNC'
#!/bin/bash
# Twenty CRM -> PostgreSQL sync (direkt SQL, workspace-schema)

# 1. Find the workspace schema name
WS_SCHEMA=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -c "
SELECT schema_name FROM information_schema.schemata 
WHERE schema_name LIKE 'workspace_%' LIMIT 1
" 2>/dev/null)

if [ -z "$WS_SCHEMA" ]; then
    echo "$(date): No workspace schema found"
    exit 1
fi

# 2. Get deals from workspace schema
DEALS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -F '|' -c "
SELECT id, name, stage FROM ${WS_SCHEMA}.opportunity WHERE \"deletedAt\" IS NULL
" 2>/dev/null)

if [ -z "$DEALS" ]; then
    echo "$(date): No deals found in $WS_SCHEMA"
    exit 0
fi

# 3. Clear old and insert new
docker exec rm-postgres psql -U rmadmin -d rm_central -c "DELETE FROM pipeline_deal WHERE company_code = 'RM';" 2>/dev/null

COUNT=0
echo "$DEALS" | while IFS='|' read -r id name stage; do
    if [ -n "$name" ]; then
        safe_name=$(echo "$name" | sed "s/'/''/g")
        stage_lower=$(echo "$stage" | tr '[:upper:]' '[:lower:]')
        case "$stage" in
            PROPOSAL) hr=75 ;;
            MEETING) hr=50 ;;
            *) hr=25 ;;
        esac
        docker exec rm-postgres psql -U rmadmin -d rm_central -c "
            INSERT INTO pipeline_deal (company_code, twenty_id, name, stage, value, hit_rate) 
            VALUES ('RM', '$id', '$safe_name', '$stage_lower', 0, $hr);
        " 2>/dev/null
        COUNT=$((COUNT + 1))
    fi
done

echo "$(date): Synced deals from $WS_SCHEMA"
SYNC

chmod +x /opt/rm-infra/sync_twenty.sh

# Update cron
(crontab -l 2>/dev/null | grep -v sync_twenty; echo "*/15 * * * * /opt/rm-infra/sync_twenty.sh >> /var/log/rm_sync.log 2>&1") | crontab -

# Run now
/opt/rm-infra/sync_twenty.sh

echo "SYNC V2 COMPLETE"
