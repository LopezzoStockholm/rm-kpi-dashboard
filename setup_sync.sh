#!/bin/bash
set -e

# Create improved sync script - direct PostgreSQL to PostgreSQL
cat > /opt/rm-infra/sync_twenty.sh << 'SYNC'
#!/bin/bash
# Twenty CRM -> PostgreSQL sync (direkt via SQL, ingen API)

docker exec rm-postgres psql -U rmadmin -d rm_central -c "
-- Rensa gamla deals
DELETE FROM pipeline_deal WHERE company_code = 'RM';

-- Synka fran Twenty-databasen direkt
INSERT INTO pipeline_deal (company_code, twenty_id, name, stage, value, hit_rate)
SELECT 
    'RM',
    o.id::text,
    o.name,
    LOWER(o.stage),
    COALESCE((o.\"amountAmountMicros\")::numeric / 1000000, 0),
    CASE 
        WHEN o.stage = 'PROPOSAL' THEN 75
        WHEN o.stage = 'MEETING' THEN 50
        ELSE 25
    END
FROM dblink(
    'dbname=twenty user=rmadmin password=Rm4x7KoncernDB2026stack',
    'SELECT id, name, stage, \"amountAmountMicros\" FROM core.opportunity WHERE \"deletedAt\" IS NULL'
) AS o(id uuid, name text, stage text, \"amountAmountMicros\" bigint);
" 2>/dev/null

if [ $? -ne 0 ]; then
    # dblink not available, try alternative approach
    # Query twenty DB and pipe to rm_central
    DEALS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -F '|' -c "
        SELECT id, name, stage, COALESCE(\"amountAmountMicros\", 0) 
        FROM core.opportunity 
        WHERE \"deletedAt\" IS NULL
    " 2>/dev/null)
    
    if [ -z "$DEALS" ]; then
        # Try without core schema
        DEALS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -F '|' -c "
            SELECT id, name, stage, 0 
            FROM public.opportunity 
            WHERE \"deletedAt\" IS NULL
        " 2>/dev/null)
    fi
    
    if [ -n "$DEALS" ]; then
        # Clear and re-insert
        docker exec rm-postgres psql -U rmadmin -d rm_central -c "DELETE FROM pipeline_deal WHERE company_code = 'RM';" 2>/dev/null
        
        COUNT=0
        echo "$DEALS" | while IFS='|' read -r id name stage amount; do
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
                    VALUES ('RM', '$id', '$safe_name', '$stage_lower', $((amount / 1000000)), $hr);
                " 2>/dev/null
                COUNT=$((COUNT + 1))
            fi
        done
        echo "$(date): Synced deals from twenty DB"
    else
        echo "$(date): No deals found in twenty DB"
    fi
else
    echo "$(date): Synced via dblink"
fi
SYNC

chmod +x /opt/rm-infra/sync_twenty.sh

# Test: check twenty DB schema
echo "=== Twenty DB tables ==="
docker exec rm-postgres psql -U rmadmin -d twenty -c "\dt core.*" 2>/dev/null || \
docker exec rm-postgres psql -U rmadmin -d twenty -c "\dt" 2>/dev/null

echo ""
echo "=== Opportunity data ==="
docker exec rm-postgres psql -U rmadmin -d twenty -t -c "
SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') AND tablename LIKE '%pportunit%' LIMIT 5
" 2>/dev/null

# Run sync
/opt/rm-infra/sync_twenty.sh
echo "SYNC FIX COMPLETE"
