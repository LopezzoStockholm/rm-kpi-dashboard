#!/bin/bash
set -e
cat > /opt/rm-infra/sync_twenty.sh << 'SYNCEOF'
#!/bin/bash
WS="workspace_13e0qz9uia3v9w5dx0mk6etm5"
Q="SELECT id,name,stage,COALESCE(affarstyp,'KALL'),COALESCE(\"uppskattatVardeAmountMicros\",0),COALESCE(\"kalkyleratVardeAmountMicros\",0) FROM ${WS}.opportunity WHERE \"deletedAt\" IS NULL"
DEALS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -F '|' -c "$Q" 2>/dev/null)
if [ -z "$DEALS" ]; then echo "$(date): No deals"; exit 0; fi
docker exec rm-postgres psql -U rmadmin -d rm_central -c "DELETE FROM pipeline_deal WHERE company_code='RM';" 2>/dev/null
COUNT=0
while IFS='|' read -r id name stage dtype est calc; do
  if [ -n "$name" ]; then
    sn=$(echo "$name" | sed "s/'/''/g")
    sl=$(echo "$stage" | tr '[:upper:]' '[:lower:]')
    dl=$(echo "$dtype" | tr '[:upper:]' '[:lower:]')
    ev=$((est / 1000000))
    cv=$((calc / 1000000))
    hr=$(docker exec rm-postgres psql -U rmadmin -d rm_central -t -A -c "SELECT COALESCE((SELECT hitrate FROM hitrate_matrix WHERE deal_type='$dl' AND stage='$sl'),25)" 2>/dev/null)
    docker exec rm-postgres psql -U rmadmin -d rm_central -c "INSERT INTO pipeline_deal(company_code,twenty_id,name,stage,deal_type,estimated_value,calculated_value,hit_rate) VALUES('RM','$id','$sn','$sl','$dl',$ev,$cv,$hr);" 2>/dev/null
    COUNT=$((COUNT+1))
  fi
done <<< "$DEALS"
echo "$(date): Synced $COUNT deals"
SYNCEOF
chmod +x /opt/rm-infra/sync_twenty.sh
(crontab -l 2>/dev/null | grep -v sync_twenty; echo "*/15 * * * * /opt/rm-infra/sync_twenty.sh >> /var/log/rm_sync.log 2>&1") | crontab -
/opt/rm-infra/sync_twenty.sh
echo "SYNC V3 DONE"
