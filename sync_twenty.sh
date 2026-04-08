#!/bin/bash
# sync_twenty v6 — includes ownerId for role-based filtering — includes new fields (region, contractType, leadSource, margin, probability, nextProjectNo)
WS="workspace_13e0qz9uia3v9w5dx0mk6etm5"

# Get deals from Twenty with new fields
Q="SELECT o.id,o.name,o.stage,COALESCE(o.affarstyp,'KALL'),COALESCE(o.\"uppskattatVardeAmountMicros\",0),COALESCE(o.\"kalkyleratVardeAmountMicros\",0),COALESCE(c.name,''),COALESCE(o.\"leadSource\",''),COALESCE(o.\"contractType\",''),COALESCE(o.region,''),COALESCE(o.margin,0),COALESCE(o.probability,0),COALESCE(o.\"nextProjectNo\",''),COALESCE(o.\"ownerId\"::text,'') FROM ${WS}.opportunity o LEFT JOIN ${WS}.company c ON o.\"companyId\"=c.id WHERE o.\"deletedAt\" IS NULL AND o.\"lostReason\" IS NULL AND o.stage != 'FORLORAD'"
DEALS=$(docker exec rm-postgres psql -U rmadmin -d twenty -t -A -F '|' -c "$Q" 2>/dev/null)

if [ -z "$DEALS" ]; then
  echo "$(date): No deals found in Twenty"
  exit 0
fi

# Clear existing RM deals
docker exec rm-postgres psql -U rmadmin -d rm_central -c "DELETE FROM pipeline_deal WHERE company_code='RM';" 2>/dev/null

COUNT=0
while IFS='|' read -r id name stage dtype est calc company leadsrc ctype region margin prob nextpno ownerid; do
  if [ -n "$name" ]; then
    sn=$(echo "$name" | sed "s/'/''/g")
    sl=$(echo "$stage" | tr '[:upper:]' '[:lower:]')
    dl=$(echo "$dtype" | tr '[:upper:]' '[:lower:]')
    ev=$((est / 1000000))
    cv=$((calc / 1000000))
    hr=$(docker exec rm-postgres psql -U rmadmin -d rm_central -t -A -c \
      "SELECT COALESCE((SELECT hitrate FROM hitrate_matrix WHERE deal_type='$dl' AND stage='$sl'),25)" 2>/dev/null)
    
    # Use probability from CRM if set, otherwise use hitrate matrix
    if [ "$prob" != "0" ] && [ -n "$prob" ]; then
      hr="$prob"
    fi
    
    cn=$(echo "$company" | sed "s/'/''/g")
    ls=$(echo "$leadsrc" | sed "s/'/''/g")
    ct=$(echo "$ctype" | sed "s/'/''/g")
    rg=$(echo "$region" | sed "s/'/''/g")
    np=$(echo "$nextpno" | sed "s/'/''/g")
    
    docker exec rm-postgres psql -U rmadmin -d rm_central -c \
      "INSERT INTO pipeline_deal(company_code,name,stage,deal_type,estimated_value,calculated_value,hit_rate,twenty_id,customer_name,lead_source,contract_type,region,margin,probability,next_project_no,owner) VALUES('RM','$sn','$sl','$dl',$ev,$cv,$hr,'$id','$cn','$ls','$ct','$rg',${margin:-0},${prob:-0},'$np','$ownerid');" 2>/dev/null
    COUNT=$((COUNT+1))
  fi
done <<< "$DEALS"

echo "$(date): Synced $COUNT deals from Twenty (v5)"

# Regenerate dashboard

# Fortnox sync (runs only if config exists)
if [ -f /opt/rm-infra/fortnox-config.json ]; then
    echo "$(date): Running Fortnox sync..."
    python3 /opt/rm-infra/fortnox_sync.py 2>&1
fi
