#!/bin/bash
set -e

# Create sync script
cat > /opt/rm-infra/sync_twenty.sh << 'SYNC'
#!/bin/bash
# Twenty CRM -> PostgreSQL sync (var 15:e minut via cron)

TOKEN=$(docker exec rm-twenty curl -s http://localhost:3000/metadata \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { signIn(email: \"daniel@boenosverige.se\", password: \"RmTwenty2026crm\") { tokens { accessOrWorkspaceAgnosticToken { token } } } }"}' \
  2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['signIn']['tokens']['accessOrWorkspaceAgnosticToken']['token'])" 2>/dev/null)

if [ -z "$TOKEN" ]; then echo "$(date): Auth failed"; exit 1; fi

DEALS=$(docker exec rm-twenty curl -s http://localhost:3000/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query":"query { opportunities(first: 200) { edges { node { id name stage closeDate } } } }"}' 2>/dev/null)

echo "$DEALS" | python3 -c "
import sys, json, subprocess
data = json.load(sys.stdin)
edges = data.get('data',{}).get('opportunities',{}).get('edges',[])
hr = {'SCREENING': 25, 'MEETING': 50, 'PROPOSAL': 75}
sql = \"DELETE FROM pipeline_deal WHERE company_code = 'RM';\n\"
for e in edges:
    n = e['node']
    name = n['name'].replace(\"'\", \"''\")
    stage = (n.get('stage') or 'screening').lower()
    h = hr.get(n.get('stage',''), 25)
    sql += \"INSERT INTO pipeline_deal (company_code, twenty_id, name, stage, value, hit_rate) VALUES ('RM', '%s', '%s', '%s', 0, %d);\n\" % (n['id'], name, stage, h)
proc = subprocess.run(['docker', 'exec', '-i', 'rm-postgres', 'psql', '-U', 'rmadmin', '-d', 'rm_central'], input=sql, capture_output=True, text=True)
print('$(date): Synced %d deals' % len(edges))
"
SYNC

chmod +x /opt/rm-infra/sync_twenty.sh

# Add to cron (every 15 min)
(crontab -l 2>/dev/null | grep -v sync_twenty; echo "*/15 * * * * /opt/rm-infra/sync_twenty.sh >> /var/log/rm_sync.log 2>&1") | crontab -

# Run once now
/opt/rm-infra/sync_twenty.sh

# Also stop n8n to free memory
cd /opt/rm-infra
docker stop rm-n8n 2>/dev/null
echo "SYNC SETUP COMPLETE"
