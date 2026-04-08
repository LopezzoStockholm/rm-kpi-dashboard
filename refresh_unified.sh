#!/bin/bash
# Refresh crm_next_unified materialized view
# Kör efter next_sync och sync_twenty för att ha färsk data från båda systemen
docker exec rm-postgres psql -U rmadmin -d rm_central -c "REFRESH MATERIALIZED VIEW crm_next_unified;" 2>&1
echo "$(date '+%Y-%m-%d %H:%M:%S') — crm_next_unified refreshed"
