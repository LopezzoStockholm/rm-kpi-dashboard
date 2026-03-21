#!/bin/bash
set -e
cd /opt/rm-infra

# Add Twenty to docker-compose
cat >> docker-compose.yml << 'YML'
  twenty-server:
    image: twentycrm/twenty:latest
    container_name: rm-twenty
    restart: always
    environment:
      SERVER_URL: http://161.35.79.92:3001
      FRONT_BASE_URL: http://161.35.79.92:3001
      PG_DATABASE_URL: postgresql://rmadmin:Rm4x7KoncernDB2026stack@postgres:5432/twenty
      REDIS_URL: redis://redis:6379
      APP_SECRET: a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2
      SIGN_IN_PREFILLED: "false"
      IS_SIGN_UP_DISABLED: "false"
      STORAGE_TYPE: local
      STORAGE_LOCAL_PATH: /app/docker-data
    volumes:
      - twenty_data:/app/docker-data
    ports:
      - "3001:3000"
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - rm-net
YML

# Add twenty volume
sed -i 's/^  redis_data:/  redis_data:\n  twenty_data:/' docker-compose.yml

# Create twenty database
docker exec rm-postgres psql -U rmadmin -d rm_central -c "SELECT 'CREATE DATABASE twenty' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'twenty')\gexec" 2>/dev/null || docker exec rm-postgres psql -U rmadmin -c "CREATE DATABASE twenty;" 2>/dev/null || echo "DB exists"

# Pull and start Twenty
docker compose up -d
echo "TWENTY STARTING..."
sleep 15
docker compose ps
