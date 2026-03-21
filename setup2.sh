#!/bin/bash
set -e
cd /opt/rm-infra

# Stop current stack
docker compose down

# Write complete docker-compose with Twenty
cat > docker-compose.yml << 'YML'
services:
  postgres:
    image: postgres:16-alpine
    container_name: rm-postgres
    restart: always
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-rmadmin}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: rm_central
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-db:/docker-entrypoint-initdb.d
    ports:
      - "127.0.0.1:5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U rmadmin -d rm_central"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - rm-net
  metabase:
    image: metabase/metabase:latest
    container_name: rm-metabase
    restart: always
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: metabase
      MB_DB_PORT: 5432
      MB_DB_USER: ${POSTGRES_USER:-rmadmin}
      MB_DB_PASS: ${POSTGRES_PASSWORD}
      MB_DB_HOST: postgres
      JAVA_TIMEZONE: Europe/Stockholm
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - rm-net
  n8n:
    image: n8nio/n8n:latest
    container_name: rm-n8n
    restart: always
    environment:
      N8N_BASIC_AUTH_ACTIVE: "true"
      N8N_BASIC_AUTH_USER: ${N8N_USER:-admin}
      N8N_BASIC_AUTH_PASSWORD: ${N8N_PASSWORD}
      DB_TYPE: postgresdb
      DB_POSTGRESDB_HOST: postgres
      DB_POSTGRESDB_PORT: 5432
      DB_POSTGRESDB_DATABASE: n8n
      DB_POSTGRESDB_USER: ${POSTGRES_USER:-rmadmin}
      DB_POSTGRESDB_PASSWORD: ${POSTGRES_PASSWORD}
      GENERIC_TIMEZONE: Europe/Stockholm
    volumes:
      - n8n_data:/home/node/.n8n
    ports:
      - "5678:5678"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - rm-net
  redis:
    image: redis:7-alpine
    container_name: rm-redis
    restart: always
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - rm-net
  twenty:
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
volumes:
  postgres_data:
  n8n_data:
  redis_data:
  twenty_data:
networks:
  rm-net:
    driver: bridge
YML

# Create twenty database if not exists
docker compose up -d postgres redis
sleep 5
docker exec rm-postgres psql -U rmadmin -c "CREATE DATABASE twenty;" 2>/dev/null || echo "twenty DB exists"

# Start everything
docker compose up -d
echo "STACK STARTING WITH TWENTY..."
sleep 20
docker compose ps
