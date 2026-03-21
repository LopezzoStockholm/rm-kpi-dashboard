#!/bin/bash
set -e
cd /opt/rm-infra

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
volumes:
  postgres_data:
  n8n_data:
  redis_data:
networks:
  rm-net:
    driver: bridge
YML

cat > init-db/01-create-databases.sql << 'SQL'
CREATE DATABASE metabase;
CREATE DATABASE n8n;

\c rm_central;

CREATE TABLE company (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    org_nr VARCHAR(20),
    sphere INT NOT NULL DEFAULT 3,
    active BOOLEAN DEFAULT TRUE
);

INSERT INTO company (code, name, org_nr, sphere) VALUES
    ('RM','RM Entreprenad och Fasad AB','559251-1462',3),
    ('RF','Roslag Fastighetsutveckling AB',NULL,2),
    ('BM','Boeno Markforadling AB',NULL,2),
    ('LH','Lopezzo Holding AB',NULL,1),
    ('LI','Lopez Invest & Consulting AB',NULL,1),
    ('RE','Rosersberg Entreprenad AB',NULL,3);

CREATE TABLE fortnox_invoice (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) REFERENCES company(code),
    fortnox_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(300),
    invoice_date DATE,
    due_date DATE,
    total NUMERIC(14,2),
    balance NUMERIC(14,2),
    status VARCHAR(50),
    project_code VARCHAR(50),
    is_credit BOOLEAN DEFAULT FALSE,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_code, fortnox_id)
);

CREATE TABLE fortnox_supplier_invoice (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) REFERENCES company(code),
    fortnox_id VARCHAR(50) NOT NULL,
    supplier_name VARCHAR(300),
    invoice_date DATE,
    due_date DATE,
    total NUMERIC(14,2),
    balance NUMERIC(14,2),
    status VARCHAR(50),
    project_code VARCHAR(50),
    parked BOOLEAN DEFAULT TRUE,
    linked_customer_invoice VARCHAR(50),
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_code, fortnox_id)
);

CREATE TABLE fortnox_payment (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) REFERENCES company(code),
    fortnox_id VARCHAR(50) NOT NULL,
    payment_date DATE,
    amount NUMERIC(14,2),
    type VARCHAR(20),
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_code, fortnox_id)
);

CREATE TABLE project (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) REFERENCES company(code),
    name VARCHAR(300) NOT NULL,
    customer_name VARCHAR(300),
    contract_value NUMERIC(14,2),
    status VARCHAR(50) DEFAULT 'active'
);

CREATE TABLE pipeline_deal (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) REFERENCES company(code),
    name VARCHAR(300) NOT NULL,
    customer_name VARCHAR(300),
    value NUMERIC(14,2),
    stage VARCHAR(100),
    hit_rate NUMERIC(5,2) DEFAULT 50
);

CREATE TABLE budget_period (
    id SERIAL PRIMARY KEY,
    company_code VARCHAR(10) REFERENCES company(code),
    period VARCHAR(7),
    revenue_budget NUMERIC(14,2),
    tb1_budget NUMERIC(14,2)
);

INSERT INTO budget_period (company_code, period, revenue_budget, tb1_budget)
SELECT 'RM', to_char(d, 'YYYY-MM'), 1700, 340
FROM generate_series('2025-11-01'::date, '2026-10-01'::date, '1 month'::interval) d;

CREATE INDEX idx_fi_company ON fortnox_invoice(company_code);
CREATE INDEX idx_si_company ON fortnox_supplier_invoice(company_code);
CREATE INDEX idx_pay_company ON fortnox_payment(company_code);
SQL

echo "ALL FILES CREATED"
docker compose up -d
echo "STACK STARTING..."
sleep 10
docker compose ps
