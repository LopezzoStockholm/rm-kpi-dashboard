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
