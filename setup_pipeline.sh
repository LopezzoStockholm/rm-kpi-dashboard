#!/bin/bash
set -e

# 1. Update PostgreSQL schema with new fields
docker exec rm-postgres psql -U rmadmin -d rm_central << 'SQL'

-- Add new columns to pipeline_deal
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS deal_type VARCHAR(20) DEFAULT 'kall';
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS estimated_value NUMERIC(14,2) DEFAULT 0;
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS calculated_value NUMERIC(14,2) DEFAULT 0;
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS best_value NUMERIC(14,2) GENERATED ALWAYS AS (CASE WHEN calculated_value > 0 THEN calculated_value ELSE estimated_value END) STORED;
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS owner VARCHAR(50);
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS customer VARCHAR(200);
ALTER TABLE pipeline_deal ADD COLUMN IF NOT EXISTS city VARCHAR(100);

-- Create hitrate matrix table
CREATE TABLE IF NOT EXISTS hitrate_matrix (
    deal_type VARCHAR(20) NOT NULL,
    stage VARCHAR(30) NOT NULL,
    hitrate NUMERIC(5,2) NOT NULL,
    PRIMARY KEY (deal_type, stage)
);

-- Insert hitrate values
DELETE FROM hitrate_matrix;
INSERT INTO hitrate_matrix (deal_type, stage, hitrate) VALUES
    ('kall', 'inkommit', 5), ('kall', 'kalkyl', 10), ('kall', 'offert', 20), ('kall', 'forhandling', 50), ('kall', 'kontrakterat', 100),
    ('varm', 'inkommit', 15), ('varm', 'kalkyl', 30), ('varm', 'offert', 50), ('varm', 'forhandling', 70), ('varm', 'kontrakterat', 100),
    ('styrd', 'inkommit', 40), ('styrd', 'kalkyl', 60), ('styrd', 'offert', 80), ('styrd', 'forhandling', 90), ('styrd', 'kontrakterat', 100),
    ('service', 'inkommit', 70), ('service', 'kalkyl', 85), ('service', 'offert', 95), ('service', 'forhandling', 95), ('service', 'kontrakterat', 100),
    ('intern', 'inkommit', 90), ('intern', 'kalkyl', 95), ('intern', 'offert', 100), ('intern', 'forhandling', 100), ('intern', 'kontrakterat', 100);

-- Create view for weighted pipeline
CREATE OR REPLACE VIEW v_pipeline_weighted AS
SELECT 
    d.name,
    d.deal_type,
    d.stage,
    d.best_value,
    d.owner,
    d.customer,
    COALESCE(h.hitrate, 25) AS hitrate,
    ROUND(d.best_value * COALESCE(h.hitrate, 25) / 100, 0) AS weighted_value
FROM pipeline_deal d
LEFT JOIN hitrate_matrix h ON h.deal_type = d.deal_type AND h.stage = d.stage
WHERE d.company_code = 'RM';

-- Create view for pipeline summary by type
CREATE OR REPLACE VIEW v_pipeline_by_type AS
SELECT
    deal_type,
    COUNT(*) AS deal_count,
    SUM(best_value) AS total_value,
    SUM(ROUND(best_value * COALESCE(h.hitrate, 25) / 100, 0)) AS weighted_value
FROM pipeline_deal d
LEFT JOIN hitrate_matrix h ON h.deal_type = d.deal_type AND h.stage = d.stage
WHERE d.company_code = 'RM'
GROUP BY deal_type;

-- Update existing deals with type and values from offer data
UPDATE pipeline_deal SET deal_type = 'intern', estimated_value = 9000000, customer = 'Daniel Lopez', owner = 'MED', city = 'Jarfalla' WHERE name LIKE 'Grimvagen 12%';
UPDATE pipeline_deal SET deal_type = 'intern', estimated_value = 5750000, customer = 'RM Fasad', owner = 'MED', city = 'Jarfalla' WHERE name LIKE '%Stomkomp%';
UPDATE pipeline_deal SET deal_type = 'intern', estimated_value = 400000, customer = 'RM Fasad', owner = 'MED', city = 'Jarfalla' WHERE name LIKE '%Betong%' AND name LIKE '%Grimvagen%';
UPDATE pipeline_deal SET deal_type = 'intern', estimated_value = 260295, customer = 'RM Fasad', owner = 'MED', city = 'Jarfalla' WHERE name LIKE '%Mark%' AND name LIKE '%Grimvagen%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 7731180, customer = 'Ozu Bygg', owner = 'EBY', city = 'Varmdo' WHERE name LIKE 'Varmdo%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 250000, customer = 'Urban Lindskog', owner = 'JNO', city = 'Stocksund' WHERE name LIKE 'Kallargolv%';
UPDATE pipeline_deal SET deal_type = 'varm', estimated_value = 2000000, owner = '', city = '' WHERE name LIKE 'Campus%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 10000000, customer = 'Jan Lindgren', owner = 'MED', city = 'Trakvista' WHERE name LIKE 'Villa Trakvista%';
UPDATE pipeline_deal SET deal_type = 'varm', estimated_value = 325000, customer = 'BRF Rouletten', city = '' WHERE name LIKE 'Rouletten%';
UPDATE pipeline_deal SET deal_type = 'styrd', estimated_value = 1225400, customer = 'Rocmore', owner = 'JNO', city = 'Solna' WHERE name LIKE 'RAM Solna%';
UPDATE pipeline_deal SET deal_type = 'styrd', estimated_value = 4914100, customer = 'Rocmore', owner = 'JNO', city = 'Stockholm' WHERE name LIKE 'Lappkarrsberget%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 3500000, customer = 'Malardalens Projektplanering', owner = 'JNO', city = 'Norra lanken' WHERE name LIKE 'Grus%';
UPDATE pipeline_deal SET deal_type = 'varm', estimated_value = 14686531, customer = 'G17 Gruppen', city = 'Vallentuna' WHERE name LIKE 'LSS G17%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 11312562, customer = 'Marbit AB', owner = 'JNO', city = 'Hasselby' WHERE name LIKE 'Hasselby%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 525200, customer = 'Tetrad AB', owner = 'JNO', city = 'Norsborg' WHERE name LIKE 'Oljeavskiljare%';
UPDATE pipeline_deal SET deal_type = 'kall', estimated_value = 264500, customer = 'Ankan bygg', city = 'Varmdo' WHERE name LIKE 'Bjorklövsstigen%';
UPDATE pipeline_deal SET deal_type = 'service', estimated_value = 125000, customer = 'Ikano' WHERE name LIKE 'Brf Vakna%';

-- Verify
SELECT deal_type, COUNT(*), SUM(estimated_value) AS total_est FROM pipeline_deal WHERE company_code = 'RM' GROUP BY deal_type ORDER BY deal_type;

SQL

echo "PIPELINE MODEL COMPLETE"
