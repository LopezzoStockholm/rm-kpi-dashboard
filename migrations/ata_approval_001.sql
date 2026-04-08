-- ÄTA approval flow migration 001
-- Token-baserat godkännande + audit-logg
-- 2026-04-05

CREATE TABLE IF NOT EXISTS ata_approval_token (
    token VARCHAR(80) PRIMARY KEY,
    ata_id INTEGER NOT NULL REFERENCES ata_register(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    recipient_email VARCHAR(255) NOT NULL,
    recipient_name VARCHAR(255),
    sent_by VARCHAR(100) NOT NULL,
    decision VARCHAR(20),
    approver_ip VARCHAR(64),
    approver_name VARCHAR(255),
    approver_user_agent TEXT,
    rejection_reason TEXT,
    email_message_id VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_ata_approval_token_ata ON ata_approval_token(ata_id);
CREATE INDEX IF NOT EXISTS idx_ata_approval_token_expires ON ata_approval_token(expires_at) WHERE used_at IS NULL;

CREATE TABLE IF NOT EXISTS ata_audit_log (
    id SERIAL PRIMARY KEY,
    ata_id INTEGER NOT NULL REFERENCES ata_register(id) ON DELETE CASCADE,
    event_type VARCHAR(40) NOT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor VARCHAR(255) NOT NULL,
    actor_type VARCHAR(20) NOT NULL,
    ip_address VARCHAR(64),
    user_agent TEXT,
    old_status VARCHAR(40),
    new_status VARCHAR(40),
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_ata_audit_log_ata ON ata_audit_log(ata_id);
CREATE INDEX IF NOT EXISTS idx_ata_audit_log_event_at ON ata_audit_log(event_at DESC);

-- Lägg till kolumner på ata_register om de saknas
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS customer_email VARCHAR(255);
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS sent_to_customer_at TIMESTAMPTZ;
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS sent_by VARCHAR(100);
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS customer_decision VARCHAR(20);
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS customer_decision_at TIMESTAMPTZ;
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS customer_rejection_reason TEXT;
ALTER TABLE ata_register ADD COLUMN IF NOT EXISTS approval_pdf_path TEXT;
