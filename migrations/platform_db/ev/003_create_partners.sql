CREATE TABLE IF NOT EXISTS ev.partners (
    partner_id VARCHAR(16) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    partner_type VARCHAR(20) CHECK (partner_type IN ('INDIVIDUAL', 'COMPANY')),
    support_phone VARCHAR(50),
    support_email VARCHAR(255),
    is_verified BOOLEAN DEFAULT FALSE,
    created_by_uuid UUID,
    updated_by_uuid UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ
);
