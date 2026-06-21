CREATE TABLE inventory.partners (
    partner_id VARCHAR(32) PRIMARY KEY
        CHECK (partner_id ~ '^PAR-[A-Za-z0-9_-]{12}$'),

    name VARCHAR(255) NOT NULL,

    partner_type VARCHAR(20)
        CHECK (partner_type IN ('INDIVIDUAL','COMPANY')),

    support_phone VARCHAR(50),
    support_email VARCHAR(255)
        CHECK (
            support_email IS NULL
            OR support_email ~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'
        ),

    is_verified BOOLEAN DEFAULT FALSE,

    metadata JSONB NOT NULL DEFAULT '{}',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ,

    created_by VARCHAR(32),
    updated_by VARCHAR(32),

    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    deleted_by VARCHAR(32)
);

CREATE TABLE inventory.partner_users (
    partner_id VARCHAR(32) NOT NULL
        REFERENCES inventory.partners(partner_id)
        ON DELETE CASCADE,

    user_id VARCHAR(64) NOT NULL,

    access_type_id INT REFERENCES inventory.access_types(id),

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (partner_id, user_id)
);
