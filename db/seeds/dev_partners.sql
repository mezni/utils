-- Seed Data: Partners
-- Purpose: Development seed data for partners
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- Clear existing data for idempotent re-runs
TRUNCATE inventory.partner CASCADE;

-- Partner A: Based in Tunis
INSERT INTO inventory.partner (id, name, created_at) VALUES
    ('PRT-A7xL', 'Tunis Charging Solutions', now()),
    ('PRT-B2mN', 'Sfax Electric Power', now()),
    ('PRT-C9kP', 'Sousse Mobility Network', now());

-- Verify insertion
SELECT 'Partners inserted:' AS message, COUNT(*) AS count FROM inventory.partner;
