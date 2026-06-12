-- 005-seed-data.sql
-- Target: platform_db
-- Purpose: Tunisia test stations and chargers
-- Idempotent: YES (IF NOT EXISTS guards)

INSERT INTO inventory.partner (id, name, contact_email)
VALUES
    ('PRT-totalenergies-tn', 'TotalEnergies TN', 'ev-charging@totalenergies.tn'),
    ('PRT-steg', 'STEG', 'bornes@steg.com.tn')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.station (id, name, address, lat, lng, status, opening_hours, partner_id)
VALUES
    ('STA-tunis-centre', 'TotalEnergies Tunis Centre', 'Avenue Habib Bourguiba, Tunis', 36.7995, 10.1806, 'available', '06:00-23:00', 'PRT-totalenergies-tn'),
    ('STA-sfax-borj', 'STEG Sfax Borj', 'Route de Gabès, Sfax', 34.7400, 10.7600, 'available', '07:00-22:00', 'PRT-steg'),
    ('STA-sousse-corniche', 'TotalEnergies Sousse Corniche', 'Boulevard du 14 Janvier, Sousse', 35.8250, 10.6386, 'busy', '06:00-00:00', 'PRT-totalenergies-tn'),
    ('STA-tunis-lac', 'STEG Tunis Lac', 'Les Berges du Lac, Tunis', 36.8340, 10.2420, 'offline', '07:00-21:00', 'PRT-steg'),
    ('STA-nabeul', 'TotalEnergies Nabeul', 'Avenue Habib Bourguiba, Nabeul', 36.4512, 10.7358, 'available', '07:00-22:00', 'PRT-totalenergies-tn')
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventory.charger (id, station_id, type, power_kw, status, price_per_kwh)
VALUES
    -- Tunis Centre: CCS2 + CHAdeMO
    ('CHR-tc-ccs2-1', 'STA-tunis-centre', 'CCS2', 150, 'available', 0.45),
    ('CHR-tc-chademo-1', 'STA-tunis-centre', 'CHAdeMO', 100, 'available', 0.40),

    -- Sfax Borj: CCS2 + Type2
    ('CHR-sb-ccs2-1', 'STA-sfax-borj', 'CCS2', 120, 'available', 0.35),
    ('CHR-sb-type2-1', 'STA-sfax-borj', 'Type2', 22, 'available', 0.30),

    -- Sousse Corniche: CCS2 + CHAdeMO + Type2
    ('CHR-sc-ccs2-1', 'STA-sousse-corniche', 'CCS2', 150, 'busy', 0.50),
    ('CHR-sc-chademo-1', 'STA-sousse-corniche', 'CHAdeMO', 100, 'busy', 0.45),
    ('CHR-sc-type2-1', 'STA-sousse-corniche', 'Type2', 22, 'available', 0.38),

    -- Tunis Lac: Type2 only (offline station)
    ('CHR-tl-type2-1', 'STA-tunis-lac', 'Type2', 22, 'offline', 0.28),

    -- Nabeul: CCS2 + Type2
    ('CHR-nb-ccs2-1', 'STA-nabeul', 'CCS2', 100, 'available', 0.42),
    ('CHR-nb-type2-1', 'STA-nabeul', 'Type2', 22, 'available', 0.35)
ON CONFLICT (id) DO NOTHING;
