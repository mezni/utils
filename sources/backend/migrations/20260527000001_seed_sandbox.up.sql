-- Deterministic seed data for sandbox environments
-- All records carry is_test = true per Constitution Principle V

-- Connector Types
INSERT INTO connector_types (id, name, description, is_test) VALUES
    ('CNT-seed00000001', 'Type 2 AC', 'IEC 62196 Type 2 connector for AC charging up to 43 kW', true),
    ('CNT-seed00000002', 'CCS 2 DC', 'Combined Charging System Type 2 for DC fast charging up to 350 kW', true);

-- Admin User
INSERT INTO users (id, email, username, password_hash, role, is_test) VALUES
    ('USR-seedadmin01', 'admin@bornemap.tn', 'admin', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'admin', true);

-- Partner Users
INSERT INTO users (id, email, username, password_hash, role, is_test) VALUES
    ('USR-seedprt00001', 'partner1@bornemap.tn', 'partner1', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'partner', true),
    ('USR-seedprt00002', 'partner2@bornemap.tn', 'partner2', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'partner', true),
    ('USR-seedprt00003', 'partner3@bornemap.tn', 'partner3', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'partner', true),
    ('USR-seedprt00004', 'partner4@bornemap.tn', 'partner4', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'partner', true),
    ('USR-seedprt00005', 'partner5@bornemap.tn', 'partner5', '$argon2id$v=19$m=19456,t=2,p=1$placeholder', 'partner', true);

-- Partner Profiles
INSERT INTO partner_profiles (id, user_id, classification, display_name, tax_id, contact_phone, is_test) VALUES
    ('PRT-seedprt00001', 'USR-seedprt00001', 'business', 'TunisEV Solutions', 'TN123456789', '+21671111111', true),
    ('PRT-seedprt00002', 'USR-seedprt00002', 'business', 'Sfax Charge Networks', 'TN987654321', '+21671222222', true),
    ('PRT-seedprt00003', 'USR-seedprt00003', 'private', 'Ahmed Ben Ali', NULL, '+21671333333', true),
    ('PRT-seedprt00004', 'USR-seedprt00004', 'business', 'NordTunis Énergie', 'TN456789123', '+21671444444', true),
    ('PRT-seedprt00005', 'USR-seedprt00005', 'private', 'Sarra Khelifa', NULL, '+21671555555', true);

-- Stations (100 stations across Tunisia)
INSERT INTO stations (id, owner_id, name, address, city, coordinates, is_operational, is_test)
SELECT
    'STN-seed' || LPAD(CAST(n AS TEXT), 8, '0'),
    CASE WHEN n % 5 = 1 THEN 'USR-seedprt00001'
         WHEN n % 5 = 2 THEN 'USR-seedprt00002'
         WHEN n % 5 = 3 THEN 'USR-seedprt00003'
         WHEN n % 5 = 4 THEN 'USR-seedprt00004'
         ELSE 'USR-seedprt00005'
    END,
    'Station ' || n,
    n || ' Rue de la Charge',
    CASE WHEN n <= 30 THEN 'Tunis'
         WHEN n <= 50 THEN 'Sfax'
         WHEN n <= 70 THEN 'Sousse'
         WHEN n <= 85 THEN 'Nabeul'
         ELSE 'Bizerte'
    END,
    ST_SetSRID(ST_MakePoint(
        10.0 + (n % 10) * 0.05,
        36.5 + (n / 10) * 0.03
    ), 4326),
    n % 10 != 0,
    true
FROM generate_series(1, 100) AS n;

-- Chargers (300 chargers, 3 per station)
INSERT INTO chargers (id, station_id, connector_type_id, power_kw, current_type, status)
SELECT
    'CHG-seed' || LPAD(CAST((s * 3 + c) AS TEXT), 8, '0'),
    'STN-seed' || LPAD(CAST(s AS TEXT), 8, '0'),
    CASE WHEN c % 2 = 0 THEN 'CNT-seed00000001' ELSE 'CNT-seed00000002' END,
    CASE WHEN c % 2 = 0 THEN 22.0 ELSE 150.0 END,
    CASE WHEN c % 2 = 0 THEN 'AC' ELSE 'DC' END,
    CASE WHEN c = 2 THEN 'faulted' ELSE 'available' END
FROM generate_series(1, 100) AS s, generate_series(0, 2) AS c;
