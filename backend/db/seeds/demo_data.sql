INSERT INTO partners (id, name, type, contact_email, is_live) VALUES
  ('prt-a1b2c3d4', 'TotalEnergies Tunisia',  'Business', 'ev@totalenergies.tn',      true),
  ('prt-b2c3d4e5', 'Ola Energy Tunisia',     'Business', 'ev@olaenergy.tn',         true),
  ('prt-c3d4e5f6', 'STEG',                   'Private',  'mobilite@steg.tn',         true),
  ('prt-d4e5f6a7', 'Charge Tunisie',         'Business', 'hello@chargetunisie.tn',  true),
  ('prt-e5f6a7b8', 'GreenMotion Tunisia',    'Private',  'contact@greenmotion.tn',  true);

WITH regions (region_name, lat, lng, station_count) AS (
  VALUES
    ('Tunis',      36.8065, 10.1815, 15),
    ('Sfax',       34.7400, 10.7600, 10),
    ('Sousse',     35.8250, 10.6360, 10),
    ('Bizerte',    37.2740,  9.8730,  8),
    ('Gabès',      33.8810, 10.0980,  7)
),
partner_ids (id, idx) AS (
  SELECT id, ROW_NUMBER() OVER () - 1 FROM partners
),
station_data AS (
  SELECT
    'stn-' || LPAD(TO_HEX(generate_series + 1), 8, '0') AS id,
    r.region_name || ' Station ' || (generate_series + 1) AS name,
    (SELECT id FROM partner_ids WHERE idx = (generate_series + gs.offset) % 5) AS partner_id,
    r.lat + (random() - 0.5) * 0.04 AS station_lat,
    r.lng + (random() - 0.5) * 0.04 AS station_lng,
    CASE WHEN random() < 0.6 THEN 'Available' ELSE 'Occupied' END AS status,
    false AS is_live
  FROM regions r
  CROSS JOIN LATERAL (
    SELECT generate_series, SUM(station_count) OVER (ORDER BY region_name) - station_count AS offset
    FROM generate_series(0, r.station_count - 1)
    WHERE generate_series < r.station_count
  ) gs
)
INSERT INTO stations (id, name, partner_id, geom, status, is_live)
SELECT
  sd.id,
  sd.name,
  sd.partner_id,
  ST_SetSRID(ST_MakePoint(sd.station_lng, sd.station_lat), 4326)::geography,
  sd.status,
  sd.is_live
FROM station_data sd;

WITH all_stations AS (
  SELECT id FROM stations ORDER BY id
),
charger_data AS (
  SELECT
    'chg-' || LPAD(TO_HEX(ROW_NUMBER() OVER ()), 8, '0') AS id,
    s.id AS station_id,
    CASE WHEN ROW_NUMBER() OVER () % 3 = 0 THEN 'Type2' WHEN ROW_NUMBER() OVER () % 3 = 1 THEN 'CCS2' ELSE 'CHAdeMO' END AS plug_type,
    CASE WHEN ROW_NUMBER() OVER () % 3 = 0 THEN 22 WHEN ROW_NUMBER() OVER () % 3 = 1 THEN 120 ELSE 50 END AS power_output,
    CASE WHEN random() < 0.6 THEN 'Available' ELSE 'Occupied' END AS status,
    false AS is_live
  FROM all_stations s
  CROSS JOIN LATERAL generate_series(1, 2) AS gs
)
INSERT INTO chargers (id, station_id, plug_type, power_output, status, is_live)
SELECT id, station_id, plug_type, power_output, status, is_live FROM charger_data;
