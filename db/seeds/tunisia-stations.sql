INSERT INTO stations (id, name, provider_id, provider_name, location, status, updated_at)
VALUES
  ('stn-e3b0c442', 'LES BERGES DU LAC 2 HUB', 'prv-k9x2m47a', 'TotalEnergies Tunisia',
   ST_SetSRID(ST_MakePoint(10.2321, 36.8324), 4326), 'Available', NOW()),
  ('stn-f4a1d553', 'TUNIS MARINE PLAZA', 'prv-m1n8b52c', 'Ola Energy',
   ST_SetSRID(ST_MakePoint(10.1912, 36.8010), 4326), 'Occupied', NOW());

INSERT INTO chargers (id, station_id, plug_type, power_output, status)
VALUES
  ('chg-7b2a19f4', 'stn-e3b0c442', 'CCS2', 120, 'Available'),
  ('chg-3a1b2c3d', 'stn-f4a1d553', 'CCS2', 120, 'Occupied');
