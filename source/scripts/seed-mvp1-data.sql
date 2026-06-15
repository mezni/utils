INSERT INTO gis.osm_stations (id, name, address, coordinates, source, is_available) VALUES
(
    'SEED-TUNIS-001',
    'Tunis Central Station',
    'Avenue Habib Bourguiba, Tunis',
    ST_SetSRID(ST_MakePoint(10.1815, 36.8065), 4326),
    'SEED',
    TRUE
),
(
    'SEED-TUNIS-002',
    'La Marsa Charging Hub',
    'Rue de la Liberté, La Marsa',
    ST_SetSRID(ST_MakePoint(10.3200, 36.8800), 4326),
    'SEED',
    TRUE
),
(
    'SEED-TUNIS-003',
    'Sousse Station',
    'Boulevard du 14 Janvier, Sousse',
    ST_SetSRID(ST_MakePoint(10.6370, 35.8250), 4326),
    'SEED',
    TRUE
),
(
    'SEED-TUNIS-004',
    'Sfax City Charger',
    'Route de l''Aéroport, Sfax',
    ST_SetSRID(ST_MakePoint(10.7700, 34.7400), 4326),
    'SEED',
    TRUE
),
(
    'SEED-TUNIS-005',
    'Carthage Byrsa Point',
    'Route de La Goulette, Carthage',
    ST_SetSRID(ST_MakePoint(10.3300, 36.8500), 4326),
    'SEED',
    TRUE
);
