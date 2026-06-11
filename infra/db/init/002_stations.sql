-- Station seed data (Tunis area)
INSERT INTO inventory.station (id, partner_id, name, address, latitude, longitude)
VALUES
  ('STA-001', 'PRT-001', 'Central Tunis EV Hub', 'Avenue Habib Bourguiba, Tunis', 36.8065, 10.1815),
  ('STA-002', 'PRT-001', 'La Marsa Station', 'Rue de la Marsa, La Marsa', 36.8778, 10.3246),
  ('STA-003', 'PRT-001', 'Carthage EV Point', 'Route de Carthage, Carthage', 36.8581, 10.3198),
  ('STA-004', 'PRT-002', 'Ariana EcoCharge', 'Avenue de l''Ariana, Ariana', 36.8667, 10.1833),
  ('STA-005', 'PRT-002', 'Ben Arous Station', 'Rue de Ben Arous, Ben Arous', 36.7532, 10.2220),
  ('STA-006', 'PRT-002', 'Le Bardo Charger', 'Place du Bardo, Le Bardo', 36.8092, 10.1344),
  ('STA-007', 'PRT-003', 'Sidi Bou Said Spot', 'Rue Sidi Bou Said, Sidi Bou Said', 36.8718, 10.3428),
  ('STA-008', 'PRT-001', 'Tunis Airport EV', 'Aéroport Tunis-Carthage, Tunis', 36.8519, 10.2272),
  ('STA-009', 'PRT-002', 'El Menzah Station', 'Rue El Menzah, El Menzah', 36.8417, 10.1789),
  ('STA-010', 'PRT-003', 'Lac Tunis Charger', 'Les Berges du Lac, Tunis', 36.8342, 10.2417);
