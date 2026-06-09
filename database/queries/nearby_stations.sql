-- Query: Nearby stations with partner visibility filter
-- Only returns stations from verified, live, active partners

EXPLAIN ANALYZE
SELECT s.id, s.name, s.latitude, s.longitude,
       ST_Distance(s.location::geography, ST_SetSRID(ST_MakePoint(10.1815, 36.8008), 4326)::geography) AS distance_meters
FROM "ev-platform".station s
JOIN "ev-platform".partner p ON s.partner_id = p.id
WHERE p.is_verified = true
  AND p.is_live = true
  AND p.is_active = true
  AND ST_DWithin(s.location, ST_SetSRID(ST_MakePoint(10.1815, 36.8008), 4326)::geography, 100000)
ORDER BY distance_meters;
