-- Migration 006: Seed data for lookup tables
-- Populates ENUM-style reference tables with initial values

INSERT INTO inventory.current_types (name, description) VALUES
    ('AC', 'Alternating Current'),
    ('DC', 'Direct Current')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.connector_types (name, description) VALUES
    ('Type2', 'IEC 62196 Type 2 — Mennekes'),
    ('CCS', 'Combined Charging System'),
    ('CHAdeMO', 'CHArge de MOve — Japanese standard')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.access_types (name, description) VALUES
    ('public', 'Open to all users'),
    ('restricted', 'Limited access — authorized users only'),
    ('private', 'Not publicly accessible')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.data_sources (name, description) VALUES
    ('manual', 'Manually entered by operator'),
    ('osm', 'Imported from OpenStreetMap'),
    ('partner', 'Provided via partner API integration')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.connector_statuses (name, description) VALUES
    ('available', 'Charger is operational and available'),
    ('occupied', 'Charger is currently in use'),
    ('offline', 'Charger is not operational'),
    ('unknown', 'Status could not be determined')
ON CONFLICT (name) DO NOTHING;
