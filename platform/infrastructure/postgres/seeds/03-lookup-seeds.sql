-- Seed lookup tables

INSERT INTO inventory.data_sources (name, description) VALUES
    ('osm', 'OpenStreetMap Overpass API import'),
    ('manual', 'Manually entered by partner or admin'),
    ('partner', 'Submitted via partner portal')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.access_types (name, description) VALUES
    ('owner', 'Full access to manage partner profile and all stations'),
    ('operator', 'Can manage stations and view data'),
    ('viewer', 'Read-only access')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.connector_types (name, description) VALUES
    ('Type2', 'IEC 62196 Type 2 - Mennekes'),
    ('CCS', 'Combined Charging System'),
    ('CHAdeMO', 'CHAdeMO DC fast charging'),
    ('Type1', 'SAE J1772 Type 1'),
    ('Schuko', 'Schuko domestic plug (slow charge)')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.current_types (name, description) VALUES
    ('AC', 'Alternating Current'),
    ('DC', 'Direct Current')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.connector_statuses (name) VALUES
    ('available'),
    ('in_use'),
    ('offline'),
    ('out_of_order')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.station_statuses (name) VALUES
    ('active'),
    ('inactive'),
    ('planned'),
    ('closed')
ON CONFLICT (name) DO NOTHING;

INSERT INTO inventory.charger_statuses (name) VALUES
    ('online'),
    ('offline'),
    ('faulted')
ON CONFLICT (name) DO NOTHING;
