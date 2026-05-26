CREATE TYPE user_role AS ENUM ('admin', 'partner', 'driver');
CREATE TYPE partner_classification AS ENUM ('business', 'private');
CREATE TYPE current_type AS ENUM ('AC', 'DC');
CREATE TYPE charger_status AS ENUM ('available', 'occupied', 'faulted', 'offline');
