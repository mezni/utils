-- Migration 0001: Database Extensions
-- Purpose: Install required PostgreSQL extensions
-- Author: BorneMap Development Team
-- Date: 2026-06-07

-- PostGIS for spatial data support
CREATE EXTENSION IF NOT EXISTS postgis;

-- uuid-ossp for generating UUIDs (fallback from NanoID if needed)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- pgcrypto for encryption/decryption functions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
