-- setup_postgres.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- One-time PostgreSQL setup script for MediEquip Ghana data warehouse.
-- Run this ONCE as the postgres superuser before the first pipeline run.
--
-- USAGE:
--   psql -U postgres -f setup_postgres.sql
--
-- WHAT THIS CREATES:
--   1. The mediequip_dw database
--   2. The mediequip_dbt user (used by dbt and the ingestion script)
--   3. The pgcrypto extension (required for SHA-256 row hashing in macros)
--   4. The raw, staging, and marts schemas with correct permissions
-- ─────────────────────────────────────────────────────────────────────────────

-- ── 1. Create database ────────────────────────────────────────────────────────
CREATE DATABASE mediequip_dw
    ENCODING    'UTF8'
    LC_COLLATE  'en_US.UTF-8'
    LC_CTYPE    'en_US.UTF-8'
    TEMPLATE    template0;

-- ── 2. Create application user ────────────────────────────────────────────────
-- Replace 'your_strong_password_here' with the value of MEDIEQUIP_DB_PASSWORD
CREATE USER mediequip_dbt WITH
    PASSWORD Dnk@33street
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE;

-- ── 3. Connect to the new database for remaining setup ────────────────────────
\connect mediequip_dw

-- ── 4. Enable pgcrypto (required for row_hash() macro) ────────────────────────
-- pgcrypto provides DIGEST() for SHA-256 hashing used in all five mart models.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ── 5. Create schemas ─────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;        -- ingest_from_s3.py writes here
CREATE SCHEMA IF NOT EXISTS staging;    -- dbt staging views land here
CREATE SCHEMA IF NOT EXISTS marts;      -- dbt fact tables — Power BI connects here

-- ── 6. Grant permissions to mediequip_dbt user ────────────────────────────────

-- Database-level
GRANT CONNECT ON DATABASE mediequip_dw TO mediequip_dbt;

-- Schema-level
GRANT USAGE, CREATE ON SCHEMA raw      TO mediequip_dbt;
GRANT USAGE, CREATE ON SCHEMA staging  TO mediequip_dbt;
GRANT USAGE, CREATE ON SCHEMA marts    TO mediequip_dbt;

-- Table-level (covers future tables created in each schema)
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO mediequip_dbt;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO mediequip_dbt;

ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO mediequip_dbt;

-- ── 7. Power BI read-only user (optional but recommended) ─────────────────────
-- Create a separate read-only user for Power BI to avoid exposing write credentials.
-- Uncomment and set a password when ready.
--
CREATE USER powerbi_reader WITH PASSWORD 'ama-data-engineer';
GRANT CONNECT ON DATABASE mediequip_dw TO powerbi_reader;
GRANT USAGE ON SCHEMA marts TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO powerbi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
GRANT SELECT ON TABLES TO powerbi_reader;

-- ── Verification ─────────────────────────────────────────────────────────────
\echo ''
\echo '=== Setup complete ==='
\echo 'Database  : mediequip_dw'
\echo 'User      : mediequip_dbt'
\echo 'Schemas   : raw, staging, marts'
\echo 'Extension : pgcrypto'
\echo ''
\echo 'Next steps:'
\echo '  1. Copy profiles.yml to ~/.dbt/profiles.yml'
\echo '  2. Set MEDIEQUIP_DB_PASSWORD environment variable'
\echo '  3. Run: dbt debug'
\echo '  4. Run: python ingest_from_s3.py'
\echo '  5. Run: dbt build'
