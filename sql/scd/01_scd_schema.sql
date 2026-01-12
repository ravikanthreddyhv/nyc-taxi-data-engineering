-- =========================================================
-- Script Name   : 01_scd_schema.sql
-- Author        : Ravikanth Reddy
-- Owner         : Master Data Governance
-- Steward       : MDM Steward
-- Domain        : Vendor Master
-- Purpose       : Create SCD Type 2 tables with
--                 versioning and audit metadata
--
-- SCD Type      : Type 2 (Historical Tracking)
-- Key Columns  :
--   - entity_id
--   - version_no
--   - is_current
--
-- Execution     : One-time schema setup
-- Created Date  : 2026-01-12
-- =========================================================


CREATE SCHEMA IF NOT EXISTS master;
CREATE SCHEMA IF NOT EXISTS audit;

-- Current + history in one table (SCD2)
CREATE TABLE IF NOT EXISTS master.customer_master_scd2 (
    sk_customer          BIGSERIAL PRIMARY KEY,     -- surrogate key
    customer_id          TEXT NOT NULL,             -- business key

    full_name            TEXT,
    email                TEXT,
    phone                TEXT,
    address              TEXT,

    -- SCD2 metadata
    effective_start_ts   TIMESTAMP NOT NULL DEFAULT NOW(),
    effective_end_ts     TIMESTAMP NULL,
    is_current           BOOLEAN NOT NULL DEFAULT TRUE,
    version_no           INT NOT NULL DEFAULT 1,

    -- governance metadata
    record_hash          TEXT NOT NULL,             -- to detect changes
    created_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by           TEXT NOT NULL DEFAULT 'system',
    updated_at           TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_by           TEXT NOT NULL DEFAULT 'system',

    approval_status      TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/APPROVED/REJECTED
    approved_by          TEXT NULL,
    approved_at          TIMESTAMP NULL,
    approval_reason      TEXT NULL,

    CONSTRAINT uq_customer_current UNIQUE(customer_id, is_current)
);

CREATE INDEX IF NOT EXISTS idx_customer_scd2_bk ON master.customer_master_scd2(customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_scd2_current ON master.customer_master_scd2(is_current);
