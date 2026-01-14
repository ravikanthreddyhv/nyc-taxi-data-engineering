-- =========================================================
-- Script Name   : 02_scd_procedures.sql
-- Author        : Ravikanth Reddy
-- Owner         : Master Data Governance
-- Steward       : MDM Steward
-- Domain        : Vendor Master
-- Purpose       : Implement stored procedures for
--                 SCD Type 2 insert, update, expire logic
--
-- Includes      :
--   - Insert new version
--   - Expire current record
--   - Maintain version numbers
--
-- Execution     : Invoked by ETL or orchestration layer
-- Created Date  : 2026-01-12
-- =========================================================

CREATE OR REPLACE FUNCTION master.scd2_upsert_customer(
    p_customer_id   TEXT,
    p_full_name     TEXT,
    p_email         TEXT,
    p_phone         TEXT,
    p_address       TEXT,
    p_actor         TEXT DEFAULT 'glue_job'
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_hash TEXT;
    v_new_hash     TEXT;
    v_current_ver  INT;
BEGIN
    -- Build hash to detect changes (simple + stable)
    v_new_hash := md5(
        coalesce(p_full_name,'') || '|' ||
        coalesce(p_email,'')     || '|' ||
        coalesce(p_phone,'')     || '|' ||
        coalesce(p_address,'')
    );

    SELECT record_hash, version_no
      INTO v_current_hash, v_current_ver
      FROM master.customer_master_scd2
     WHERE customer_id = p_customer_id
       AND is_current = TRUE
     LIMIT 1;

    -- If no current row exists: insert initial
    IF v_current_hash IS NULL THEN
        INSERT INTO master.customer_master_scd2 (
            customer_id, full_name, email, phone, address,
            record_hash,
            effective_start_ts, effective_end_ts, is_current, version_no,
            created_by, updated_by,
            approval_status
        )
        VALUES (
            p_customer_id, p_full_name, p_email, p_phone, p_address,
            v_new_hash,
            NOW(), NULL, TRUE, 1,
            p_actor, p_actor,
            'PENDING'
        );
        RETURN;
    END IF;

    -- If hash is same: nothing to do (idempotent)
    IF v_current_hash = v_new_hash THEN
        RETURN;
    END IF;

    -- Expire current row
    UPDATE master.customer_master_scd2
       SET effective_end_ts = NOW(),
           is_current = FALSE,
           updated_at = NOW(),
           updated_by = p_actor
     WHERE customer_id = p_customer_id
       AND is_current = TRUE;

    -- Insert new version row
    INSERT INTO master.customer_master_scd2 (
        customer_id, full_name, email, phone, address,
        record_hash,
        effective_start_ts, effective_end_ts, is_current, version_no,
        created_by, updated_by,
        approval_status
    )
    VALUES (
        p_customer_id, p_full_name, p_email, p_phone, p_address,
        v_new_hash,
        NOW(), NULL, TRUE, v_current_ver + 1,
        p_actor, p_actor,
        'PENDING'
    );
END;
$$;
