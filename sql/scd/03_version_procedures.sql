-- =========================================================
-- Audit table for version actions
-- =========================================================
CREATE TABLE IF NOT EXISTS audit.version_actions (
    action_id      BIGSERIAL PRIMARY KEY,
    entity_name    TEXT NOT NULL,
    record_id      TEXT NOT NULL,        -- business key (customer_id)
    version_no     INT NOT NULL,
    action_type    TEXT NOT NULL,        -- APPROVE / ROLLBACK
    actor          TEXT NOT NULL,
    reason         TEXT,
    action_ts      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- =========================================================
-- approve_version(record_id, approver, reason)
-- Approves the CURRENT version for that record.
-- =========================================================
CREATE OR REPLACE FUNCTION master.approve_version(
    p_customer_id TEXT,
    p_approver    TEXT,
    p_reason      TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_ver INT;
BEGIN
    SELECT version_no INTO v_ver
      FROM master.customer_master_scd2
     WHERE customer_id = p_customer_id
       AND is_current = TRUE
     LIMIT 1;

    IF v_ver IS NULL THEN
        RAISE EXCEPTION 'No current version found for %', p_customer_id;
    END IF;

    UPDATE master.customer_master_scd2
       SET approval_status = 'APPROVED',
           approved_by = p_approver,
           approved_at = NOW(),
           approval_reason = p_reason,
           updated_at = NOW(),
           updated_by = p_approver
     WHERE customer_id = p_customer_id
       AND is_current = TRUE;

    INSERT INTO audit.version_actions(entity_name, record_id, version_no, action_type, actor, reason)
    VALUES ('customer_master_scd2', p_customer_id, v_ver, 'APPROVE', p_approver, p_reason);
END;
$$;

-- =========================================================
-- rollback_version(record_id, target_version, reason)
-- Makes target_version CURRENT and expires current.
-- =========================================================
CREATE OR REPLACE FUNCTION master.rollback_version(
    p_customer_id     TEXT,
    p_target_version  INT,
    p_actor           TEXT,
    p_reason          TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_exists INT;
    v_current_ver INT;
BEGIN
    SELECT version_no INTO v_current_ver
      FROM master.customer_master_scd2
     WHERE customer_id = p_customer_id
       AND is_current = TRUE
     LIMIT 1;

    IF v_current_ver IS NULL THEN
        RAISE EXCEPTION 'No current version found for %', p_customer_id;
    END IF;

    SELECT 1 INTO v_exists
      FROM master.customer_master_scd2
     WHERE customer_id = p_customer_id
       AND version_no = p_target_version
     LIMIT 1;

    IF v_exists IS NULL THEN
        RAISE EXCEPTION 'Target version % not found for %', p_target_version, p_customer_id;
    END IF;

    -- expire current
    UPDATE master.customer_master_scd2
       SET effective_end_ts = NOW(),
           is_current = FALSE,
           updated_at = NOW(),
           updated_by = p_actor
     WHERE customer_id = p_customer_id
       AND is_current = TRUE;

    -- reactivate target (create a "new" current copy to preserve lineage)
    INSERT INTO master.customer_master_scd2 (
        customer_id, full_name, email, phone, address,
        record_hash,
        effective_start_ts, effective_end_ts, is_current, version_no,
        created_by, updated_by,
        approval_status, approved_by, approved_at, approval_reason
    )
    SELECT
        customer_id, full_name, email, phone, address,
        record_hash,
        NOW(), NULL, TRUE, (v_current_ver + 1),
        p_actor, p_actor,
        'APPROVED', p_actor, NOW(), ('Rollback to version ' || p_target_version || ': ' || p_reason)
    FROM master.customer_master_scd2
    WHERE customer_id = p_customer_id
      AND version_no = p_target_version
    LIMIT 1;

    INSERT INTO audit.version_actions(entity_name, record_id, version_no, action_type, actor, reason)
    VALUES ('customer_master_scd2', p_customer_id, (v_current_ver + 1), 'ROLLBACK', p_actor, p_reason);
END;
$$;

-- =========================================================
-- audit_version_history(record_id, date_range)
-- Return all versions and actions for a date range.
-- =========================================================
CREATE OR REPLACE FUNCTION master.audit_version_history(
    p_customer_id TEXT,
    p_from_ts     TIMESTAMP,
    p_to_ts       TIMESTAMP
)
RETURNS TABLE (
    customer_id TEXT,
    version_no INT,
    is_current BOOLEAN,
    effective_start_ts TIMESTAMP,
    effective_end_ts TIMESTAMP,
    approval_status TEXT,
    approved_by TEXT,
    approved_at TIMESTAMP,
    action_type TEXT,
    action_actor TEXT,
    action_reason TEXT,
    action_ts TIMESTAMP
)
LANGUAGE sql
AS $$
    SELECT
        s.customer_id,
        s.version_no,
        s.is_current,
        s.effective_start_ts,
        s.effective_end_ts,
        s.approval_status,
        s.approved_by,
        s.approved_at,
        a.action_type,
        a.actor as action_actor,
        a.reason as action_reason,
        a.action_ts
    FROM master.customer_master_scd2 s
    LEFT JOIN audit.version_actions a
      ON a.record_id = s.customer_id
     AND a.entity_name = 'customer_master_scd2'
    WHERE s.customer_id = p_customer_id
      AND s.effective_start_ts BETWEEN p_from_ts AND p_to_ts
    ORDER BY s.version_no;
$$;
-- =========================================================
-- Script Name   : 03_version_procedures.sql
-- Author        : Ravikanth Reddy
-- Owner         : Data Governance
-- Steward       : Data Steward
-- Domain        : Master Data
-- Purpose       : Implement version lifecycle management
--
-- Procedures   :
--   - approve_version
--   - rollback_version
--   - audit_version_history
--
-- Governance   :
--   Manual approval required for production versions
--
-- Execution    : Steward-driven or workflow-triggered
-- Created Date : 2026-01-12
-- =========================================================
