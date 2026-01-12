-- =========================================================
-- Script Name   : qc_vendor_fk.sql
-- Author        : Ravikanth Reddy
-- Owner         : Master Data Governance
-- Steward       : Vendor Data Steward
-- Domain        : Vendor Master
-- Purpose       : Ensure all vendor IDs in transactions
--                 exist in vendor master data
--
-- Quality Rule  : Referential integrity enforced
-- Rule Type     : Data Quality – Integrity
-- Severity      : CRITICAL
--
-- Pass Condition:
--   orphan_vendor_count = 0
--
-- Failure Action:
--   - Block SCD merge
--   - Require steward review
--
-- Execution     : Pre-SCD validation
-- Created Date  : 2026-01-12
-- =========================================================


SELECT COUNT(*) AS orphan_count
FROM curated.yellow_tripdata t
LEFT JOIN master.vendor_master v
  ON t.vendorid = v.vendorid
WHERE v.vendorid IS NULL;
