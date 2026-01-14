-- =========================================================
-- Script Name   : qc_positive_amounts.sql
-- Author        : Ravikanth Reddy
-- Owner         : Finance Governance
-- Steward       : Finance Data Steward
-- Domain        : NYC Taxi – Transactions
-- Purpose       : Validate that all monetary amounts
--                 (fare, tip, total) are non-negative
--
-- Quality Rule  : Amounts must be >= 0
-- Rule Type     : Data Quality – Accuracy
-- Severity      : HIGH
--
-- Pass Condition:
--   negative_amount_count = 0
--
-- Failure Action:
--   - Fail quality gate
--   - Block downstream transformations
--   - Notify steward
--
-- Execution     : Pre-transformation quality gate
-- Created Date  : 2026-01-12
-- =========================================================


SELECT COUNT(*) AS negative_amount_count
FROM curated.yellow_tripdata
WHERE total_amount < 0;
