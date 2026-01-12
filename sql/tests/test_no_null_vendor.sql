-- =========================================================
-- Test Name     : test_no_null_vendor.sql
-- Author        : Ravikanth Reddy
-- Owner         : Data Engineering
-- Domain        : Vendor Master
-- Purpose       : Ensure no NULL vendor IDs exist
--
-- Test Type     : SQL Assertion
-- Expected Pass :
--   null_vendor_count = 0
--
-- Failure Impact:
--   Invalid master data
--   Downstream joins fail
--
-- Execution     : Post-SCD validation
-- Created Date  : 2026-01-12
-- =========================================================


SELECT COUNT(*) AS null_vendor_count
FROM curated.yellow_tripdata
WHERE vendorid IS NULL;

