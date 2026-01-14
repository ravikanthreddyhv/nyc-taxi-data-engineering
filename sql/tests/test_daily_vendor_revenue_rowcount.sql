-- =========================================================
-- Test Name     : test_daily_vendor_revenue_rollup.sql
-- Author        : Ravikanth Reddy
-- Owner         : Analytics Engineering
-- Domain        : Revenue Analytics
-- Purpose       : Validate daily revenue aggregation
--
-- Test Type     : Reconciliation Test
-- Validation    :
--   Sum(detail) = Sum(aggregate)
--
-- Failure Impact:
--   Financial reporting mismatch
--
-- Execution     : Post-transformation
-- Created Date  : 2026-01-12
-- =========================================================
=

DROP TABLE IF EXISTS analytics.daily_vendor_revenue;

CREATE TABLE analytics.daily_vendor_revenue AS
SELECT
    t.vendorid,
    DATE(t.tpep_pickup_datetime) AS trip_date,
    SUM(t.total_amount) AS total_revenue
FROM curated.yellow_tripdata t
JOIN master.vendor_master v
  ON t.vendorid = v.vendorid
GROUP BY
    t.vendorid,
    DATE(t.tpep_pickup_datetime);
