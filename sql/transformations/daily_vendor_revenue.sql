-- =========================================================
-- Script Name   : daily_vendor_revenue.sql
-- Author        : Ravikanth Reddy
-- Owner         : Finance Analytics
-- Steward       : Finance Data Steward
-- Domain        : Revenue Analytics
-- Purpose       : Aggregate daily revenue metrics
--                 per vendor
--
-- Source Tables :
--   - curated.nyc_taxi_trips
-- Target Table  :
--   - analytics.daily_vendor_revenue
--
-- Grain         : vendor_id, business_date
-- Refresh Type : Daily Incremental
--
-- Dependencies :
--   - qc_positive_amounts.sql
--   - qc_vendor_fk.sql
--
-- Created Date : 2026-01-12
-- =========================================================


DROP TABLE IF EXISTS analytics.daily_vendor_revenue;

CREATE TABLE analytics.daily_vendor_revenue AS
WITH base_trips AS (
    SELECT
        vendorid,
        DATE(tpep_pickup_datetime) AS trip_date,
        total_amount
    FROM curated.yellow_tripdata
    WHERE total_amount > 0
),
active_vendors AS (
    SELECT
        vendorid
    FROM master.vendor_master
    WHERE is_active = true
)
SELECT
    b.vendorid,
    b.trip_date,
    SUM(b.total_amount) AS total_revenue,
    COUNT(*) AS trip_count
FROM base_trips b
JOIN active_vendors v
  ON b.vendorid = v.vendorid
GROUP BY
    b.vendorid,
    b.trip_date;
