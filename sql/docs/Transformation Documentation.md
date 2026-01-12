1. Overview

Transformation Name: daily_vendor_revenue
Layer: Analytics / Gold
Type: Aggregation Transformation
Frequency: Daily
Refresh Mode: Incremental (by business date)

This transformation calculates daily revenue metrics per vendor using curated NYC taxi trip data.
It serves as a finance-ready dataset for reporting, dashboards, and governance audits.

2. Business Purpose

The purpose of this transformation is to:

Provide daily revenue visibility per vendor

Support financial reporting and reconciliation

Enable vendor performance analysis

Serve as a trusted, governed metric for downstream analytics

This dataset is consumed by:

Finance dashboards

Vendor scorecards

Revenue reconciliation workflows

3. Source Data
Source Table(s)
Table Name	Layer	Description
curated.nyc_taxi_trips	Curated (Silver)	Cleaned and standardized taxi trip records
Source Filters

Records must pass data quality checks

Only valid vendor IDs are included

Monetary values must be non-negative

4. Target Data
Target Table
Table Name	Layer	Storage
analytics.daily_vendor_revenue	Analytics (Gold)	Parquet
Grain (Uniqueness)
vendor_id + business_date


Each row represents one vendor’s revenue for one day.

5. Transformation Logic
Business Date Derivation

business_date is derived from pickup_datetime

Metrics Calculated
Column	Description
trip_count	Total trips per vendor per day
total_fare_amount	Sum of fare amounts
total_tip_amount	Sum of tip amounts
total_revenue	fare + tip + surcharges
avg_trip_distance	Average trip distance
avg_trip_duration	Average trip duration
Aggregation Logic (High Level)
GROUP BY
  vendor_id,
  business_date

6. Data Quality Controls

This transformation does not run unless all quality gates pass.

Pre-Transformation Checks
Check	File	Rule
Positive amounts	qc_positive_amounts.sql	No negative monetary values
Vendor FK integrity	qc_vendor_fk.sql	All vendor IDs exist in master
Failure Handling

If any quality check fails:

Transformation is blocked

Data is not written

Steward review is required

7. Governance & Ownership
Attribute	Value
Data Owner	Finance Governance
Data Steward	Finance Data Steward
Domain	Revenue Analytics
Criticality	HIGH
Compliance	SOX-aligned
8. Versioning & Change Management

Transformation logic is version-controlled in Git

Any logic change requires:

PR review

Steward approval

Re-execution of quality tests

Version Metadata (in SQL header)

Author

Owner

Purpose

Dependencies

Creation date

9. Dependencies
Upstream Dependencies

curated.nyc_taxi_trips

Vendor master SCD table

Downstream Consumers

Revenue dashboards

Finance reconciliation jobs

Vendor performance reports

10. Execution & Orchestration
Execution Layer

AWS Glue (Spark SQL)

Triggered via orchestration pipeline (Step Functions – planned)

Execution Order

Quality checks

Vendor FK validation

Revenue aggregation

Post-transformation tests

11. Audit & Lineage

This transformation supports full auditability:

Source → Curated → Analytics lineage

Quality rule linkage

Ownership metadata

Execution timestamps (via Glue)

Auditors can trace:

Raw trip → Curated trip → Daily vendor revenue

12. Known Assumptions

Vendor IDs are stable and governed

All monetary values are in the same currency

Late-arriving data handled in future enhancement

13. Future Enhancements

Incremental processing with bookmarks

Late-arrival adjustment logic

Partitioning by business_date

SLA breach alerts

14. Approval
Role	Status
Data Steward	Pending
Finance Owner	Pending
Platform	Approved
