## NYC Taxi Analytics – Governance Summary

### Fact Table
- trip_fact
- Grain: One row per taxi trip

### Dimensions
- zone_dim (conformed)
  - Source: NYC TLC taxi_zones (golden dataset)
  - Business key: location_id
  - Surrogate key: zone_key
  - UNKNOWN row enforced (location_id = -1)

### Key Mapping Rules
- trip_fact.pickup_zone_key → zone_dim.location_id
- trip_fact.dropoff_zone_key → zone_dim.location_id

### Data Quality Controls
- Orphan zone checks enforced
- UNKNOWN zone usage monitored
- 0% unknown rate validated

### Analytics Access
- Use analytics.v_trip_fact_conformed
- Raw tables restricted to ETL only

### performance:
  distribution_key: pickup_zone_key
  sort_key: pickup_datetime
  rationale:
    - colocated joins with zone_dim
    - time-series pruning
    - optimal hash joins

