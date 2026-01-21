#!/usr/bin/env python3
"""
Glue-2: validated_input  -> curated_output (Parquet)

Option A compliant:
- NO RUN_ID dependency
- Discovers latest validated partition automatically
- Safe joins (no column collisions)
- Governance-safe enrichment
"""

import sys
import re
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime


# =================================================
# Helpers
# =================================================
def require_cols(df: DataFrame, cols: list, df_name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")


def normalize_column_names(df: DataFrame) -> DataFrame:
    cleaned = []
    for c in df.columns:
        c2 = c.strip()
        c2 = re.sub(r"[^0-9a-zA-Z_]+", "_", c2)
        c2 = re.sub(r"_+", "_", c2)
        c2 = c2.strip("_").lower()
        cleaned.append(c2)

    seen = {}
    final = []
    for c in cleaned:
        if c not in seen:
            seen[c] = 0
            final.append(c)
        else:
            seen[c] += 1
            final.append(f"{c}_{seen[c]}")

    for old, new in zip(df.columns, final):
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


# =================================================
# Main
# =================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# -------------------------------------------------
# Paths
# -------------------------------------------------
BUCKET = "nyc-taxi-data-dev-ravikanth-us-east-1"
metrics_run_ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")

VALIDATED_BASE = f"s3://{BUCKET}/validated_input/"
REFERENCE_PATH = f"s3://{BUCKET}/reference/"
CURATED_PATH   = f"s3://{BUCKET}/curated_output/"
METRICS_PATH = (
    f"s3://{BUCKET}/audit/metrics/validated_to_curated/"
    f"run_ts={metrics_run_ts}/"
)



# =================================================
# Read validated data (LATEST ONLY)
# =================================================
validated_all = spark.read.parquet(VALIDATED_BASE)

latest_run_date = (
    validated_all
    .select(F.max("run_date").alias("max_date"))
    .collect()[0]["max_date"]
)

trips = validated_all.filter(F.col("run_date") == latest_run_date)
trips = normalize_column_names(trips)

require_cols(trips, ["pulocationid", "dolocationid"], "validated trips")


# =================================================
# Read reference: taxi zones
# =================================================
zones = spark.read.option("header", True).csv(REFERENCE_PATH)
zones = normalize_column_names(zones)

require_cols(zones, ["locationid"], "taxi_zones reference")

zones_small = zones.select(
    "locationid",
    "borough",
    "zone",
    "service_zone"
)

zones_pickup = (
    zones_small
    .withColumnRenamed("locationid", "pulocationid")
    .withColumnRenamed("borough", "pu_borough")
    .withColumnRenamed("zone", "pu_zone")
    .withColumnRenamed("service_zone", "pu_service_zone")
)

zones_dropoff = (
    zones_small
    .withColumnRenamed("locationid", "dolocationid")
    .withColumnRenamed("borough", "do_borough")
    .withColumnRenamed("zone", "do_zone")
    .withColumnRenamed("service_zone", "do_service_zone")
)


# =================================================
# Enrichment joins
# =================================================
curated = (
    trips
    .join(zones_pickup, on="pulocationid", how="left")
    .join(zones_dropoff, on="dolocationid", how="left")
)

if curated.filter(F.col("pu_zone").isNull()).count() > 0:
    raise Exception("Invalid pickup location detected")

if curated.filter(F.col("do_zone").isNull()).count() > 0:
    raise Exception("Invalid dropoff location detected")


# =================================================
# Governance metadata
# =================================================
curated = (
    curated
    .withColumn("data_source", F.lit("NYC_TLC"))
    .withColumn("curated_timestamp_utc", F.current_timestamp())
)

curated = normalize_column_names(curated)


# =================================================
# Write curated output
# =================================================
curated.write.mode("append").parquet(CURATED_PATH)


# =================================================
# Emit metrics
# =================================================
metrics_df = spark.createDataFrame(
    [{
        "job_name": "validated_to_curated",
        "run_date": str(latest_run_date),
        "records_written": curated.count(),
        "status": "SUCCESS"
    }]
)

metrics_df \
    .coalesce(1) \
    .write \
    .mode("append") \
    .json(METRICS_PATH)

print("SUCCESS: Glue-2 completed (latest validated batch processed)")
