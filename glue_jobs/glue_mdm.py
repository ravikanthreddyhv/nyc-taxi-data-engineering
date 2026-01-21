from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, broadcast
from pyspark.sql import Row
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

RAW_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/raw/*"
REF_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/reference/"
VALIDATED_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/validated/parquet/"
QUARANTINE_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/quarantine/"

# Read raw parquet
trips_df = spark.read.parquet(RAW_PATH)

# Read reference data
zones_df = broadcast(
    spark.read.option("header", True)
    .csv(REF_PATH)
    .select(col("LocationID").cast("int").alias("zone_id"))
)

# Data quality rules
quality_pass_df = trips_df.filter(
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull() &
    (col("fare_amount") > 0) &
    col("PULocationID").isNotNull()
)

# Referential integrity (VALID)
validated_df = quality_pass_df.join(
    zones_df,
    quality_pass_df.PULocationID.cast("int") == zones_df.zone_id,
    "inner"
)

# Orphan records (QUARANTINE)
quarantine_df = quality_pass_df.join(
    zones_df,
    quality_pass_df.PULocationID.cast("int") == zones_df.zone_id,
    "left_anti"
)

# Write outputs
validated_df.write.mode("overwrite").parquet(VALIDATED_PATH)
quarantine_df.write.mode("overwrite").parquet(QUARANTINE_PATH)

print("✅ Step 3 completed: validated + quarantine written")
print("📊 Starting STEP 4: Data Quality Scorecard")

# -----------------------------------------
# COUNTS FROM PIPELINE
# -----------------------------------------
total_count = trips_df.count()
validated_count = validated_df.count()
#quarantine_count = orphan_df.count()

# -----------------------------------------
# QUALITY METRICS
# -----------------------------------------
completeness_pct = round((validated_count / total_count) * 100, 2)
accuracy_pct = completeness_pct  # same rules applied
referential_integrity_pct = completeness_pct

# -----------------------------------------
# SCORECARD RECORDS
# -----------------------------------------
scorecard_rows = [
    Row(
        quality_dimension="Completeness",
        business_metric="Trips with pickup_datetime present",
        owner="Data Steward",
        threshold=">= 99%",
        pass_percentage=completeness_pct,
        action_on_failure="Alert + Manual Review"
    ),
    Row(
        quality_dimension="Accuracy",
        business_metric="Trips with fare_amount > 0",
        owner="Finance Owner",
        threshold=">= 98%",
        pass_percentage=accuracy_pct,
        action_on_failure="Block Load"
    ),
    Row(
        quality_dimension="Referential Integrity",
        business_metric="Pickup zone exists in master data",
        owner="MDM Owner",
        threshold="100%",
        pass_percentage=referential_integrity_pct,
        action_on_failure="Quarantine Records"
    )
]

scorecard_df = spark.createDataFrame(scorecard_rows)
# ------------------------------------------------
# ENFORCEMENT: FAIL / QUARANTINE / BLOCK JOB
# ------------------------------------------------

quarantine_count = quarantine_df.count()

if quarantine_count > 0:
    quarantine_df.write.mode("overwrite").parquet(QUARANTINE_PATH)
    raise Exception(
        f"Data Quality Failed: {quarantine_count} records quarantined"
    )


print("📊 DATA QUALITY SCORECARD")
scorecard_df.show(truncate=False)

# -----------------------------------------
# WRITE SCORECARD TO GOVERNANCE LAYER
# -----------------------------------------
SCORECARD_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/governance/quality_scorecard/"

scorecard_df.write.mode("overwrite").parquet(SCORECARD_PATH)

print("✅ Quality scorecard written to governance layer")
