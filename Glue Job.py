from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# ------------------------------------------------
# Glue + Spark setup
# ------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ------------------------------------------------
# Paths
# ------------------------------------------------
RAW_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/raw/"
REF_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/reference/taxi_zone_lookup.csv"
VALIDATED_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/validated/parquet/"
QUARANTINE_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/quarantine/"

# ------------------------------------------------
# Read RAW Parquet (CORRECT)
# ------------------------------------------------
trips_df = spark.read.parquet(RAW_PATH)

# ------------------------------------------------
# Read Master / Reference Data (CSV is correct here)
# ------------------------------------------------
zones_df = (
    spark.read.option("header", True)
    .csv(REF_PATH)
    .select(col("LocationID").cast("int").alias("zone_id"))
)

# ------------------------------------------------
# DATA QUALITY RULES (Validated Zone)
# ------------------------------------------------
quality_pass_df = trips_df.filter(
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull() &
    (col("fare_amount") > 0) &
    (col("trip_distance") >= 0) &
    col("PULocationID").isNotNull()
)

# ------------------------------------------------
# REFERENTIAL INTEGRITY CHECK
# ------------------------------------------------
validated_df = quality_pass_df.join(
    zones_df,
    quality_pass_df.PULocationID.cast("int") == zones_df.zone_id,
    "inner"
)

# ------------------------------------------------
# QUARANTINE RECORDS
# ------------------------------------------------
quarantine_df = trips_df.subtract(validated_df)
quarantine_count = quarantine_df.count()



# ------------------------------------------------
# WRITE OUTPUTS WITH ENFORCEMENT
# ------------------------------------------------

validated_df.write.mode("overwrite").parquet(VALIDATED_PATH)

if quarantine_count > 0:
    quarantine_df.write.mode("overwrite").parquet(QUARANTINE_PATH)
    raise Exception(
        f"Data Quality Failed: {quarantine_count} records moved to quarantine"
    )

print("✅ Glue Job succeeded: No data quality violations")

