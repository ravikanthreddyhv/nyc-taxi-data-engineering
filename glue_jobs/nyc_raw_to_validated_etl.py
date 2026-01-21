import sys
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.functions import col, when, lit
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# -------------------------------------------------
# 1. Args & Run ID
# -------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
#args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RUN_ID'])
#run_id = args['RUN_ID']


# -------------------------------------------------
# 2. Spark / Glue setup
# -------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# -------------------------------------------------
# 3. S3 Paths
# -------------------------------------------------
BUCKET = "nyc-taxi-data-dev-ravikanth-us-east-1"

RAW_PATH = f"s3://{BUCKET}/raw/"
VALIDATED_PATH = f"s3://{BUCKET}/validated_input/run_id={run_id}/"
QUARANTINE_PATH = f"s3://{BUCKET}/quarantine/raw_to_validated/run_id={run_id}/"
METRICS_PATH = f"s3://{BUCKET}/audit/metrics/raw_to_validated/run_id={run_id}/"

# -------------------------------------------------
# 4. Read RAW data
# -------------------------------------------------
raw_df = spark.read.parquet(RAW_PATH)
records_read = raw_df.count()

# -------------------------------------------------
# 5. Schema enforcement
# -------------------------------------------------
df = raw_df.select(
    col("vendorid").cast("int"),
    col("tpep_pickup_datetime").cast("timestamp"),
    col("tpep_dropoff_datetime").cast("timestamp"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("ratecodeid").cast("int"),
    col("store_and_fwd_flag").cast("string"),
    col("pulocationid").cast("int"),
    col("dolocationid").cast("int"),
    col("payment_type").cast("int"),
    col("fare_amount").cast("double"),
    col("extra").cast("double"),
    col("mta_tax").cast("double"),
    col("tip_amount").cast("double"),
    col("tolls_amount").cast("double"),
    col("improvement_surcharge").cast("double"),
    col("total_amount").cast("double"),
    col("congestion_surcharge").cast("double")
)

# -------------------------------------------------
# 6. Validation rules
# -------------------------------------------------
df = df.withColumn(
    "is_valid",
    when(col("pulocationid").isNull(), False)
    .when(col("fare_amount") < 0, False)
    .when(col("trip_distance") <= 0, False)
    .when(col("tpep_dropoff_datetime") < col("tpep_pickup_datetime"), False)
    .otherwise(True)
)

good_df = df.filter(col("is_valid")).drop("is_valid")
bad_df = df.filter(~col("is_valid"))

# -------------------------------------------------
# 7. Add metadata
# -------------------------------------------------
good_df = (
    good_df
    .withColumn("run_id", lit(run_id))
    .withColumn("run_date", col("tpep_pickup_datetime").cast("date"))
)

bad_df = bad_df.withColumn("run_id", lit(run_id))

# -------------------------------------------------
# 8. Write outputs
# -------------------------------------------------
good_df.write \
    .mode("overwrite") \
    .partitionBy("run_date") \
    .parquet(VALIDATED_PATH)

if bad_df.count() > 0:
    bad_df.write \
        .mode("overwrite") \
        .parquet(QUARANTINE_PATH)

# -------------------------------------------------
# 9. Metrics
# -------------------------------------------------
metrics = [{
    "run_id": run_id,
    "job_name": "raw_to_validated",
    "records_read": records_read,
    "records_valid": good_df.count(),
    "records_quarantined": bad_df.count(),
    "status": "SUCCESS"
}]

spark.createDataFrame(metrics) \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .json(METRICS_PATH)

print(f"Glue-1 completed successfully. run_id={run_id}")
