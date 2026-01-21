from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --------------------------------------------------
# 1. Create Spark Session with Delta support
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("NYC Taxi - Raw to Validated Delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# --------------------------------------------------
# 2. Define Data Lake Paths
# --------------------------------------------------
RAW_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/raw/"
DELTA_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/validated/nyc_taxi_delta/"


# --------------------------------------------------
# 3. Read Raw Data
# --------------------------------------------------
raw_df = spark.read.parquet(RAW_PATH)

# --------------------------------------------------
# 4. Basic Validation (Validated Zone Logic)
#    - Drop records with null critical fields
# --------------------------------------------------
validated_df = (
    raw_df
    .filter(col("tpep_pickup_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime").isNotNull())
    .filter(col("fare_amount").isNotNull())
)

# --------------------------------------------------
# 5. Write as Delta Lake
# --------------------------------------------------
(
    validated_df.write
    .format("delta")
    .mode("overwrite")
    .save(DELTA_PATH)
)

print("✅ Raw data successfully converted to Delta format in Validated zone")





mkdir glue_jobs
mkdir lambda
mkdir step_functions
mkdir governance
mkdir tests
