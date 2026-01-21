from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Delta Audit - Create New Version")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

DELTA_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/validated/nyc_taxi_delta/"

# Read current Delta version
df = spark.read.format("delta").load(DELTA_PATH)

# Compliance rule: remove negative fares
clean_df = df.filter(col("fare_amount") > 0)

# Overwrite -> creates NEW VERSION
(
    clean_df.write
    .format("delta")
    .mode("overwrite")
    .save(DELTA_PATH)
)

print("New Delta version created after compliance rule")

spark.stop()
