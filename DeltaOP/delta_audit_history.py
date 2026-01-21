from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Delta Audit History")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

DELTA_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/validated/nyc_taxi_delta/"

# Show Delta transaction history
spark.sql(f"""
    DESCRIBE HISTORY delta.`{DELTA_PATH}`
""").show(truncate=False)
