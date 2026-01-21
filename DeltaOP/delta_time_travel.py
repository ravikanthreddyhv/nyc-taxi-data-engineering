from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Delta Time Travel")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

DELTA_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/validated/nyc_taxi_delta/"

# Read older version (before compliance change)
old_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(DELTA_PATH)
)

print("Old version record count:", old_df.count())

spark.stop()
