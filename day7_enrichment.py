from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("NYC Taxi Enrichment")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

VALIDATED_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/validated/nyc_taxi_delta"
ZONE_LOOKUP_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/reference/"
ENRICHED_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/enriched/nyc_taxi_enriched"

# Read validated Delta
trips_df = spark.read.format("delta").load(VALIDATED_PATH)

# Read reference data
zones_df = (
    spark.read.option("header", "true")
    .csv(ZONE_LOOKUP_PATH)
    .select(
        col("LocationID").cast("int"),
        col("Borough"),
        col("Zone")
    )
)

# Enrich pickup zone
enriched_df = (
    trips_df
    .join(zones_df, trips_df.PULocationID == zones_df.LocationID, "left")
    .drop("LocationID")
    .withColumnRenamed("Borough", "pickup_borough")
    .withColumnRenamed("Zone", "pickup_zone")
)

# Write enriched Delta
(
    enriched_df.write
    .format("delta")
    .mode("overwrite")
    .save(ENRICHED_PATH)
)

print("✅ Day 7 enrichment completed successfully")
