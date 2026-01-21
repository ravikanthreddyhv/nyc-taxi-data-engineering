from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, current_timestamp, lit, to_date

print("🚀 Starting Day 7 FINAL Enrichment Job...")

# --------------------------------------------------
# Spark Session with Delta support
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Day7 - NYC Taxi Enrichment FINAL")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created")

# --------------------------------------------------
# Paths
# --------------------------------------------------
VALIDATED_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/validated/nyc_taxi_delta"
ZONE_LOOKUP_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/reference/"
CURATED_PATH = "s3a://nyc-taxi-data-dev-ravikanth-us-east-1/curated/nyc_taxi_enriched_v2"

# --------------------------------------------------
# Read validated Delta
# --------------------------------------------------
trips_df = spark.read.format("delta").load(VALIDATED_PATH)
print("✅ Trips loaded:", trips_df.count())

# --------------------------------------------------
# Read & prepare reference data
# --------------------------------------------------
zones_df = (
    spark.read.option("header", True)
    .csv(ZONE_LOOKUP_PATH)
    .select(
        col("LocationID").cast("int").alias("zone_id"),
        col("Borough").alias("borough"),
        col("Zone").alias("zone_name")
    )
)

zones_df = broadcast(zones_df)
print("✅ Zone lookup loaded:", zones_df.count())

# --------------------------------------------------
# Enrich Pickup Zone
# --------------------------------------------------
enriched_df = (
    trips_df
    .join(zones_df, trips_df.PULocationID == zones_df.zone_id, "left")
    .withColumnRenamed("zone_name", "pickup_zone")
    .withColumnRenamed("borough", "pickup_borough")
    .drop("zone_id")
)

# --------------------------------------------------
# Enrich Dropoff Zone
# --------------------------------------------------
zones_df2 = zones_df.select(
    col("zone_id").alias("drop_zone_id"),
    col("zone_name").alias("dropoff_zone"),
    col("borough").alias("dropoff_borough")
)

enriched_df = (
    enriched_df
    .join(zones_df2, enriched_df.DOLocationID == zones_df2.drop_zone_id, "left")
    .drop("drop_zone_id")
)

# --------------------------------------------------
# Data Quality Checks
# --------------------------------------------------
missing_pickup = enriched_df.filter(col("pickup_zone").isNull()).count()
missing_dropoff = enriched_df.filter(col("dropoff_zone").isNull()).count()

print(f"⚠ Missing pickup zones: {missing_pickup}")
print(f"⚠ Missing dropoff zones: {missing_dropoff}")

# --------------------------------------------------
# Add Audit / Lineage Metadata
# --------------------------------------------------
enriched_df = (
    enriched_df
    .withColumn("pipeline_name", lit("day7_taxi_enrichment"))
    .withColumn("source_layer", lit("validated"))
    .withColumn("target_layer", lit("curated"))
    .withColumn("processed_at", current_timestamp())
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
)

# --------------------------------------------------
# Write Curated Delta (Partitioned)
# --------------------------------------------------
(
    enriched_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_date")
    .save(CURATED_PATH)
)

print("🎉 Day 7 FINAL enrichment completed successfully")
spark.stop()
