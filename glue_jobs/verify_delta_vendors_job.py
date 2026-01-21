import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
DELTA_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/master/vendors_roll/"

# --------------------------------------------------
# INIT (Glue Safe)
# --------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --------------------------------------------------
# 1️⃣ VERIFY _delta_log EXISTS (REAL DELTA CHECK)
# --------------------------------------------------
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
    sc._jvm.java.net.URI.create(DELTA_PATH),
    sc._jsc.hadoopConfiguration()
)

delta_log_path = sc._jvm.org.apache.hadoop.fs.Path(DELTA_PATH + "/_delta_log")

if not fs.exists(delta_log_path):
    raise Exception("❌ NOT a Delta table: _delta_log not found")

print("✅ Delta table structure verified (_delta_log exists)")

# --------------------------------------------------
# 2️⃣ READ DELTA DATA (Glue-supported)
# --------------------------------------------------
df = spark.read.format("delta").load(DELTA_PATH)

print("✅ Delta table readable")

# --------------------------------------------------
# 3️⃣ VERIFY SCD TYPE 2 BEHAVIOR
# --------------------------------------------------
df.select(
    "vendorid",
    "version_no",
    "is_current",
    "effective_start_ts",
    "effective_end_ts"
).orderBy("vendorid", "version_no") \
 .show(truncate=False)

# --------------------------------------------------
# 4️⃣ VALIDATION CHECKS
# --------------------------------------------------

# Only ONE current record per key
dup_current = (
    df.filter("is_current = true")
      .groupBy("vendorid")
      .count()
      .filter("count > 1")
)

if dup_current.count() > 0:
    raise Exception("❌ SCD violation: multiple current records found")

print("✅ SCD rule validated: single current record per vendor")

print("🎉 DELTA + SCD VERIFICATION COMPLETE")
