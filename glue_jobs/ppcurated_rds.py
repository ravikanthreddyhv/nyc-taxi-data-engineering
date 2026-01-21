import json
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# ----------------------------------
# Glue setup
# ----------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ----------------------------------
# Fetch DB credentials from Secrets Manager
# ----------------------------------
def get_db_secret(secret_name, region="us-east-1"):
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

db_secret = get_db_secret("nyc-taxi-rds-master-credentials")

jdbc_url = (
    f"jdbc:postgresql://{db_secret['host']}:"
    f"{db_secret['port']}/taxi_mdm"
)

# ----------------------------------
# Read curated_output from S3
# ----------------------------------
df = spark.read.parquet(
    "s3://nyc-taxi-data-dev-ravikanth-us-east-1/curated_output/"
)

# ----------------------------------
# Select columns in correct order
# ----------------------------------
df_final = df.select(
    "dolocationid",
    "pulocationid",
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "run_id",
    "run_date"
)

# ----------------------------------
# OVERWRITE load into RDS
# ----------------------------------
df_final.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "curated.yellow_tripdata") \
    .option("user", db_secret["username"]) \
    .option("password", db_secret["password"]) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()
