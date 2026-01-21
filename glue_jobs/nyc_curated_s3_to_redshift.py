import sys
import json
import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ----------------------------------------------------
# Glue 5.0 REQUIRED INITIALIZATION (DO NOT CHANGE)
# ----------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.read.parquet(
    "s3://nyc-taxi-data-dev-ravikanth-us-east-1/curated_output/"
).printSchema()

# ----------------------------------------------------
# CONFIG (EDIT ONLY VALUES IF NEEDED)
# ----------------------------------------------------
SECRET_NAME = "redshift"
REGION = "us-east-1"

CURATED_S3_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/curated_output/"
REDSHIFT_SCHEMA = "analytics"
REDSHIFT_TABLE = "curated_output"

REDSHIFT_IAM_ROLE = (
    "arn:aws:iam::160013954436:role/"
    "service-role/AmazonRedshift-CommandsAccessRole-20260115T112847"
)

# ----------------------------------------------------
# GET REDSHIFT CREDS FROM SECRETS MANAGER
# ----------------------------------------------------
def get_redshift_secret():
    client = boto3.client("secretsmanager", region_name=REGION)
    response = client.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(response["SecretString"])

secret = get_redshift_secret()

REDSHIFT_DB = secret["dbname"]
REDSHIFT_USER = secret["username"]

# ----------------------------------------------------
# REDSHIFT COPY USING DATA API (NO JDBC)
# ----------------------------------------------------
REDSHIFT_WORKGROUP = "default-workgroup"
redshift_data = boto3.client("redshift-data", region_name="us-east-1")

copy_sql = f"""
COPY {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}
FROM '{CURATED_S3_PATH}'
IAM_ROLE '{REDSHIFT_IAM_ROLE}'
FORMAT AS PARQUET;
"""

print("Executing COPY command on Redshift...")

response = redshift_data.execute_statement(
    WorkgroupName=REDSHIFT_WORKGROUP,
    Database=REDSHIFT_DB,
    Sql=copy_sql
)

print("COPY command submitted:", response["Id"])

job.commit()
