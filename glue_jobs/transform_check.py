import json
import boto3
import psycopg2


# -------------------------------------------------
# Config
# -------------------------------------------------
REGION = "us-east-1"

SQL_BUCKET = "nyc-taxi-data-dev-ravikanth-us-east-1"
SQL_PREFIX = "sql"

SECRET_NAME = "nyc-taxi-rds-master-credentials"
DB_NAME = "taxi_mdm"

# -------------------------------------------------
# Fetch DB credentials
# -------------------------------------------------
def get_db_secret(secret_name):
    client = boto3.client("secretsmanager", region_name=REGION)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

db_secret = get_db_secret(SECRET_NAME)

# -------------------------------------------------
# Read SQL file from S3
# -------------------------------------------------
def read_sql_from_s3(bucket, key):
    s3 = boto3.client("s3", region_name=REGION)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")

# -------------------------------------------------
# Open DB connection
# -------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        host=db_secret["host"],
        port=db_secret.get("port", 5432),
        database=DB_NAME,
        user=db_secret["username"],
        password=db_secret["password"]
    )

# -------------------------------------------------
# Execute TRANSFORMATION SQL (DDL / DML)
# -------------------------------------------------
def execute_sql(sql_text):
    conn = get_db_connection()
    conn.autocommit = True
    cursor = conn.cursor()

    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    for stmt in statements:
        print(f"\nExecuting SQL:\n{stmt}\n")
        cursor.execute(stmt)

    cursor.close()
    conn.close()

# -------------------------------------------------
# Execute VALIDATION / TEST SQL (SELECT COUNT(*))
# -------------------------------------------------
def execute_validation(sql_text, step_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(sql_text)
    violations = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if violations > 0:
        raise Exception(
            f"❌ {step_name} FAILED — violations found: {violations}"
        )

    print(f"✅ {step_name} PASSED")

# -------------------------------------------------
# SQL workflow definition (ORDER MATTERS)
# -------------------------------------------------
SQL_WORKFLOW = [
    {
        "type": "transform",
        "name": "daily_vendor_revenue",
        "path": "transformations/daily_vendor_revenue.sql"
    },
    {
        "type": "quality",
        "name": "qc_positive_amounts",
        "path": "quality_checks/qc_positive_amounts.sql"
    },
    {
        "type": "quality",
        "name": "qc_vendor_fk",
        "path": "quality_checks/qc_vendor_fk.sql"
    },
    {
        "type": "test",
        "name": "test_no_null_vendor",
        "path": "tests/test_no_null_vendor.sql"
    }
]

# -------------------------------------------------
# Execute workflow
# -------------------------------------------------
for step in SQL_WORKFLOW:
    print(f"\n▶ Running {step['type']} — {step['name']}")

    sql_text = read_sql_from_s3(
        SQL_BUCKET,
        f"{SQL_PREFIX}/{step['path']}"
    )

    if step["type"] == "transform":
        execute_sql(sql_text)
        print(f"✅ Transformation {step['name']} completed")

    elif step["type"] in ["quality", "test"]:
        execute_validation(sql_text, step["name"])

print("\n🎉 SQL WORKFLOW COMPLETED SUCCESSFULLY")
