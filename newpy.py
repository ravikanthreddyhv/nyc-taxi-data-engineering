import boto3
import json
import yaml
from datetime import datetime, timezone
import os

# -----------------------------
# CONFIGURATION
# -----------------------------

REGION = "us-east-1"
BUCKET_NAME = "nyc-taxi-data-dev-ravikanth-us-east-1"

PARQUET_FILE = "yellow_tripdata_2025-08.parquet"
CSV_FILE = "sample.csv"
JSON_FILE = "sample.json"

OWNER = "DataEngineeringTeam"
DOMAIN = "Transportation"
CLASSIFICATION = "Internal"

s3 = boto3.client("s3", region_name=REGION)

# -----------------------------
# 1️⃣ CREATE S3 BUCKET
# -----------------------------

def create_bucket():
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket created: {BUCKET_NAME}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print("Bucket already exists")

# -----------------------------
# 2️⃣ ENABLE BUCKET VERSIONING (NEW)
# -----------------------------

def enable_versioning():
    s3.put_bucket_versioning(
        Bucket=BUCKET_NAME,
        VersioningConfiguration={"Status": "Enabled"}
    )
    print("Bucket versioning enabled")

# -----------------------------
# 3️⃣ CREATE PREFIXES (NEW)
# -----------------------------

def create_prefixes():
    for prefix in ["raw/", "processed/", "curated/", "metadata/"]:
        s3.put_object(Bucket=BUCKET_NAME, Key=prefix)
    print("Prefixes created: raw/, processed/, curated/, metadata/")

# -----------------------------
# 4️⃣ CONFIGURE BUCKET POLICY
# -----------------------------

def apply_bucket_policy():
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAccountReadOnlyAccessToCurated",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::160013954436:root"
                },
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/curated/*"
            }
        ]
    }

    s3.put_bucket_policy(
        Bucket=BUCKET_NAME,
        Policy=json.dumps(policy)
    )
    print("Private bucket policy applied")

# -----------------------------
# 5️⃣ APPLY LIFECYCLE RULES
# -----------------------------

def apply_lifecycle_policy():
    lifecycle_config = {
        "Rules": [
            {
                "ID": "MoveRawToGlacier",
                "Prefix": "raw/",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Days": 30,
                        "StorageClass": "GLACIER"
                    }
                ]
            }
        ]
    }

    s3.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration=lifecycle_config
    )
    print("Lifecycle policy applied")

# -----------------------------
# 6️⃣ UPLOAD MULTIPLE FILE TYPES (NEW)
# -----------------------------

def upload_multiple_files():
    files = [
        (PARQUET_FILE, f"raw/{PARQUET_FILE}"),
        (CSV_FILE, f"raw/{CSV_FILE}"),
        (JSON_FILE, f"raw/{JSON_FILE}")
    ]

    for local_file, s3_key in files:
        if os.path.exists(local_file):
            s3.upload_file(
                local_file,
                BUCKET_NAME,
                s3_key,
                ExtraArgs={
                    "Tagging": f"Owner={OWNER}&Domain={DOMAIN}&Classification={CLASSIFICATION}"
                }
            )
            print(f"Uploaded {local_file} → {s3_key}")
        else:
            print(f"File not found locally: {local_file}")


#-------------
# Upload data dictionary
# -----------------------------
def upload_data_dictionary():
    s3.upload_file(
        "data_dictionary.csv",
        BUCKET_NAME,
        "metadata/data_dictionary.csv"
    )
    print("Data dictionary uploaded")




# -----------------------------
# 7️⃣ CREATE METADATA MANIFEST
# -----------------------------
            

def create_metadata_manifest():
    manifest = {
        "dataset": "NYC Yellow Taxi Trips",
        "bucket": BUCKET_NAME,
        "owner": OWNER,
        "domain": DOMAIN,
        "classification": CLASSIFICATION,
        "created_on": datetime.now(timezone.utc).isoformat(),
        "retention_policy": "7 years",
        "zones": ["raw", "processed", "curated"]
    }

    with open("metadata_manifest.json", "w") as jf:
        json.dump(manifest, jf, indent=4)

    with open("metadata_manifest.yaml", "w") as yf:
        yaml.dump(manifest, yf)

    s3.upload_file(
        "metadata_manifest.json",
        BUCKET_NAME,
        "metadata/metadata_manifest.json"
    )

    s3.upload_file(
        "metadata_manifest.yaml",
        BUCKET_NAME,
        "metadata/metadata_manifest.yaml"
    )

    print("Metadata manifest created and uploaded")

# -----------------------------
# MAIN EXECUTION
# -----------------------------

if __name__ == "__main__":
    create_bucket()
    enable_versioning()
    create_prefixes()
    apply_bucket_policy()
    apply_lifecycle_policy()
    upload_multiple_files()
    create_metadata_manifest()
    upload_data_dictionary()

