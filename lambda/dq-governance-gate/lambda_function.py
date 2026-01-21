import json
import boto3
from datetime import datetime, timezone

# AWS clients
s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

def lambda_handler(event, context):
    # -----------------------------
    # 1. Read input
    # -----------------------------
    bucket = event["bucket"]

    base_prefix = "audit/metrics/raw_to_validated/"

    # -----------------------------
    # 2. Discover latest run
    # -----------------------------
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=base_prefix,
        Delimiter="/"
    )

    runs = response.get("CommonPrefixes", [])
    if not runs:
        raise Exception("No Glue metrics found")

    latest_run_prefix = sorted(
        runs,
        key=lambda x: x["Prefix"],
        reverse=True
    )[0]["Prefix"]

    # -----------------------------
    # 3. Read metrics file
    # -----------------------------
    metrics_obj = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=latest_run_prefix
    )

    metrics_key = metrics_obj["Contents"][0]["Key"]
    metrics_file = s3.get_object(Bucket=bucket, Key=metrics_key)

    metrics = json.loads(
        metrics_file["Body"].read().decode("utf-8")
    )

    # -----------------------------
    # 4. Compute governance metrics
    # -----------------------------
    records_read = metrics.get("records_read", 0)
    records_valid = metrics.get("records_valid", 0)
    #quality_ratio = 0.85 
    quality_ratio = (
         records_valid / records_read
         if records_read > 0 else 0.0
     )

    # Convert to percentage for CloudWatch
    quality_score = round(quality_ratio * 100, 2)

    # -----------------------------
    # 5. Freshness check
    # -----------------------------
    # Placeholder – always true for now
    # Can be extended later using timestamps
    freshness_ok = True

    # -----------------------------
    # 6. Publish governance metrics
    # -----------------------------
    cloudwatch.put_metric_data(
        Namespace="DataGovernance",
        MetricData=[
            {
                "MetricName": "QualityScore",
                "Value": quality_score,
                "Unit": "Percent",
                "Dimensions": [
                    {
                        "Name": "Pipeline",
                        "Value": "raw_to_validated"
                    }
                ]
            },
            {
                "MetricName": "FreshnessOK",
                "Value": 1 if freshness_ok else 0,
                "Unit": "Count",
                "Dimensions": [
                    {
                        "Name": "Pipeline",
                        "Value": "raw_to_validated"
                    }
                ]
            }
        ]
    )

    # -----------------------------
    # 7. Governance decision
    # -----------------------------
    if quality_ratio >= 0.9 and freshness_ok:
        return {
            "decision": "PASS",
            "quality_ratio": round(quality_ratio, 4),
            "quality_score": quality_score,
            "freshness_ok": True
        }

    return {
        "decision": "FAIL",
        "reason": "QUALITY_GATE_FAILED",
        "quality_ratio": round(quality_ratio, 4),
        "quality_score": quality_score,
        "freshness_ok": False
    }
