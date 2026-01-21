import json
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:160013954436:data-steward-alerts"


def read_metrics(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        raise Exception("Metrics file not found")

    key = resp["Contents"][0]["Key"]
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())[0]


def is_reference_stale(bucket, reference_prefix, max_age_days):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=reference_prefix)
    if "Contents" not in resp:
        return True

    latest = max(o["LastModified"] for o in resp["Contents"])
    age_days = (datetime.now(timezone.utc) - latest).days
    return age_days > max_age_days


def send_sns(message):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject="🚨 Data Governance Alert",
        Message=message
    )


def write_audit(bucket, run_id, payload):
    key = f"audit/governance/run_id={run_id}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2),
        ContentType="application/json"
    )


def lambda_handler(event, context):
    run_id = event["run_id"]
    bucket = event["bucket"]
    dq_threshold = event.get("dq_threshold_pct", 95)
    freshness_days = event.get("reference_freshness_days", 7)

    audit = {
        "run_id": run_id,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "status": "PASS",
        "checks": {}
    }

    # 1️⃣ Quality gate
    metrics_prefix = f"audit/metrics/raw_to_validated/run_id={run_id}/"
    metrics = read_metrics(bucket, metrics_prefix)

    records_read = metrics["records_read"]
    valid_records = metrics["records_valid"]
    quality_pct = round((valid_records / records_read) * 100, 2)

    audit["checks"]["quality"] = {
        "records_read": records_read,
        "records_valid": valid_records,
        "quality_pct": quality_pct,
        "threshold": dq_threshold
    }

    if quality_pct < dq_threshold:
        audit["status"] = "FAIL"
        audit["reason"] = "DATA_QUALITY_THRESHOLD_BREACHED"

        send_sns(
            f"❌ Quality gate FAILED\n"
            f"Run ID: {run_id}\n"
            f"Quality: {quality_pct}% (threshold {dq_threshold}%)"
        )

        write_audit(bucket, run_id, audit)

        return {
            "status": "FAIL",
            "run_id": run_id,
            "reason": "QUALITY_GATE_FAILED"
        }

    # 2️⃣ Reference freshness
    stale = is_reference_stale(bucket, "reference/", freshness_days)

    audit["checks"]["reference_freshness"] = {
        "stale": stale,
        "max_age_days": freshness_days
    }

    if stale:
        send_sns(
            f"⚠️ Reference data is STALE\n"
            f"Run ID: {run_id}\n"
            f"Threshold: {freshness_days} days"
        )

    # 3️⃣ Final audit
    write_audit(bucket, run_id, audit)

    return {
        "status": "PASS",
        "run_id": run_id
    }
