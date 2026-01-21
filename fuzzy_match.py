import pandas as pd
import re
import recordlinkage
from fuzzywuzzy import fuzz
from datetime import datetime, timezone

# -------------------------------------------------
# STEP 1: Load vendor data
# -------------------------------------------------
df = pd.read_csv("vendors.csv")

# -------------------------------------------------
# STEP 2: Normalize text fields
# -------------------------------------------------
def normalize(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

for col in ["vendor_name", "vendor_address", "city", "state"]:
    df[col + "_norm"] = df[col].apply(normalize)

# -------------------------------------------------
# STEP 3: Blocking (candidate pairs)
# -------------------------------------------------
indexer = recordlinkage.Index()
indexer.block(["state_norm", "city_norm"])
candidate_pairs = indexer.index(df)

print(f"Candidate pairs: {len(candidate_pairs)}")

# -------------------------------------------------
# STEP 4: Fuzzy matching
# -------------------------------------------------
results = []

for idx1, idx2 in candidate_pairs:
    r1 = df.loc[idx1]
    r2 = df.loc[idx2]

    name_score = fuzz.token_sort_ratio(
        r1["vendor_name_norm"],
        r2["vendor_name_norm"]
    )

    address_score = fuzz.token_sort_ratio(
        r1["vendor_address_norm"],
        r2["vendor_address_norm"]
    )

    final_score = round((0.7 * name_score) + (0.3 * address_score), 2)

    results.append({
        "vendor_id_1": r1["vendor_id"],
        "vendor_id_2": r2["vendor_id"],
        "name_score": name_score,
        "address_score": address_score,
        "final_score": final_score
    })

matches_df = pd.DataFrame(results)

print("\n--- FUZZY MATCH RESULTS ---")
print(matches_df)

# -------------------------------------------------
# STEP 5: Confidence buckets & governance actions
# -------------------------------------------------
def confidence_bucket(score):
    if score >= 95:
        return "HIGH"
    elif score >= 80:
        return "MEDIUM"
    else:
        return "LOW"

def governance_action(bucket):
    if bucket == "HIGH":
        return "AUTO_MERGE"
    elif bucket == "MEDIUM":
        return "STEWARD_REVIEW"
    else:
        return "NO_ACTION"

matches_df["confidence_bucket"] = matches_df["final_score"].apply(confidence_bucket)
matches_df["governance_action"] = matches_df["confidence_bucket"].apply(governance_action)

print("\n--- MATCH RESULTS WITH GOVERNANCE ---")
print(matches_df)

# -------------------------------------------------
# STEP 6: Steward review queue
# -------------------------------------------------
steward_queue_df = matches_df[
    matches_df["governance_action"] == "STEWARD_REVIEW"
].copy()

steward_queue_df["review_status"] = "PENDING"
steward_queue_df["reviewed_by"] = None
steward_queue_df["reviewed_at"] = None
steward_queue_df["created_at"] = datetime.now(timezone.utc)

print("\n--- STEWARD REVIEW QUEUE ---")
print(steward_queue_df)

steward_queue_df.to_csv("steward_review_queue.csv", index=False)

# -------------------------------------------------
# STEP 7: Simulate steward approval(hard-coded)
# -------------------------------------------------
steward_queue_df["review_status"] = "APPROVED"
steward_queue_df["reviewed_by"] = "steward_user_1"
steward_queue_df["reviewed_at"] = datetime.now(timezone.utc)

# -------------------------------------------------
# STEP 8: Deduplication & golden record creation
# -------------------------------------------------
def create_golden_vendor(record_1, record_2):
    return {
        "golden_vendor_name": max(
            record_1["vendor_name"],
            record_2["vendor_name"],
            key=len
        ),
        "golden_vendor_address": max(
            record_1["vendor_address"],
            record_2["vendor_address"],
            key=len
        ),
        "city": record_1["city"],
        "state": record_1["state"],
        "source_vendor_ids": f'{record_1["vendor_id"]},{record_2["vendor_id"]}',

        # ---- LIFECYCLE FIELDS ----
        "lifecycle_state": "PROPOSED",
        "effective_from": datetime.now(timezone.utc),
        "effective_to": None,
        "approved_by": None,
        "approved_at": None,
        "created_at": datetime.now(timezone.utc)
    }

golden_records = []

for _, row in steward_queue_df.iterrows():
    v1 = df[df["vendor_id"] == row["vendor_id_1"]].iloc[0]
    v2 = df[df["vendor_id"] == row["vendor_id_2"]].iloc[0]

    golden_record = create_golden_vendor(v1, v2)
    golden_records.append(golden_record)

golden_df = pd.DataFrame(golden_records)

# -------------------------------------------------
# STEP 8B: Lifecycle transition PROPOSED → ACTIVE
# -------------------------------------------------
golden_df["lifecycle_state"] = "ACTIVE"
golden_df["approved_by"] = "steward_user_1"
golden_df["approved_at"] = datetime.now(timezone.utc)

print("\n--- GOLDEN VENDOR RECORDS (MASTER DATA) ---")
print(golden_df)

# Persist locally
golden_df.to_csv("golden_vendor_records.csv", index=False)

# Persist to MASTER zone (MDM)
golden_df.to_parquet(
    "s3://nyc-taxi-data-dev-ravikanth-us-east-1/master/vendors/",
    index=False
)

# -------------------------------------------------
# STEP 9: Data Quality Scorecard
# -------------------------------------------------
print("\n>>> ENTERED STEP 9: DATA QUALITY SCORECARD <<<")

total_records = len(df)
duplicate_candidate_pairs = len(matches_df)
steward_reviews = len(steward_queue_df)
auto_merges = len(matches_df[matches_df["governance_action"] == "AUTO_MERGE"])

scorecard_df = pd.DataFrame([{
    "total_vendor_records": total_records,
    "duplicate_candidate_pairs": duplicate_candidate_pairs,
    "duplicate_rate_percent": round((duplicate_candidate_pairs / total_records) * 100, 2),
    "steward_review_rate_percent": round((steward_reviews / duplicate_candidate_pairs) * 100, 2)
        if duplicate_candidate_pairs else 0,
    "auto_merge_rate_percent": round((auto_merges / duplicate_candidate_pairs) * 100, 2)
        if duplicate_candidate_pairs else 0
}])

print("\n--- DATA QUALITY SCORECARD ---")
print(scorecard_df)

scorecard_df.to_csv("data_quality_scorecard.csv", index=False)
print("Saved data_quality_scorecard.csv")
