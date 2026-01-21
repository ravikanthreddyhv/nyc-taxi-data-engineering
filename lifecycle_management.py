import pandas as pd
from datetime import datetime, timezone
import os

# ---------------------------
# CONFIG (adjust if needed)
# ---------------------------
MASTER_INPUT_FILE = "golden_vendor_records.csv"          # from fuzzy_match.py
STEWARD_QUEUE_FILE = "steward_review_queue.csv"          # from fuzzy_match.py

MASTER_OUTPUT_FILE = "master_vendors_current.csv"        # current master snapshot
HISTORY_OUTPUT_FILE = "master_vendors_lifecycle_history.csv"
CHANGE_LOG_FILE = "master_vendors_change_log.csv"
STEWARD_ACTIVITY_FILE = "steward_activity_log.csv"
GOV_METRICS_FILE = "governance_metrics.csv"


# ---------------------------
# HELPERS
# ---------------------------
def now_utc():
    return datetime.now(timezone.utc).isoformat()

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure master has lifecycle and audit columns.
    """
    required_defaults = {
        "master_vendor_id": None,
        "lifecycle_state": "PROPOSED",
        "effective_from": None,
        "effective_to": None,
        "approved_by": None,
        "approved_at": None,
        "change_reason": None,
        "changed_by": None,
        "changed_at": None,
        "created_at": None,
    }

    for col, default_val in required_defaults.items():
        if col not in df.columns:
            df[col] = default_val

    # ---- FIX: generate IDs as a Series, not Index ----
    if df["master_vendor_id"].isna().any():
        generated_ids = pd.Series(
            [f"MV-{i+1:05d}" for i in range(len(df))],
            index=df.index
        )
        df.loc[df["master_vendor_id"].isna(), "master_vendor_id"] = \
            generated_ids[df["master_vendor_id"].isna()]

    # Fill timestamps
    df["created_at"] = df["created_at"].fillna(now_utc())
    df["effective_from"] = df["effective_from"].fillna(now_utc())

    return df



def append_history(history_df: pd.DataFrame, master_row: pd.Series,
                   old_state: str, new_state: str,
                   actor: str, reason: str) -> pd.DataFrame:
    """
    Add one row to lifecycle history.
    """
    hist_row = {
        "master_vendor_id": master_row["master_vendor_id"],
        "golden_vendor_name": master_row.get("golden_vendor_name"),
        "golden_vendor_address": master_row.get("golden_vendor_address"),
        "city": master_row.get("city"),
        "state": master_row.get("state"),
        "source_vendor_ids": master_row.get("source_vendor_ids"),
        "old_state": old_state,
        "new_state": new_state,
        "changed_by": actor,
        "change_reason": reason,
        "changed_at": now_utc(),
    }
    return pd.concat([history_df, pd.DataFrame([hist_row])], ignore_index=True)


def append_change_log(change_log_df: pd.DataFrame,
                      master_vendor_id: str,
                      action: str,
                      actor: str,
                      reason: str) -> pd.DataFrame:
    """
    Change reason log (separate from lifecycle history).
    """
    row = {
        "master_vendor_id": master_vendor_id,
        "action": action,
        "changed_by": actor,
        "change_reason": reason,
        "changed_at": now_utc(),
    }
    return pd.concat([change_log_df, pd.DataFrame([row])], ignore_index=True)


# ---------------------------
# TRANSITION RULES
# ---------------------------
def promote_to_active(master_df, history_df, change_log_df, master_vendor_id, actor, reason):
    idx = master_df.index[master_df["master_vendor_id"] == master_vendor_id]
    if len(idx) == 0:
        raise ValueError(f"master_vendor_id not found: {master_vendor_id}")
    i = idx[0]

    old_state = master_df.at[i, "lifecycle_state"]
    if old_state != "PROPOSED":
        raise ValueError(f"Invalid transition: {old_state} → ACTIVE (only PROPOSED can become ACTIVE)")

    master_df.at[i, "lifecycle_state"] = "ACTIVE"
    master_df.at[i, "approved_by"] = actor
    master_df.at[i, "approved_at"] = now_utc()
    master_df.at[i, "changed_by"] = actor
    master_df.at[i, "changed_at"] = now_utc()
    master_df.at[i, "change_reason"] = reason

    history_df = append_history(history_df, master_df.loc[i], old_state, "ACTIVE", actor, reason)
    change_log_df = append_change_log(change_log_df, master_vendor_id, "PROMOTE_TO_ACTIVE", actor, reason)
    return master_df, history_df, change_log_df


def deprecate(master_df, history_df, change_log_df, master_vendor_id, actor, reason):
    idx = master_df.index[master_df["master_vendor_id"] == master_vendor_id]
    if len(idx) == 0:
        raise ValueError(f"master_vendor_id not found: {master_vendor_id}")
    i = idx[0]

    old_state = master_df.at[i, "lifecycle_state"]
    if old_state != "ACTIVE":
        raise ValueError(f"Invalid transition: {old_state} → DEPRECATED (only ACTIVE can be deprecated)")

    master_df.at[i, "lifecycle_state"] = "DEPRECATED"
    master_df.at[i, "changed_by"] = actor
    master_df.at[i, "changed_at"] = now_utc()
    master_df.at[i, "change_reason"] = reason

    history_df = append_history(history_df, master_df.loc[i], old_state, "DEPRECATED", actor, reason)
    change_log_df = append_change_log(change_log_df, master_vendor_id, "DEPRECATE", actor, reason)
    return master_df, history_df, change_log_df


def retire(master_df, history_df, change_log_df, master_vendor_id, actor, reason):
    idx = master_df.index[master_df["master_vendor_id"] == master_vendor_id]
    if len(idx) == 0:
        raise ValueError(f"master_vendor_id not found: {master_vendor_id}")
    i = idx[0]

    old_state = master_df.at[i, "lifecycle_state"]
    if old_state not in ["DEPRECATED", "ACTIVE"]:
        raise ValueError(f"Invalid transition: {old_state} → RETIRED (only ACTIVE/DEPRECATED can be retired)")

    master_df.at[i, "lifecycle_state"] = "RETIRED"
    master_df.at[i, "effective_to"] = now_utc()  # validity ends
    master_df.at[i, "changed_by"] = actor
    master_df.at[i, "changed_at"] = now_utc()
    master_df.at[i, "change_reason"] = reason

    history_df = append_history(history_df, master_df.loc[i], old_state, "RETIRED", actor, reason)
    change_log_df = append_change_log(change_log_df, master_vendor_id, "RETIRE", actor, reason)
    return master_df, history_df, change_log_df


# ---------------------------
# MAIN
# ---------------------------
def main():
    if not os.path.exists(MASTER_INPUT_FILE):
        raise FileNotFoundError(f"Missing {MASTER_INPUT_FILE}. Run fuzzy_match.py first.")

    master_df = pd.read_csv(MASTER_INPUT_FILE)

    # Make sure lifecycle columns exist
    master_df = ensure_columns(master_df)

    # Prepare empty governance tables
    history_df = pd.DataFrame(columns=[
        "master_vendor_id", "golden_vendor_name", "golden_vendor_address",
        "city", "state", "source_vendor_ids",
        "old_state", "new_state", "changed_by", "change_reason", "changed_at"
    ])
    change_log_df = pd.DataFrame(columns=[
        "master_vendor_id", "action", "changed_by", "change_reason", "changed_at"
    ])

    # -------------------------
    # DEMO TRANSITIONS (finish Week-2)
    # -------------------------
    # If your fuzzy_match already set lifecycle_state = ACTIVE, we’ll keep it.
    # But to demonstrate full lifecycle, we will:
    # 1) Ensure at least 1 record exists
    # 2) Deprecate it
    # 3) Retire it
    # (This creates history + audit trail)

    if len(master_df) == 0:
        print("No master records found. Nothing to transition.")
        master_df.to_csv(MASTER_OUTPUT_FILE, index=False)
        history_df.to_csv(HISTORY_OUTPUT_FILE, index=False)
        change_log_df.to_csv(CHANGE_LOG_FILE, index=False)
        return

    # Pick first record
    target_id = master_df.iloc[0]["master_vendor_id"]

    # If record is still PROPOSED, promote to ACTIVE (optional)
    if master_df.iloc[0]["lifecycle_state"] == "PROPOSED":
        master_df, history_df, change_log_df = promote_to_active(
            master_df, history_df, change_log_df,
            master_vendor_id=target_id,
            actor="steward_user_1",
            reason="Steward approved golden record"
        )

    # Deprecate (demonstrate lifecycle)
    # If already retired, skip
    current_state = master_df.loc[master_df["master_vendor_id"] == target_id, "lifecycle_state"].iloc[0]
    if current_state == "ACTIVE":
        master_df, history_df, change_log_df = deprecate(
            master_df, history_df, change_log_df,
            master_vendor_id=target_id,
            actor="mdm_owner_1",
            reason="Duplicate vendor consolidated into a newer supplier master"
        )

    # Retire (demonstrate lifecycle)
    current_state = master_df.loc[master_df["master_vendor_id"] == target_id, "lifecycle_state"].iloc[0]
    if current_state in ["ACTIVE", "DEPRECATED"]:
        master_df, history_df, change_log_df = retire(
            master_df, history_df, change_log_df,
            master_vendor_id=target_id,
            actor="mdm_owner_1",
            reason="Vendor no longer valid; archived for compliance"
        )

    # -------------------------
    # STEWARD ACTIVITY LOG
    # -------------------------
    steward_activity_df = pd.DataFrame()
    if os.path.exists(STEWARD_QUEUE_FILE):
        sq = pd.read_csv(STEWARD_QUEUE_FILE)
        # minimal steward activity metrics
        steward_activity_df = (
            sq.groupby(["reviewed_by", "review_status"])
              .size()
              .reset_index(name="count")
        )

    # -------------------------
    # GOVERNANCE METRICS (simple dashboard base)
    # -------------------------
    total_master = len(master_df)
    active_cnt = (master_df["lifecycle_state"] == "ACTIVE").sum()
    deprecated_cnt = (master_df["lifecycle_state"] == "DEPRECATED").sum()
    retired_cnt = (master_df["lifecycle_state"] == "RETIRED").sum()

    governance_metrics_df = pd.DataFrame([{
        "domain": "vendors",
        "total_master_records": total_master,
        "active_records": int(active_cnt),
        "deprecated_records": int(deprecated_cnt),
        "retired_records": int(retired_cnt),
        "lifecycle_events_logged": len(history_df)
    }])

    # -------------------------
    # WRITE OUTPUT FILES
    # -------------------------
    master_df.to_csv(MASTER_OUTPUT_FILE, index=False)
    history_df.to_csv(HISTORY_OUTPUT_FILE, index=False)
    change_log_df.to_csv(CHANGE_LOG_FILE, index=False)

    if not steward_activity_df.empty:
        steward_activity_df.to_csv(STEWARD_ACTIVITY_FILE, index=False)
    else:
        # create empty file for completeness
        pd.DataFrame(columns=["reviewed_by", "review_status", "count"]).to_csv(STEWARD_ACTIVITY_FILE, index=False)

    governance_metrics_df.to_csv(GOV_METRICS_FILE, index=False)

    # -------------------------
    # PRINT SUMMARY
    # -------------------------
    print("\n✅ Lifecycle Management Completed")
    print(f" - Master snapshot: {MASTER_OUTPUT_FILE}")
    print(f" - Lifecycle history: {HISTORY_OUTPUT_FILE}")
    print(f" - Change log: {CHANGE_LOG_FILE}")
    print(f" - Steward activity: {STEWARD_ACTIVITY_FILE}")
    print(f" - Governance metrics: {GOV_METRICS_FILE}")

    print("\n--- CURRENT MASTER STATES ---")
    print(master_df[["master_vendor_id", "golden_vendor_name", "lifecycle_state", "effective_from", "effective_to"]])


if __name__ == "__main__":
    main()
