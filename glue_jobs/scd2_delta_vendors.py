import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def main():
    # --------------------------------------------------
    # CONFIG (YOUR PATHS)
    # --------------------------------------------------
    JOB_NAME = "scd2_delta_vendors"

    SOURCE_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/curated_output/"
    TARGET_DELTA_PATH = "s3://nyc-taxi-data-dev-ravikanth-us-east-1/master/vendors_roll/"
    ENTITY_KEY = "vendorid"

    # --------------------------------------------------
    # INIT (Glue creates SparkContext; reuse it)
    # --------------------------------------------------
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(JOB_NAME, {})

    # Safe to create Spark expressions ONLY after Spark is ready
    MAX_TS = F.expr("timestamp('9999-12-31')")

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------
    def add_record_hash(df):
        ignore_cols = {
            ENTITY_KEY,
            "version_no",
            "is_current",
            "effective_start_ts",
            "effective_end_ts",
            "record_hash"
        }
        business_cols = [c for c in df.columns if c not in ignore_cols]

        return df.withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in business_cols]
                ),
                256
            )
        )

    # --------------------------------------------------
    # READ SOURCE
    # --------------------------------------------------
    src = spark.read.parquet(SOURCE_PATH)

    if ENTITY_KEY not in src.columns:
        raise Exception(f"Missing ENTITY_KEY {ENTITY_KEY}. Columns: {src.columns}")

    src = add_record_hash(src)

    now_ts = F.current_timestamp()

    incoming = (
        src
        .withColumn("effective_start_ts", now_ts)
        .withColumn("effective_end_ts", MAX_TS)
        .withColumn("is_current", F.lit(True))
    )

    # Deduplicate source by ENTITY_KEY (choose 1 row per key)
    # If you have a real timestamp column, orderBy that instead of F.lit(1)
    w = Window.partitionBy(ENTITY_KEY).orderBy(F.lit(1))
    incoming = (
        incoming
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # --------------------------------------------------
    # FIRST RUN → CREATE DELTA TABLE
    # --------------------------------------------------
    if not DeltaTable.isDeltaTable(spark, TARGET_DELTA_PATH):
        (
            incoming
            .withColumn("version_no", F.lit(1))
            .write.format("delta")
            .mode("overwrite")
            .save(TARGET_DELTA_PATH)
        )

        print("✅ Initial SCD2 Delta table created at:", TARGET_DELTA_PATH)
        job.commit()
        return

    # --------------------------------------------------
    # LOAD CURRENT DELTA
    # --------------------------------------------------
    delta_tbl = DeltaTable.forPath(spark, TARGET_DELTA_PATH)

    current = (
        delta_tbl.toDF()
        .filter(F.col("is_current") == True)
        .select(
            F.col(ENTITY_KEY).alias("k"),
            F.col("record_hash").alias("curr_hash"),
            F.col("version_no").alias("curr_version")
        )
    )

    staged = (
        incoming
        .join(current, incoming[ENTITY_KEY] == current["k"], "left")
        .withColumn("is_new", F.col("k").isNull())
        .withColumn(
            "is_changed",
            (F.col("k").isNotNull()) & (F.col("record_hash") != F.col("curr_hash"))
        )
        .withColumn(
            "next_version_no",
            F.when(F.col("curr_version").isNull(), F.lit(1)).otherwise(F.col("curr_version") + F.lit(1))
        )
    )

    # --------------------------------------------------
    # EXPIRE OLD RECORDS (only for changed keys)
    # --------------------------------------------------
    changed_keys = (
        staged
        .filter(F.col("is_changed") == True)
        .select(F.col(ENTITY_KEY).alias("ck"))
        .distinct()
    )

    # If there are no changed keys, skip merge to avoid extra work
    if changed_keys.take(1):
        (
            delta_tbl.alias("t")
            .merge(
                changed_keys.alias("c"),
                f"t.{ENTITY_KEY} = c.ck AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current": F.lit(False),
                "effective_end_ts": F.current_timestamp()
            })
            .execute()
        )

    # --------------------------------------------------
    # INSERT NEW + CHANGED
    # --------------------------------------------------
    to_insert = staged.filter((F.col("is_new") == True) | (F.col("is_changed") == True))

    # IMPORTANT: select columns directly from staged (no "s." alias usage)
    insert_cols = incoming.columns  # includes effective_start_ts/end_ts/is_current/record_hash
    insert_df = to_insert.select(
        *[F.col(c) for c in insert_cols],
        F.col("next_version_no").alias("version_no")
    )

    # If nothing to insert, just finish cleanly
    if not insert_df.take(1):
        print("✅ No new/changed rows. Delta table is already up to date.")
        job.commit()
        return

    (
        insert_df.write
        .format("delta")
        .mode("append")
        .save(TARGET_DELTA_PATH)
    )

    print("🎉 SCD Type 2 Delta update completed successfully")
    job.commit()


if __name__ == "__main__":
    main()
