import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------
# Args
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_ZONES_PATH",
    "MASTER_OUT_PATH",
    "STEWARD_QUEUE_PATH",
    "REJECTS_PATH",
    "AUDIT_PATH",
    "HIGH_CONF",
    "MED_CONF",

    # NEW dashboard outputs
    "MASTER_APPROVAL_QUEUE_CSV_OUT",
    "MDM_SCORECARD_CSV_OUT",
])

JOB_NAME = args["JOB_NAME"]
RAW_ZONES_PATH = args["RAW_ZONES_PATH"].rstrip("/")
MASTER_OUT_PATH = args["MASTER_OUT_PATH"].rstrip("/")
STEWARD_QUEUE_PATH = args["STEWARD_QUEUE_PATH"].rstrip("/")
REJECTS_PATH = args["REJECTS_PATH"].rstrip("/")
AUDIT_PATH = args["AUDIT_PATH"].rstrip("/")
HIGH_CONF = float(args["HIGH_CONF"])
MED_CONF = float(args["MED_CONF"])

MASTER_APPROVAL_QUEUE_CSV_OUT = args["MASTER_APPROVAL_QUEUE_CSV_OUT"].rstrip("/")
MDM_SCORECARD_CSV_OUT = args["MDM_SCORECARD_CSV_OUT"].rstrip("/")

# ------------------------------------------------------------
# Spark
# ------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== Day 9 MDM Matching & Dedup + Governance CSVs ===")
print("RAW_ZONES_PATH:", RAW_ZONES_PATH)
print("MASTER_OUT_PATH:", MASTER_OUT_PATH)
print("STEWARD_QUEUE_PATH:", STEWARD_QUEUE_PATH)
print("REJECTS_PATH:", REJECTS_PATH)
print("AUDIT_PATH:", AUDIT_PATH)
print("HIGH_CONF:", HIGH_CONF, "MED_CONF:", MED_CONF)
print("MASTER_APPROVAL_QUEUE_CSV_OUT:", MASTER_APPROVAL_QUEUE_CSV_OUT)
print("MDM_SCORECARD_CSV_OUT:", MDM_SCORECARD_CSV_OUT)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def normalize_text_col(colname: str):
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(F.upper(F.col(colname).cast("string")), r"[^A-Z0-9 ]", " "),
            r"\s+", " "
        )
    )

def normalized_lev_similarity(col_a, col_b):
    max_len = F.greatest(F.length(col_a), F.length(col_b))
    lev = F.levenshtein(col_a, col_b)
    return F.when(max_len == 0, F.lit(0.0)).otherwise(F.round(1 - (lev / max_len), 4))

def write_delta_if_available(df, path):
    try:
        df.write.mode("overwrite").format("delta").save(path)
        print(f"✅ Wrote DELTA: {path}")
        return "delta"
    except Exception as e:
        print("⚠ Delta write failed; falling back to Parquet. Reason:", str(e)[:300])
        df.write.mode("overwrite").format("parquet").save(path)
        print(f"✅ Wrote PARQUET: {path}")
        return "parquet"

# ------------------------------------------------------------
# Read zones reference (CSV)
# ------------------------------------------------------------
zones = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_ZONES_PATH)
)

if "LocationID" not in zones.columns:
    raise Exception(f"Expected LocationID in zones. Found: {zones.columns}")

zone_col = "Zone" if "Zone" in zones.columns else ("zone_name" if "zone_name" in zones.columns else None)
if zone_col is None:
    raise Exception(f"Expected Zone or zone_name in zones. Found: {zones.columns}")

z = (
    zones
    .withColumnRenamed("LocationID", "location_id")
    .withColumn("location_id", F.col("location_id").cast("int"))
    .withColumn("zone_norm", normalize_text_col(zone_col))
)

if "Borough" in z.columns:
    z = z.withColumn("borough_norm", normalize_text_col("Borough"))
else:
    z = z.withColumn("borough_norm", F.lit("UNKNOWN"))

z = z.withColumn("match_key", F.concat_ws("::", F.col("borough_norm"), F.col("zone_norm")))

total_zone_rows = z.count()
print("Zones rows:", total_zone_rows)

# ------------------------------------------------------------
# Candidate pair generation within borough (simple, scalable enough for this dataset)
# ------------------------------------------------------------
a = z.alias("a")
b = z.alias("b")

pairs = (
    a.join(
        b,
        (F.col("a.borough_norm") == F.col("b.borough_norm")) &
        (F.col("a.location_id") < F.col("b.location_id")),
        "inner"
    )
    .select(
        F.col("a.location_id").alias("id_a"),
        F.col("b.location_id").alias("id_b"),
        F.col("a.zone_norm").alias("zone_a"),
        F.col("b.zone_norm").alias("zone_b"),
        F.col("a.borough_norm").alias("borough"),
    )
)

# Similarity + confidence
pairs = pairs.withColumn("name_similarity", normalized_lev_similarity(F.col("zone_a"), F.col("zone_b")))

pairs = (pairs
    .withColumn("token_a", F.split(F.col("zone_a"), " ").getItem(0))
    .withColumn("token_b", F.split(F.col("zone_b"), " ").getItem(0))
    .withColumn("token_match", F.when(F.col("token_a") == F.col("token_b"), F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("match_confidence", F.round(F.col("name_similarity") * F.lit(0.90) + F.col("token_match") * F.lit(0.10), 4))
)

pairs = pairs.withColumn(
    "decision",
    F.when(F.col("match_confidence") >= F.lit(HIGH_CONF), F.lit("AUTO_MERGE"))
     .when(F.col("match_confidence") >= F.lit(MED_CONF), F.lit("STEWARD_REVIEW"))
     .otherwise(F.lit("REJECT"))
)

total_pairs = pairs.count()
auto_merge_pairs = pairs.filter(F.col("decision") == "AUTO_MERGE").count()
steward_review_pairs = pairs.filter(F.col("decision") == "STEWARD_REVIEW").count()
reject_pairs = pairs.filter(F.col("decision") == "REJECT").count()

# ------------------------------------------------------------
# Steward review queue (dataset) + dashboard CSV
# ------------------------------------------------------------
steward_queue_pairs = (
    pairs.filter(F.col("decision") == "STEWARD_REVIEW")
         .withColumn("review_status", F.lit("PENDING"))
         .withColumn("submitted_at_utc", F.current_timestamp())
         .select(
             "id_a", "id_b", "zone_a", "zone_b", "borough",
             "name_similarity", "match_confidence", "decision",
             "review_status", "submitted_at_utc"
         )
         .orderBy(F.desc("match_confidence"))
)

# Save steward queue data (parquet) + dashboard CSV
steward_queue_pairs.write.mode("overwrite").format("parquet").save(STEWARD_QUEUE_PATH)

(steward_queue_pairs
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(MASTER_APPROVAL_QUEUE_CSV_OUT))

print("✅ Steward queue parquet:", STEWARD_QUEUE_PATH)
print("✅ Master approval queue CSV:", MASTER_APPROVAL_QUEUE_CSV_OUT)

# ------------------------------------------------------------
# Rejects
# ------------------------------------------------------------
reject_df = pairs.filter(F.col("decision") == "REJECT")
reject_df.write.mode("overwrite").format("parquet").save(REJECTS_PATH)
print("✅ Reject pairs written:", REJECTS_PATH)

# ------------------------------------------------------------
# Golden master creation (survivorship: min(location_id) per match_key)
# ------------------------------------------------------------
gold_w = Window.partitionBy("match_key").orderBy(F.col("location_id").asc())

golden_master = (
    z.withColumn("rn", F.row_number().over(gold_w))
     .filter(F.col("rn") == 1)
     .drop("rn")
     .withColumn("mdm_state", F.lit("ACTIVE"))
     .withColumn("created_at_utc", F.current_timestamp())
     .withColumn("updated_at_utc", F.current_timestamp())
     .withColumn("survivorship_rule", F.lit("min(location_id) per match_key"))
     # IMPORTANT: Day10 expects LocationID column
     .withColumnRenamed("location_id", "LocationID")
)

master_format = write_delta_if_available(golden_master, MASTER_OUT_PATH)

# ------------------------------------------------------------
# Audit JSON (existing)
# ------------------------------------------------------------
audit = (
    spark.createDataFrame([{
        "total_zone_rows": int(total_zone_rows),
        "total_pairs_compared": int(total_pairs),
        "auto_merge_pairs": int(auto_merge_pairs),
        "steward_review_pairs": int(steward_review_pairs),
        "reject_pairs": int(reject_pairs),
        "high_conf_threshold": float(HIGH_CONF),
        "med_conf_threshold": float(MED_CONF),
        "raw_zones_path": RAW_ZONES_PATH,
        "master_out_path": MASTER_OUT_PATH,
        "master_format": master_format,
        "steward_queue_path": STEWARD_QUEUE_PATH,
        "rejects_path": REJECTS_PATH,
        "job_name": JOB_NAME
    }]).withColumn("generated_at_utc", F.current_timestamp())
)

audit.coalesce(1).write.mode("overwrite").json(AUDIT_PATH)
print("✅ Audit report written:", AUDIT_PATH)

# ------------------------------------------------------------
# MDM SCORECARD (CSV) with business impact mapping
# ------------------------------------------------------------
mdm_scorecard = (
    spark.createDataFrame([{
        "run_id": spark.sparkContext.applicationId,
        "domain": "taxi_zones",
        "metric": "duplicate_candidates_rate",
        "business_impact": "Bad master data causes incorrect joins → wrong zone reporting and broken downstream analytics",
        "owner": "MDM Steward",
        "threshold": "<= 2%",
        "action_on_failure": "Trigger steward review workflow",
        "total_records": int(total_zone_rows),
        "pairs_compared": int(total_pairs),
        "auto_merge_pairs": int(auto_merge_pairs),
        "steward_review_pairs": int(steward_review_pairs),
        "reject_pairs": int(reject_pairs)
    }])
    .withColumn("generated_at_utc", F.current_timestamp())
)

(mdm_scorecard
 .coalesce(1)
 .write.mode("append")
 .option("header", True)
 .csv(MDM_SCORECARD_CSV_OUT))

print("✅ MDM scorecard CSV appended:", MDM_SCORECARD_CSV_OUT)

print("🎉 Day 9 completed successfully.")
