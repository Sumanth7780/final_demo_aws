import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ------------------------------------------------------------
# Args
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "MASTER_ZONES_DELTA_PATH",
    "CURATED_TRIPS_DELTA_PATH",
    "ORPHANS_OUT_PATH",
    "LIFECYCLE_SNAPSHOT_PATH",
    "AUDIT_HISTORY_OUT_PATH",
    "RUN_SUMMARY_PATH",

    # NEW dashboard outputs
    "ORPHANS_CSV_OUT",
    "STEWARD_ACTIVITY_LOG_CSV_OUT",
])

JOB_NAME = args["JOB_NAME"]
MASTER_ZONES_DELTA_PATH = args["MASTER_ZONES_DELTA_PATH"].rstrip("/")
CURATED_TRIPS_DELTA_PATH = args["CURATED_TRIPS_DELTA_PATH"].rstrip("/")
ORPHANS_OUT_PATH = args["ORPHANS_OUT_PATH"].rstrip("/")
LIFECYCLE_SNAPSHOT_PATH = args["LIFECYCLE_SNAPSHOT_PATH"].rstrip("/")
AUDIT_HISTORY_OUT_PATH = args["AUDIT_HISTORY_OUT_PATH"].rstrip("/")
RUN_SUMMARY_PATH = args["RUN_SUMMARY_PATH"].rstrip("/")

ORPHANS_CSV_OUT = args["ORPHANS_CSV_OUT"].rstrip("/")
STEWARD_ACTIVITY_LOG_CSV_OUT = args["STEWARD_ACTIVITY_LOG_CSV_OUT"].rstrip("/")

# ------------------------------------------------------------
# Glue / Spark
# ------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== Day 10: Lifecycle + Audit + Orphans + Governance CSVs ===")
print("JOB_NAME:", JOB_NAME)
print("MASTER_ZONES_DELTA_PATH:", MASTER_ZONES_DELTA_PATH)
print("CURATED_TRIPS_DELTA_PATH:", CURATED_TRIPS_DELTA_PATH)
print("ORPHANS_OUT_PATH:", ORPHANS_OUT_PATH)
print("LIFECYCLE_SNAPSHOT_PATH:", LIFECYCLE_SNAPSHOT_PATH)
print("AUDIT_HISTORY_OUT_PATH:", AUDIT_HISTORY_OUT_PATH)
print("RUN_SUMMARY_PATH:", RUN_SUMMARY_PATH)
print("ORPHANS_CSV_OUT:", ORPHANS_CSV_OUT)
print("STEWARD_ACTIVITY_LOG_CSV_OUT:", STEWARD_ACTIVITY_LOG_CSV_OUT)

# ------------------------------------------------------------
# Load inputs (Delta)
# ------------------------------------------------------------
zones = spark.read.format("delta").load(MASTER_ZONES_DELTA_PATH)
trips = spark.read.format("delta").load(CURATED_TRIPS_DELTA_PATH)

zones_count = zones.count()
trips_count = trips.count()
print("Zones count:", zones_count)
print("Trips count:", trips_count)
print("Zones columns:", zones.columns)
print("Trips columns:", trips.columns)

# ------------------------------------------------------------
# Validate required columns
# ------------------------------------------------------------
if "LocationID" not in zones.columns:
    raise Exception(f"Master zones must contain LocationID. Found: {zones.columns}")

if "PULocationID" not in trips.columns or "DOLocationID" not in trips.columns:
    raise Exception("Trips must contain PULocationID and DOLocationID for orphan detection.")

# ------------------------------------------------------------
# Orphan detection
# ------------------------------------------------------------
zones_ids = zones.select(F.col("LocationID").cast("int").alias("LocationID")).dropna().distinct()

trips_ids = (
    trips
    .withColumn("PULocationID_int", F.col("PULocationID").cast("int"))
    .withColumn("DOLocationID_int", F.col("DOLocationID").cast("int"))
)

pu_orphans = (
    trips_ids.join(zones_ids, trips_ids["PULocationID_int"] == zones_ids["LocationID"], "left_anti")
    .withColumn("orphan_type", F.lit("PICKUP_NOT_IN_MASTER"))
)

do_orphans = (
    trips_ids.join(zones_ids, trips_ids["DOLocationID_int"] == zones_ids["LocationID"], "left_anti")
    .withColumn("orphan_type", F.lit("DROPOFF_NOT_IN_MASTER"))
)

orphans = (
    pu_orphans.unionByName(do_orphans, allowMissingColumns=True)
    .withColumn("job_name", F.lit(JOB_NAME))
    .withColumn("detected_at_utc", F.current_timestamp())
)

orphans_count = orphans.count()
print("Orphans count:", orphans_count)

# Delta output
orphans.write.mode("overwrite").format("delta").save(ORPHANS_OUT_PATH)
print("✅ Orphans (Delta) written:", ORPHANS_OUT_PATH)

# ------------------------------------------------------------
# ORPHANS DASHBOARD CSV
# ------------------------------------------------------------
(orphans
 .coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(ORPHANS_CSV_OUT))

print("✅ Orphans dashboard CSV written:", ORPHANS_CSV_OUT)

# ------------------------------------------------------------
# Lifecycle snapshot (Delta)
# ------------------------------------------------------------
if "mdm_state" in zones.columns:
    lifecycle = (
        zones.groupBy("mdm_state")
        .agg(F.count("*").alias("record_count"))
        .withColumn("job_name", F.lit(JOB_NAME))
        .withColumn("snapshot_at_utc", F.current_timestamp())
    )
else:
    lifecycle = (
        zones.select(F.lit("ACTIVE").alias("mdm_state"))
        .groupBy("mdm_state")
        .agg(F.count("*").alias("record_count"))
        .withColumn("job_name", F.lit(JOB_NAME))
        .withColumn("snapshot_at_utc", F.current_timestamp())
    )

lifecycle.write.mode("overwrite").format("delta").save(LIFECYCLE_SNAPSHOT_PATH)
print("✅ Lifecycle snapshot (Delta) written:", LIFECYCLE_SNAPSHOT_PATH)

# ------------------------------------------------------------
# Delta History Audit using DeltaTable API
# ------------------------------------------------------------
def delta_history_df(path: str, table_name: str):
    dt = DeltaTable.forPath(spark, path)
    h = dt.history()
    return (
        h.withColumn("table_name", F.lit(table_name))
         .withColumn("table_path", F.lit(path))
         .withColumn("job_name", F.lit(JOB_NAME))
         .withColumn("captured_at_utc", F.current_timestamp())
    )

zones_hist = delta_history_df(MASTER_ZONES_DELTA_PATH, "master_zones")
trips_hist = delta_history_df(CURATED_TRIPS_DELTA_PATH, "curated_trips")

history = zones_hist.unionByName(trips_hist, allowMissingColumns=True)

history.write.mode("overwrite").format("delta").save(AUDIT_HISTORY_OUT_PATH)
print("✅ Delta history audit (Delta) written:", AUDIT_HISTORY_OUT_PATH)

# ------------------------------------------------------------
# Run summary (Delta)
# ------------------------------------------------------------
summary = (
    spark.createDataFrame([{
        "job_name": JOB_NAME,
        "master_zones_path": MASTER_ZONES_DELTA_PATH,
        "curated_trips_path": CURATED_TRIPS_DELTA_PATH,
        "orphans_out_path": ORPHANS_OUT_PATH,
        "lifecycle_snapshot_path": LIFECYCLE_SNAPSHOT_PATH,
        "audit_history_out_path": AUDIT_HISTORY_OUT_PATH,
        "orphans_count": int(orphans_count),
        "zones_count": int(zones_count),
        "trips_count": int(trips_count),
    }])
    .withColumn("generated_at_utc", F.current_timestamp())
)

summary.write.mode("overwrite").format("delta").save(RUN_SUMMARY_PATH)
print("✅ Run summary (Delta) written:", RUN_SUMMARY_PATH)

# ------------------------------------------------------------
# STEWARD ACTIVITY LOG (CSV) - system events per run
# ------------------------------------------------------------
run_id = spark.sparkContext.applicationId

steward_log = (
    spark.createDataFrame([{
        "run_id": run_id,
        "event_type": "PIPELINE_RUN_SUMMARY",
        "actor": "SYSTEM",
        "details": f"zones={zones_count}, trips={trips_count}, orphans={orphans_count}",
        "job_name": JOB_NAME
    }])
    .withColumn("event_time_utc", F.current_timestamp())
)

(steward_log
 .coalesce(1)
 .write.mode("append")
 .option("header", True)
 .csv(STEWARD_ACTIVITY_LOG_CSV_OUT))

print("✅ Steward activity log CSV appended:", STEWARD_ACTIVITY_LOG_CSV_OUT)

print("🎉 Day 10 completed successfully.")
