import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Row

# ------------------------------------------------------------
# Args
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_DELTA_PATH",
    "CURATED_DELTA_PATH",
    "QUARANTINE_PATH",
    "DQ_REPORT_PATH",
    "QUALITY_SCORECARD_CSV_OUT",
    "QUALITY_BY_DOMAIN_CSV_OUT",
])

JOB_NAME = args["JOB_NAME"]
INPUT_DELTA_PATH = args["INPUT_DELTA_PATH"].rstrip("/")
CURATED_DELTA_PATH = args["CURATED_DELTA_PATH"].rstrip("/")
QUARANTINE_PATH = args["QUARANTINE_PATH"].rstrip("/")
DQ_REPORT_PATH = args["DQ_REPORT_PATH"].rstrip("/")
QUALITY_SCORECARD_CSV_OUT = args["QUALITY_SCORECARD_CSV_OUT"].rstrip("/")
QUALITY_BY_DOMAIN_CSV_OUT = args["QUALITY_BY_DOMAIN_CSV_OUT"].rstrip("/")

# ------------------------------------------------------------
# Glue / Spark
# ------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print("=== Day 8 Quality Gates + Governance Scorecards (Schema-stable) ===")
print("INPUT_DELTA_PATH:", INPUT_DELTA_PATH)
print("CURATED_DELTA_PATH:", CURATED_DELTA_PATH)
print("QUARANTINE_PATH:", QUARANTINE_PATH)
print("DQ_REPORT_PATH:", DQ_REPORT_PATH)
print("QUALITY_SCORECARD_CSV_OUT:", QUALITY_SCORECARD_CSV_OUT)
print("QUALITY_BY_DOMAIN_CSV_OUT:", QUALITY_BY_DOMAIN_CSV_OUT)

# ------------------------------------------------------------
# Helper: does delta path exist?
# ------------------------------------------------------------
def delta_exists(path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).count()
        return True
    except Exception:
        return False

# ------------------------------------------------------------
# Read base dataset
# - If curated exists (from Day7), validate curated to keep schema stable
# - Else validate input and create curated
# ------------------------------------------------------------
if delta_exists(CURATED_DELTA_PATH):
    print("✅ Curated Delta exists. Validating CURATED data to keep schema stable.")
    base_df = spark.read.format("delta").load(CURATED_DELTA_PATH)
    base_source = "CURATED"
else:
    print("ℹ️ Curated Delta not found. Validating INPUT and creating CURATED.")
    base_df = spark.read.format("delta").load(INPUT_DELTA_PATH)
    base_source = "INPUT"

base_cols = base_df.columns
total_rows = base_df.count()

print("Base source:", base_source)
print("Total base rows:", total_rows)
print("Columns:", base_cols)

# ------------------------------------------------------------
# Detect pickup/dropoff cols (supports variants)
# ------------------------------------------------------------
pickup_col = None
dropoff_col = None

for c in ["tpep_pickup_datetime", "pickup_datetime", "lpep_pickup_datetime", "pickup_ts"]:
    if c in base_cols:
        pickup_col = c
        break

for c in ["tpep_dropoff_datetime", "dropoff_datetime", "lpep_dropoff_datetime", "dropoff_ts"]:
    if c in base_cols:
        dropoff_col = c
        break

# ------------------------------------------------------------
# Define Quality Rules (rule_name -> boolean condition)
# ------------------------------------------------------------
rules = []

if "VendorID" in base_cols:
    rules.append(("vendor_not_null", F.col("VendorID").isNotNull()))

if pickup_col:
    rules.append(("pickup_not_null", F.col(pickup_col).isNotNull()))
if dropoff_col:
    rules.append(("dropoff_not_null", F.col(dropoff_col).isNotNull()))
if pickup_col and dropoff_col:
    rules.append((
        "pickup_before_dropoff",
        F.to_timestamp(F.col(pickup_col)) <= F.to_timestamp(F.col(dropoff_col))
    ))

if "PULocationID" in base_cols:
    rules.append(("pu_location_not_null", F.col("PULocationID").isNotNull()))
if "DOLocationID" in base_cols:
    rules.append(("do_location_not_null", F.col("DOLocationID").isNotNull()))

nonneg_cols = ["trip_distance", "fare_amount", "total_amount", "tip_amount", "tolls_amount", "extra"]
for c in nonneg_cols:
    if c in base_cols:
        rules.append((f"{c}_non_negative", F.col(c).isNull() | (F.col(c) >= F.lit(0))))

if "passenger_count" in base_cols:
    rules.append((
        "passenger_count_range",
        F.col("passenger_count").isNull() |
        ((F.col("passenger_count") >= 0) & (F.col("passenger_count") <= 8))
    ))

if "RatecodeID" in base_cols:
    rules.append((
        "ratecode_range",
        F.col("RatecodeID").isNull() |
        ((F.col("RatecodeID") >= 1) & (F.col("RatecodeID") <= 6))
    ))

rule_cols = [name for name, _ in rules]

# ------------------------------------------------------------
# Build flags + dq_pass
# ------------------------------------------------------------
df_flags = base_df
for name, cond in rules:
    df_flags = df_flags.withColumn(name, F.when(cond, F.lit(True)).otherwise(F.lit(False)))

if rule_cols:
    pass_expr = F.lit(True)
    for c in rule_cols:
        pass_expr = pass_expr & F.col(c)
    df_flags = df_flags.withColumn("dq_pass", pass_expr)
else:
    df_flags = df_flags.withColumn("dq_pass", F.lit(True))

# ------------------------------------------------------------
# failed_rules array (FIXED: no array_remove)
# ------------------------------------------------------------
failed_rules_raw = F.array(*[
    F.when(F.col(c) == F.lit(False), F.lit(c)).otherwise(F.lit(None))
    for c in rule_cols
])

df_flags = df_flags.withColumn("failed_rules_raw", failed_rules_raw)
df_flags = df_flags.withColumn(
    "failed_rules",
    F.expr("filter(failed_rules_raw, x -> x is not null)")
).drop("failed_rules_raw")

passed_df = df_flags.filter(F.col("dq_pass") == True)
failed_df = df_flags.filter(F.col("dq_pass") == False)

passed_count = passed_df.count()
failed_count = failed_df.count()

print("Passed rows:", passed_count)
print("Failed rows:", failed_count)

# ------------------------------------------------------------
# Write curated + quarantine
# Curated: ONLY base schema columns (keeps stable)
# ------------------------------------------------------------
curated_out = passed_df.select(*base_cols)

(
    curated_out
    .write
    .format("delta")
    .mode("overwrite")
    .save(CURATED_DELTA_PATH)
)

(
    failed_df
    .drop("dq_pass")
    .write
    .mode("overwrite")
    .format("parquet")
    .save(QUARANTINE_PATH)
)

print("✅ Curated Delta written:", CURATED_DELTA_PATH)
print("✅ Quarantine written:", QUARANTINE_PATH)

# ------------------------------------------------------------
# DQ Summary Report (JSON)
# ------------------------------------------------------------
rule_fail_exprs = [
    F.sum(F.when(F.col(c) == False, 1).otherwise(0)).alias(f"{c}_fail_count")
    for c in rule_cols
]

summary = (
    df_flags.agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("dq_pass") == True, 1).otherwise(0)).alias("passed_rows"),
        F.sum(F.when(F.col("dq_pass") == False, 1).otherwise(0)).alias("failed_rows"),
        *rule_fail_exprs
    )
    .withColumn("job_name", F.lit(JOB_NAME))
    .withColumn("base_source", F.lit(base_source))
    .withColumn("input_path", F.lit(INPUT_DELTA_PATH))
    .withColumn("curated_delta_path", F.lit(CURATED_DELTA_PATH))
    .withColumn("quarantine_path", F.lit(QUARANTINE_PATH))
    .withColumn("generated_at_utc", F.current_timestamp())
)

summary.coalesce(1).write.mode("overwrite").json(DQ_REPORT_PATH)
print("✅ DQ report written:", DQ_REPORT_PATH)

# ------------------------------------------------------------
# GOVERNANCE QUALITY SCORECARD (CSV)
# ------------------------------------------------------------
rule_governance = {
    "vendor_not_null": {
        "domain": "trips",
        "quality_dimension": "Completeness",
        "business_metric": "% trips missing VendorID",
        "owner": "Data Steward",
        "threshold": "< 1%",
        "action_on_failure": "Block load"
    },
    "pickup_not_null": {
        "domain": "trips",
        "quality_dimension": "Completeness",
        "business_metric": "% trips missing pickup datetime",
        "owner": "Data Steward",
        "threshold": "< 0.1%",
        "action_on_failure": "Block load"
    },
    "dropoff_not_null": {
        "domain": "trips",
        "quality_dimension": "Completeness",
        "business_metric": "% trips missing dropoff datetime",
        "owner": "Data Steward",
        "threshold": "< 0.1%",
        "action_on_failure": "Block load"
    },
    "pickup_before_dropoff": {
        "domain": "trips",
        "quality_dimension": "Validity",
        "business_metric": "% trips with dropoff before pickup",
        "owner": "Data Owner",
        "threshold": "0%",
        "action_on_failure": "Quarantine bad rows"
    },
    "pu_location_not_null": {
        "domain": "trips",
        "quality_dimension": "Completeness",
        "business_metric": "% trips missing pickup zone",
        "owner": "Data Steward",
        "threshold": "< 0.5%",
        "action_on_failure": "Quarantine bad rows"
    },
    "do_location_not_null": {
        "domain": "trips",
        "quality_dimension": "Completeness",
        "business_metric": "% trips missing dropoff zone",
        "owner": "Data Steward",
        "threshold": "< 0.5%",
        "action_on_failure": "Quarantine bad rows"
    }
}

summary_row = summary.collect()[0].asDict()
total = int(summary_row.get("total_rows", 0))
run_id = spark.sparkContext.applicationId

rows = []
for rule_name, meta in rule_governance.items():
    fail_count_key = f"{rule_name}_fail_count"
    rule_failed = int(summary_row.get(fail_count_key, 0))
    observed_pct = (rule_failed / total) if total > 0 else 0.0
    pass_fail = "PASS" if rule_failed == 0 else "FAIL"

    rows.append(Row(
        run_id=run_id,
        generated_at_utc=str(summary_row.get("generated_at_utc")),
        domain=meta["domain"],
        quality_dimension=meta["quality_dimension"],
        business_metric=meta["business_metric"],
        owner=meta["owner"],
        threshold=meta["threshold"],
        action_on_failure=meta["action_on_failure"],
        observed_value=round(observed_pct, 6),
        pass_fail=pass_fail,
        failed_rows=rule_failed,
        total_rows=total
    ))

quality_scorecard_df = spark.createDataFrame(rows)

(
    quality_scorecard_df
    .coalesce(1)
    .write
    .mode("append")
    .option("header", True)
    .csv(QUALITY_SCORECARD_CSV_OUT)
)

print("✅ Quality scorecard CSV appended:", QUALITY_SCORECARD_CSV_OUT)

# ------------------------------------------------------------
# DASHBOARD: QUALITY METRICS BY DOMAIN (CSV)
# ------------------------------------------------------------
quality_by_domain = (
    quality_scorecard_df
    .groupBy("run_id", "domain")
    .agg(
        F.avg("observed_value").alias("avg_rule_fail_rate"),
        F.sum(F.when(F.col("pass_fail") == "FAIL", 1).otherwise(0)).alias("failed_rules"),
        F.count("*").alias("total_rules")
    )
    .withColumn("generated_at_utc", F.current_timestamp())
)

(
    quality_by_domain
    .coalesce(1)
    .write
    .mode("append")
    .option("header", True)
    .csv(QUALITY_BY_DOMAIN_CSV_OUT)
)

print("✅ Quality by domain CSV appended:", QUALITY_BY_DOMAIN_CSV_OUT)
print("🎉 Day 8 completed successfully.")
