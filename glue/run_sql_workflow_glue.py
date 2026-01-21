import sys
import re
import boto3
from typing import List, Tuple

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ------------------------------------------------------------
# Args
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",

    "ZONE_MASTER_S3_PATH",        # s3://.../raw_data/reference/taxi_zone_lookup/
    "ZONE_MASTER_FORMAT",         # csv|parquet
    "ZONE_MASTER_HAS_HEADER",     # true|false

    "SQL_S3_PREFIX",              # s3://.../scripts/sql_with_scd2/sql

    "PG_HOST",
    "PG_PORT",
    "PG_DB",
    "PG_USER",
    "PG_PASSWORD",

    "DATA_OWNER",
    "RUN_TRIGGER",
])

JOB_NAME = args["JOB_NAME"]
ZONE_MASTER_S3_PATH = args["ZONE_MASTER_S3_PATH"].rstrip("/")
ZONE_MASTER_FORMAT = args["ZONE_MASTER_FORMAT"].strip().lower()
ZONE_MASTER_HAS_HEADER = args["ZONE_MASTER_HAS_HEADER"].strip().lower() == "true"
SQL_S3_PREFIX = args["SQL_S3_PREFIX"].rstrip("/") + "/"

PG_HOST = args["PG_HOST"]
PG_PORT = int(args["PG_PORT"])
PG_DB = args["PG_DB"]
PG_USER = args["PG_USER"]
PG_PASSWORD = args["PG_PASSWORD"]

DATA_OWNER = args.get("DATA_OWNER", "analytics-team")
RUN_TRIGGER = args.get("RUN_TRIGGER", "manual")

# ------------------------------------------------------------
# Spark / Glue
# ------------------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
jdbc_props = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ------------------------------------------------------------
# Helpers: S3
# ------------------------------------------------------------
s3 = boto3.client("s3")

def parse_s3_url(s3_url: str) -> Tuple[str, str]:
    m = re.match(r"^s3://([^/]+)/?(.*)$", s3_url)
    if not m:
        raise ValueError(f"Invalid S3 url: {s3_url}")
    return m.group(1), m.group(2)

def list_s3_keys(bucket: str, prefix: str) -> List[str]:
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".sql"):
                keys.append(k)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return sorted(keys)

def read_s3_text(bucket: str, key: str) -> str:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

# ------------------------------------------------------------
# Helper: execute SQL over JDBC (no psycopg2)
# ------------------------------------------------------------
def exec_sql(sql_text: str):
    sql_text = sql_text.strip()
    if not sql_text:
        return
    # Spark JDBC execution trick: wrap SQL in a subquery so Spark sends it to DB.
    # Works for SELECT. For DDL/DML, we use "query" to trigger execution:
    # We can use "SELECT 1" after running statements split by ';' via "CALL" pattern isn't possible.
    # So we execute each statement using the Postgres "DO $$ ... $$" wrapper when needed.
    # Instead: split statements and run each as "SELECT 1" with side-effect using "BEGIN; ...; COMMIT;" is not supported.
    #
    # Best reliable method: use Spark's JVM DriverManager directly.
    jvm = spark._sc._jvm
    conn = None
    stmt = None
    try:
        conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, PG_USER, PG_PASSWORD)
        stmt = conn.createStatement()
        stmt.execute(sql_text)
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()

def exec_sql_file(bucket: str, key: str):
    print(f"Executing SQL: s3://{bucket}/{key}")
    sql_text = read_s3_text(bucket, key)
    # Remove BOM if present
    sql_text = sql_text.lstrip("\ufeff")
    # Execute whole file (Postgres handles multiple statements separated by ;)
    exec_sql(sql_text)

# ------------------------------------------------------------
# Generate run_id inside Spark (no DB dependency)
# ------------------------------------------------------------
RUN_ID = spark.range(1).select(F.md5(F.concat_ws("||", F.rand().cast("string"), F.current_timestamp().cast("string"))).alias("run_id")).collect()[0]["run_id"]
print("RUN_ID:", RUN_ID)

# ------------------------------------------------------------
# Ensure schemas exist + pipeline audit row (in case schema scripts not run yet)

# ------------------------------------------------------------

bootstrap_sql = f"""
CREATE EXTENSION IF NOT EXISTS pgcrypto;
create schema if not exists governance;
create table if not exists governance.pipeline_run_audit (
  run_id text primary key,
  job_name text,
  trigger_type text,
  data_owner text,
  status text,
  started_at timestamp,
  ended_at timestamp,
  error_message text
);
insert into governance.pipeline_run_audit(run_id, job_name, trigger_type, data_owner, status, started_at)
values ('{RUN_ID}', '{JOB_NAME}', '{RUN_TRIGGER}', '{DATA_OWNER}', 'STARTED', now())
on conflict (run_id) do nothing;
"""
exec_sql(bootstrap_sql)

# ------------------------------------------------------------
# Load taxi_zone_lookup.csv from S3 into staging table
# ------------------------------------------------------------
print("Loading zone master feed from:", ZONE_MASTER_S3_PATH)

if ZONE_MASTER_FORMAT == "csv":
    df = (
        spark.read.option("header", str(ZONE_MASTER_HAS_HEADER).lower())
        .option("inferSchema", "true")
        .csv(ZONE_MASTER_S3_PATH)
    )
elif ZONE_MASTER_FORMAT == "parquet":
    df = spark.read.parquet(ZONE_MASTER_S3_PATH)
else:
    raise ValueError("ZONE_MASTER_FORMAT must be csv or parquet")

# Normalize columns from taxi_zone_lookup.csv:
# Typical columns: LocationID, Borough, Zone, service_zone
lower_map = {c.lower(): c for c in df.columns}

def col(name):
    if name not in lower_map:
        raise ValueError(f"Missing column '{name}' in input. Found columns: {df.columns}")
    return F.col(lower_map[name])

df2 = (
    df.select(
        col("locationid").cast("int").alias("location_id"),
        col("borough").cast("string").alias("borough"),
        col("zone").cast("string").alias("zone_name"),
        col("service_zone").cast("string").alias("service_zone"),
    )
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("run_id", F.lit(RUN_ID))
)

# Ensure staging schema/table exist
exec_sql("""
create schema if not exists staging;
create table if not exists staging.stg_zone_master_incoming(
  location_id int,
  borough text,
  zone_name text,
  service_zone text,
  ingested_at timestamp,
  run_id text
);
""")

# Append staging rows for this run
df2.write.mode("append").jdbc(jdbc_url, "staging.stg_zone_master_incoming", properties=jdbc_props)
print("Staging load completed.")

# ------------------------------------------------------------
# Set session variables for SQL scripts (so your SQL uses current_setting('app.run_id'))
# We'll do this by running SQL that sets them at the DB session-level before executing scripts.
# Since each exec opens a new JDBC connection, we embed run_id/job_name by setting them
# inside each SQL file usage with "set_config" wrapper before actual SQL execution.
# ------------------------------------------------------------
def exec_sql_with_context(sql_text: str):
    ctx = f"select set_config('app.run_id','{RUN_ID}',true); select set_config('app.job_name','{JOB_NAME}',true);"
    exec_sql(ctx + "\n" + sql_text)

def exec_sql_file_with_context(bucket: str, key: str):
    print(f"Executing SQL (ctx): s3://{bucket}/{key}")
    sql_text = read_s3_text(bucket, key).lstrip("\ufeff")
    exec_sql_with_context(sql_text)

# ------------------------------------------------------------
# Execute SQL scripts in folder order from S3
# ------------------------------------------------------------
bkt, prefix = parse_s3_url(SQL_S3_PREFIX)
sql_keys = list_s3_keys(bkt, prefix)

print("Found SQL files:", len(sql_keys))
for k in sql_keys:
    exec_sql_file_with_context(bkt, k)

# ------------------------------------------------------------
# Mark run success
# ------------------------------------------------------------
exec_sql(f"""
update governance.pipeline_run_audit
set status='SUCCEEDED', ended_at=now()
where run_id='{RUN_ID}';
""")

print("Glue SQL workflow completed successfully.")
