import os
import time
import json
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
athena = boto3.client("athena")

BUCKET = os.environ["BUCKET"]
SQL_PREFIX = os.environ["SQL_PREFIX"].rstrip("/")
ATHENA_RESULTS = os.environ["ATHENA_RESULTS"]  # s3://bucket/athena_results/
ATHENA_WG = os.environ["ATHENA_WG"]
DB_MASTER = os.environ["DB_MASTER"]
DB_CURATED = os.environ["DB_CURATED"]

# Your ddl filenames (you can add more later)
DEFAULT_SQL_FILES = [
    "00_create_master_db.sql",
    "01_create_curated_db.sql",
    "02_create_dim_zone.sql",
    "03_create_fact_trip.sql",
    "10_create_certified_view.sql",
]


def _parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    no_scheme = uri.replace("s3://", "", 1)
    bucket, _, key = no_scheme.partition("/")
    return bucket, key


def _read_sql_from_s3(filename: str) -> str:
    key = f"{SQL_PREFIX}/{filename}"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8")


def _start_query(sql: str, database: str) -> str:
    result_bucket, result_prefix = _parse_s3_uri(ATHENA_RESULTS)
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=ATHENA_WG,
        ResultConfiguration={"OutputLocation": f"s3://{result_bucket}/{result_prefix}"},
    )
    return resp["QueryExecutionId"]


def _wait_query(qid: str, timeout_sec: int = 600):
    start = time.time()
    while True:
        resp = athena.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return resp
        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Athena query timed out after {timeout_sec}s. qid={qid}")
        time.sleep(2)


def lambda_handler(event, context):
    """
    Expected Step Functions input includes:
      event["athena"]["db_master"], event["athena"]["db_curated"] (optional overrides)
      event["athena"]["sql_files"] (optional list override)
    """
    athena_cfg = event.get("athena", {}) or {}
    sql_files = athena_cfg.get("sql_files", DEFAULT_SQL_FILES)

    # Allow override from StepFunctions payload, otherwise use env defaults
    db_master = athena_cfg.get("db_master", DB_MASTER)
    db_curated = athena_cfg.get("db_curated", DB_CURATED)

    # Map file -> database
    # - db create scripts can run in "default"
    # - dim_zone + master views go to master db
    # - fact_trip + curated views go to curated db
    file_db = {}
    for f in sql_files:
        if f.startswith("00_") or f.startswith("01_"):
            file_db[f] = "default"
        elif "dim_zone" in f or "master" in f:
            file_db[f] = db_master
        else:
            file_db[f] = db_curated

    results = []
    for f in sql_files:
        sql = _read_sql_from_s3(f)

        # simple templating (optional): allow ${DB_MASTER}/${DB_CURATED} inside SQL
        sql = sql.replace("${DB_MASTER}", db_master).replace("${DB_CURATED}", db_curated)

        database = file_db[f]
        qid = _start_query(sql, database=database)
        exec_resp = _wait_query(qid)

        status = exec_resp["QueryExecution"]["Status"]["State"]
        row = {
            "file": f,
            "database": database,
            "query_execution_id": qid,
            "status": status,
        }

        if status != "SUCCEEDED":
            reason = exec_resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            row["error"] = reason
            results.append(row)
            raise RuntimeError(f"Athena SQL failed: {f} | {status} | {reason}")

        results.append(row)

    return {
        "message": "Athena DDL + certified views completed",
        "executed": results,
    }
