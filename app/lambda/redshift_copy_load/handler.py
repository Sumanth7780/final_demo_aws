import os
import time
import json
import boto3
from botocore.exceptions import ClientError

secrets = boto3.client("secretsmanager")
redshift_data = boto3.client("redshift-data")

SECRET_NAME = os.environ["SECRET_NAME"]
RS_DB_ENV = os.environ.get("RS_DB", "dev")
RS_IAM_ROLE_ARN = os.environ["RS_IAM_ROLE_ARN"]


def _get_secret():
    resp = secrets.get_secret_value(SecretId=SECRET_NAME)
    s = resp.get("SecretString")
    if not s:
        raise RuntimeError("SecretString empty for Redshift secret")
    return json.loads(s)


def _wait_statement(statement_id: str, timeout_sec: int = 600):
    start = time.time()
    while True:
        desc = redshift_data.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status in ("FINISHED", "FAILED", "ABORTED"):
            return desc
        if time.time() - start > timeout_sec:
            raise TimeoutError(f"Redshift statement timed out after {timeout_sec}s. id={statement_id}")
        time.sleep(2)


def _exec_sql(workgroup_name: str, database: str, sql: str):
    resp = redshift_data.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=sql,
    )
    stmt_id = resp["Id"]
    desc = _wait_statement(stmt_id)
    if desc["Status"] != "FINISHED":
        raise RuntimeError(f"Redshift SQL failed: {desc.get('Error', 'Unknown')}")
    return stmt_id


def lambda_handler(event, context):
    """
    Expects Step Functions input includes:
      event["paths"]["rs_dim_zone"]  (s3://.../dim_zone/)
      event["paths"]["rs_fact_trip"] (s3://.../fact_trip/)
      event["redshift"]["iam_role_arn"] optional override
      event["redshift"]["db"] optional override
    """
    secret = _get_secret()

    host = secret.get("host")
    dbname = secret.get("dbname", RS_DB_ENV)
    user = secret.get("username")
    password = secret.get("password")
    # For Serverless Data API you need WorkgroupName, not host/port.
    # We'll derive workgroup name from host: "<workgroup>.<account>.<region>.redshift-serverless.amazonaws.com"
    if not host or ".redshift-serverless.amazonaws.com" not in host:
        raise ValueError("Secret host must be Redshift Serverless endpoint host")

    workgroup_name = host.split(".")[0]

    rs_cfg = event.get("redshift", {}) or {}
    database = rs_cfg.get("db", dbname)
    iam_role = rs_cfg.get("iam_role_arn", RS_IAM_ROLE_ARN)

    paths = event.get("paths", {}) or {}
    dim_zone_s3 = paths.get("rs_dim_zone")
    fact_trip_s3 = paths.get("rs_fact_trip")

    if not dim_zone_s3 or not fact_trip_s3:
        raise ValueError("Missing paths.rs_dim_zone or paths.rs_fact_trip in Step Functions payload")

    # You can adjust schema/table names to match your Redshift DDL
    # Here we assume:
    #   public.dim_zone
    #   public.fact_trip
    # And Parquet exports already include correct column names/types
    sql_statements = [
        "CREATE SCHEMA IF NOT EXISTS public;",
        # Optional truncate for demo reruns
        "TRUNCATE TABLE IF EXISTS public.dim_zone;",
        "TRUNCATE TABLE IF EXISTS public.fact_trip;",

        f"""
        COPY public.dim_zone
        FROM '{dim_zone_s3}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
        """,

        f"""
        COPY public.fact_trip
        FROM '{fact_trip_s3}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
        """
    ]

    executed = []
    for sql in sql_statements:
        sql_clean = " ".join(sql.split())
        stmt_id = _exec_sql(workgroup_name, database, sql)
        executed.append({"statement_id": stmt_id, "sql": sql_clean[:250]})

    return {
        "message": "Redshift COPY load completed",
        "workgroup": workgroup_name,
        "database": database,
        "executed": executed
    }
