@echo off
REM =========================================================
REM FINAL DEMO ENV (Windows CMD)
REM =========================================================

set AWS_REGION=us-east-1
set ACCOUNT_ID=440376044042

set BUCKET=nyc-datalake-week2-sumanth

REM =========================================================
REM S3 zones (Delta)
REM =========================================================
set RAW_TRIPS=s3://%BUCKET%/raw_data/yellow_tripdata/
set RAW_ZONES=s3://%BUCKET%/raw_data/reference/

REM Exact CSV object path (needed by Day7/Day9)
set ZONES_CSV=s3://%BUCKET%/raw_data/reference/taxi_zone_lookup.csv

set VALIDATED_DELTA=s3://%BUCKET%/validated_data/yellow_tripdata_delta/
set CURATED_DELTA=s3://%BUCKET%/curated_data/yellow_tripdata/
set MASTER_ZONES_DELTA=s3://%BUCKET%/master_data/taxi_zones_delta/

REM =========================================================
REM DQ outputs (Day8)
REM =========================================================
set QUARANTINE=s3://%BUCKET%/validated_data/quarantine/
set DQ_REPORT=s3://%BUCKET%/validated_data/dq_reports/

REM =========================================================
REM MDM outputs (Day9)
REM =========================================================
set STEWARD_QUEUE=s3://%BUCKET%/master_data/steward_queue/
set MDM_REJECTS=s3://%BUCKET%/master_data/rejects/
set MDM_AUDIT=s3://%BUCKET%/master_data/mdm_audit/

REM MDM thresholds (used by Day9 in Step Functions)
set HIGH_CONF=0.95
set MED_CONF=0.80

REM =========================================================
REM Day10 outputs (Lifecycle + Orphans + Governance dashboards)
REM =========================================================
set ORPHANS=s3://%BUCKET%/master_data/orphans/
set LIFECYCLE=s3://%BUCKET%/master_data/lifecycle_snapshot/
set DELTA_HISTORY=s3://%BUCKET%/master_data/delta_history_audit/
set RUN_SUMMARY=s3://%BUCKET%/master_data/run_summary/

REM Day10 additional CSV outputs REQUIRED by your uploaded script :contentReference[oaicite:1]{index=1}
set ORPHANS_CSV_OUT=s3://%BUCKET%/master_data/dashboard/orphans_csv/
set STEWARD_ACTIVITY_LOG_CSV_OUT=s3://%BUCKET%/master_data/dashboard/steward_activity_log_csv/

REM =========================================================
REM Week4 export Parquet (Delta -> Parquet)
REM Export schema comes from export_delta_to_parquet.py :contentReference[oaicite:2]{index=2}
REM =========================================================
set RS_EXPORT_BASE=s3://%BUCKET%/redshift_export
set RS_DIM_ZONE=%RS_EXPORT_BASE%/dim_zone/
set RS_FACT_TRIP=%RS_EXPORT_BASE%/fact_trip/

REM =========================================================
REM Glue + Spark logs
REM =========================================================
set GLUE_TEMP=s3://%BUCKET%/glue_temp/
set SPARK_LOGS=s3://%BUCKET%/glue_spark_logs/

REM =========================================================
REM Athena
REM =========================================================
set ATHENA_WORKGROUP=nyc-governed-wg
set ATHENA_RESULTS=s3://%BUCKET%/athena_results/
set ATHENA_DB_CURATED=nyc_curated_db
set ATHENA_DB_MASTER=nyc_master_db

REM =========================================================
REM Redshift Serverless (use Secrets Manager for creds)
REM =========================================================
set RS_HOST=nyc-analytics-wg.440376044042.us-east-1.redshift-serverless.amazonaws.com
set RS_PORT=5439
set RS_DB=dev
set RS_SECRET_NAME=nyc/redshift/admin
set RS_IAM_ROLE_ARN=arn:aws:iam::%ACCOUNT_ID%:role/RedshiftServerlessS3Role

REM =========================================================
REM Final demo code upload prefixes (CI/CD + local)
REM =========================================================
set S3_DEMO_PREFIX=final-demo
set S3_SCRIPTS_PREFIX=%S3_DEMO_PREFIX%/scripts
set S3_SQL_PREFIX=%S3_DEMO_PREFIX%/sql

REM =========================================================
REM Glue execution role
REM =========================================================
set GLUE_ROLE_ARN=arn:aws:iam::%ACCOUNT_ID%:role/AWSGlueServiceRole-NYCTaxi

REM =========================================================
REM Step Functions name
REM =========================================================
set SFN_NAME=final-demo-governed-lakehouse
