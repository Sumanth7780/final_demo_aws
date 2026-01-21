@echo off
setlocal EnableDelayedExpansion

REM =========================================================
REM FINAL DEMO: deploy + run end-to-end
REM =========================================================
call env.cmd

echo.
echo ===============================
echo 1) Upload Glue scripts to S3
echo ===============================
aws s3 sync app\glue s3://%BUCKET%/%S3_SCRIPTS_PREFIX% --delete --region %AWS_REGION%
if errorlevel 1 exit /b 1

echo.
echo ===============================
echo 2) Upload Athena SQL to S3
echo ===============================
aws s3 sync app\sql s3://%BUCKET%/%S3_SQL_PREFIX% --delete --region %AWS_REGION%
if errorlevel 1 exit /b 1

echo.
echo ===============================
echo 3) Terraform apply (infra/)
echo ===============================
pushd infra
terraform init
if errorlevel 1 exit /b 1

terraform apply -auto-approve ^
  -var="aws_region=%AWS_REGION%" ^
  -var="account_id=%ACCOUNT_ID%" ^
  -var="bucket=%BUCKET%" ^
  -var="scripts_prefix=%S3_SCRIPTS_PREFIX%" ^
  -var="sql_prefix=%S3_SQL_PREFIX%" ^
  -var="glue_role_arn=%GLUE_ROLE_ARN%" ^
  -var="athena_workgroup=%ATHENA_WG%" ^
  -var="athena_results_s3=%ATHENA_RESULTS%" ^
  -var="athena_db_master=%ATHENA_DB_MASTER%" ^
  -var="athena_db_curated=%ATHENA_DB_CURATED%" ^
  -var="redshift_secret_name=%RS_SECRET_NAME%" ^
  -var="redshift_iam_role_arn=%RS_IAM_ROLE_ARN%" ^
  -var="redshift_db=%RS_DB%"
if errorlevel 1 exit /b 1
popd

echo.
echo ===============================
echo 4) Find Step Function ARN
echo ===============================
for /f "delims=" %%A in ('aws stepfunctions list-state-machines --region %AWS_REGION% --query "stateMachines[?name==''%SFN_NAME%''].stateMachineArn | [0]" --output text') do set SFN_ARN=%%A

if "%SFN_ARN%"=="None" (
  echo ERROR: Could not find Step Function named %SFN_NAME%
  exit /b 1
)

echo SFN_ARN=%SFN_ARN%

echo.
echo ===============================
echo 5) Build run payload (temp JSON)
echo ===============================
set PAYLOAD_FILE=%TEMP%\final_demo_run_input.json

(
echo {
echo   "run_id": "manual-demo-%DATE%-%TIME%",
echo   "flags": { "skip_dq_gate": false, "skip_steward_check": true },
echo   "jobs": {
echo     "day6": "final-day6-raw-to-validated",
echo     "day7": "final-day7-validated-to-curated",
echo     "day8": "final-day8-dq-gates",
echo     "day9": "final-day9-mdm-zones",
echo     "day10": "final-day10-lifecycle-orphans",
echo     "export": "final-week4-export-delta-to-parquet"
echo   },
echo   "lambdas": {
echo     "athena_sql": "final-athena-submit-sql",
echo     "redshift_load": "final-redshift-copy-load"
echo   },
echo   "paths": {
echo     "raw_trips": "%RAW_TRIPS%",
echo     "raw_zones": "%RAW_ZONES%",
echo     "zones_csv": "%ZONES_CSV%",
echo     "validated_delta": "%VALIDATED_DELTA%",
echo     "curated_delta": "%CURATED_DELTA%",
echo     "quarantine": "%QUARANTINE%",
echo     "dq_report": "%DQ_REPORT%",
echo     "master_zones_delta": "%MASTER_ZONES_DELTA%",
echo     "steward_queue": "%STEWARD_QUEUE%",
echo     "mdm_rejects": "%MDM_REJECTS%",
echo     "mdm_audit": "%MDM_AUDIT%",
echo     "orphans": "%ORPHANS%",
echo     "lifecycle": "%LIFECYCLE%",
echo     "delta_history": "%DELTA_HISTORY%",
echo     "run_summary": "%RUN_SUMMARY%",
echo     "rs_dim_zone": "%RS_DIM_ZONE%",
echo     "rs_fact_trip": "%RS_FACT_TRIP%",
echo     "high_conf": "%HIGH_CONF%",
echo     "med_conf": "%MED_CONF%"
echo   },
echo   "athena": {
echo     "workgroup": "%ATHENA_WG%",
echo     "results_s3": "%ATHENA_RESULTS%",
echo     "db_master": "%ATHENA_DB_MASTER%",
echo     "db_curated": "%ATHENA_DB_CURATED%",
echo     "sql_files": [
echo       "00_create_master_db.sql",
echo       "01_create_curated_db.sql",
echo       "02_create_dim_zone.sql",
echo       "03_create_fact_trip.sql",
echo       "10_create_certified_view.sql"
echo     ]
echo   },
echo   "redshift": {
echo     "secret_name": "%RS_SECRET_NAME%",
echo     "iam_role_arn": "%RS_IAM_ROLE_ARN%",
echo     "db": "%RS_DB%"
echo   }
echo }
) > "%PAYLOAD_FILE%"

echo Payload written: %PAYLOAD_FILE%

echo.
echo ===============================
echo 6) Start Step Function execution
echo ===============================
for /f "delims=" %%E in ('aws stepfunctions start-execution --state-machine-arn "%SFN_ARN%" --input file://"%PAYLOAD_FILE%" --region %AWS_REGION% --query "executionArn" --output text') do set EXEC_ARN=%%E

echo.
echo ✅ Execution started:
echo %EXEC_ARN%

echo.
echo To watch execution:
echo aws stepfunctions describe-execution --execution-arn "%EXEC_ARN%" --region %AWS_REGION%

endlocal
