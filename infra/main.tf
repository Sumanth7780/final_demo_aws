data "aws_caller_identity" "current" {}

locals {
  # Helpful canonical paths
  scripts_s3 = "s3://${var.bucket}/${var.scripts_prefix}"
  sql_s3     = "s3://${var.bucket}/${var.sql_prefix}"

  # Glue job script locations
  script_day6   = "${local.scripts_s3}/day6_datalake_delta_raw_to_validated_glue.py"
  script_day7   = "${local.scripts_s3}/day7_spark_enrich_and_catalog_prep.py"
  script_day8   = "${local.scripts_s3}/day8_glue_quality_gates_validated_to_curated.py"
  script_day9   = "${local.scripts_s3}/day9_mdm_matching_dedup_engine.py"
  script_day10  = "${local.scripts_s3}/day10_mdm_lifecycle_audit_orphans.py"
  script_export = "${local.scripts_s3}/export_delta_to_parquet.py"

  # Lambda function names
  lambda_athena_name   = "final-athena-submit-sql"
  lambda_redshift_name = "final-redshift-copy-load"

  # Step Function name
  sfn_name = "final-demo-governed-lakehouse"
}
