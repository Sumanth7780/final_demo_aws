# ------------------------------------------------------------
# Package Lambda code from repo folders
# ------------------------------------------------------------

resource "null_resource" "build_dir" {
  provisioner "local-exec" {
    command = "mkdir -p ${path.module}/.build"
  }
}

data "archive_file" "athena_submit_sql_zip" {
  depends_on  = [null_resource.build_dir]
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda/athena_submit_sql"
  output_path = "${path.module}/.build/athena_submit_sql.zip"
}

data "archive_file" "redshift_copy_load_zip" {
  depends_on  = [null_resource.build_dir]
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda/redshift_copy_load"
  output_path = "${path.module}/.build/redshift_copy_load.zip"
}

# ------------------------------------------------------------
# Lambda Functions
# Reuse existing IAM role: aws_iam_role.lambda_exec (from iam.tf)
# NOTE: Do NOT set reserved env keys like AWS_REGION.
# ------------------------------------------------------------

resource "aws_lambda_function" "athena_submit_sql" {
  function_name = "${var.project}-athena-submit-sql"

  role    = aws_iam_role.lambda_exec.arn
  runtime = "python3.11"
  handler = "lambda_function.lambda_handler"

  filename         = data.archive_file.athena_submit_sql_zip.output_path
  source_code_hash = data.archive_file.athena_submit_sql_zip.output_base64sha256

  timeout = 300

  environment {
    variables = {
      ATHENA_WORKGROUP  = var.athena_workgroup
      ATHENA_RESULTS_S3 = var.athena_results_s3
      ATHENA_DB_MASTER  = var.athena_db_master
      ATHENA_DB_CURATED = var.athena_db_curated

      SQL_S3_BUCKET = var.bucket
      SQL_S3_PREFIX = var.sql_prefix
    }
  }

  tags = var.tags
}

resource "aws_lambda_function" "redshift_copy_load" {
  function_name = "${var.project}-redshift-copy-load"

  role    = aws_iam_role.lambda_exec.arn
  runtime = "python3.11"
  handler = "lambda_function.lambda_handler"

  filename         = data.archive_file.redshift_copy_load_zip.output_path
  source_code_hash = data.archive_file.redshift_copy_load_zip.output_base64sha256

  timeout = 300

  environment {
    variables = {
      REDSHIFT_SECRET_NAME = var.redshift_secret_name
      REDSHIFT_DB          = var.redshift_db
      REDSHIFT_IAM_ROLE    = var.redshift_iam_role_arn
    }
  }

  tags = var.tags
}
