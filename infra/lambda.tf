data "archive_file" "athena_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda/athena_submit_sql"
  output_path = "${path.module}/_build/athena_submit_sql.zip"
}

data "archive_file" "redshift_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda/redshift_copy_load"
  output_path = "${path.module}/_build/redshift_copy_load.zip"
}

resource "aws_lambda_function" "athena_submit" {
  function_name = local.lambda_athena_name
  role          = aws_iam_role.lambda_exec.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"

  filename         = data.archive_file.athena_zip.output_path
  source_code_hash = data.archive_file.athena_zip.output_base64sha256

  timeout = 300

  environment {
    variables = {
      BUCKET          = var.bucket
      SQL_PREFIX      = var.sql_prefix
      ATHENA_RESULTS  = var.athena_results_s3
      ATHENA_WG       = var.athena_workgroup
      DB_MASTER       = var.athena_db_master
      DB_CURATED      = var.athena_db_curated
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_athena_logs
  ]

  tags = var.tags
}

resource "aws_lambda_function" "redshift_copy" {
  function_name = local.lambda_redshift_name
  role          = aws_iam_role.lambda_exec.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.11"

  filename         = data.archive_file.redshift_zip.output_path
  source_code_hash = data.archive_file.redshift_zip.output_base64sha256

  timeout = 300

  environment {
    variables = {
      SECRET_NAME     = var.redshift_secret_name
      RS_DB           = var.redshift_db
      RS_IAM_ROLE_ARN = var.redshift_iam_role_arn
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda_redshift_logs
  ]

  tags = var.tags
}
