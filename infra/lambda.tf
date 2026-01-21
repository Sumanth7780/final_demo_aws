# ------------------------------------------------------------
# Package Lambda code from repo folders
# ------------------------------------------------------------
data "archive_file" "athena_submit_sql_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda/athena_submit_sql"
  output_path = "${path.module}/.build/athena_submit_sql.zip"
}

data "archive_file" "redshift_copy_load_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda/redshift_copy_load"
  output_path = "${path.module}/.build/redshift_copy_load.zip"
}

# Ensure build dir exists (Terraform creates files, but folder must exist in some environments)
resource "null_resource" "build_dir" {
  provisioner "local-exec" {
    command = "mkdir -p ${path.module}/.build"
  }
}

# ------------------------------------------------------------
# Lambda Execution Role
# ------------------------------------------------------------
resource "aws_iam_role" "lambda_exec" {
  name = "${var.project}-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "lambda_exec_policy" {
  name = "${var.project}-lambda-exec-policy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # CloudWatch logs
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      },

      # Athena (submit + poll)
      {
        Effect = "Allow",
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Resource = "*"
      },

      # Glue Data Catalog (common for Athena external tables)
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:UpdateTable"
        ],
        Resource = "*"
      },

      # S3 (Athena results + reading DDL from S3)
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::${var.bucket}",
          "arn:aws:s3:::${var.bucket}/*"
        ]
      },

      # Secrets Manager (Redshift credentials)
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Resource = "*"
      },

      # Redshift Data API OR direct connectivity (kept broad for demo)
      {
        Effect = "Allow",
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:GetStatementResult"
        ],
        Resource = "*"
      }
    ]
  })
}

# ------------------------------------------------------------
# CloudWatch Log Groups (optional retention)
# ------------------------------------------------------------
resource "aws_cloudwatch_log_group" "lambda_athena_logs" {
  name              = "/aws/lambda/${var.project}-athena-submit-sql"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "lambda_redshift_logs" {
  name              = "/aws/lambda/${var.project}-redshift-copy-load"
  retention_in_days = 14
  tags              = var.tags
}

# ------------------------------------------------------------
# Lambda Functions (names match Step Functions references)
# ------------------------------------------------------------
resource "aws_lambda_function" "athena_submit_sql" {
  depends_on = [null_resource.build_dir]

  function_name = "${var.project}-athena-submit-sql"
  role          = aws_iam_role.lambda_exec.arn
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"

  filename         = data.archive_file.athena_submit_sql_zip.output_path
  source_code_hash = data.archive_file.athena_submit_sql_zip.output_base64sha256

  timeout = 300

  environment {
    variables = {
      AWS_REGION         = var.aws_region
      ATHENA_WORKGROUP   = var.athena_workgroup
      ATHENA_RESULTS_S3  = var.athena_results_s3
      ATHENA_DB_MASTER   = var.athena_db_master
      ATHENA_DB_CURATED  = var.athena_db_curated
      SQL_S3_BUCKET      = var.bucket
      SQL_S3_PREFIX      = var.sql_prefix
    }
  }

  tags = var.tags
}

resource "aws_lambda_function" "redshift_copy_load" {
  depends_on = [null_resource.build_dir]

  function_name = "${var.project}-redshift-copy-load"
  role          = aws_iam_role.lambda_exec.arn
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"

  filename         = data.archive_file.redshift_copy_load_zip.output_path
  source_code_hash = data.archive_file.redshift_copy_load_zip.output_base64sha256

  timeout = 300

  environment {
    variables = {
      AWS_REGION           = var.aws_region
      REDSHIFT_SECRET_NAME = var.redshift_secret_name
      REDSHIFT_DB          = var.redshift_db
      REDSHIFT_IAM_ROLE    = var.redshift_iam_role_arn
    }
  }

  tags = var.tags
}
