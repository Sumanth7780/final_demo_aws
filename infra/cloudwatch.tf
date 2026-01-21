resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/vendedlogs/states/${local.sfn_name}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "lambda_athena_logs" {
  name              = "/aws/lambda/${local.lambda_athena_name}"
  retention_in_days = 14
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "lambda_redshift_logs" {
  name              = "/aws/lambda/${local.lambda_redshift_name}"
  retention_in_days = 14
  tags              = var.tags
}
