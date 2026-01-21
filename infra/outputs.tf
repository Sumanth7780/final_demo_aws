output "lambda_athena_name" {
  value = aws_lambda_function.athena_submit_sql.function_name
}

output "lambda_redshift_name" {
  value = aws_lambda_function.redshift_copy_load.function_name
}

output "stepfunction_name" {
  value = aws_sfn_state_machine.final_demo.name
}

output "dynamodb_audit_table" {
  value = aws_dynamodb_table.pipeline_audit.name
}
