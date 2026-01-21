output "state_machine_arn" {
  value = aws_sfn_state_machine.final_demo.arn
}

output "lambda_athena_name" {
  value = aws_lambda_function.athena_submit.function_name
}

output "lambda_redshift_name" {
  value = aws_lambda_function.redshift_copy.function_name
}

output "glue_jobs" {
  value = {
    day6   = aws_glue_job.day6.name
    day7   = aws_glue_job.day7.name
    day8   = aws_glue_job.day8.name
    day9   = aws_glue_job.day9.name
    day10  = aws_glue_job.day10.name
    export = aws_glue_job.export_parquet.name
  }
}
