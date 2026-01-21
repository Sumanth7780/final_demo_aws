resource "aws_dynamodb_table" "pipeline_audit" {
  name         = "nyc_pipeline_audit"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  range_key    = "ts"

  attribute { name = "run_id" type = "S" }
  attribute { name = "ts"     type = "S" }

  tags = var.tags
}
