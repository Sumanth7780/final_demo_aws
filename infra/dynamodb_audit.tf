resource "aws_dynamodb_table" "pipeline_audit" {
  name         = "${var.project}-pipeline-audit"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "run_id"
  range_key = "ts"

  attribute {
    name = "run_id"
    type = "S"
  }

  attribute {
    name = "ts"
    type = "S"
  }

  tags = {
    Project = var.project
    Owner   = var.owner
  }
}
