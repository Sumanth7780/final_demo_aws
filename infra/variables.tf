variable "aws_region" { type = string }
variable "account_id" { type = string }

# Existing S3 bucket (you already created this)
variable "bucket" { type = string }

# Where CI/CD uploads your code
variable "scripts_prefix" { type = string } # e.g. "final-demo/scripts"
variable "sql_prefix"     { type = string } # e.g. "final-demo/sql"

# Glue execution role you already have (AWSGlueServiceRole-NYCTaxi)
variable "glue_role_arn" { type = string }

# Athena
variable "athena_workgroup"    { type = string } # e.g. "nyc-governed-wg"
variable "athena_results_s3"   { type = string } # e.g. "s3://bucket/athena_results/"
variable "athena_db_master"    { type = string } # e.g. "nyc_master_db"
variable "athena_db_curated"   { type = string } # e.g. "nyc_curated_db"

# Redshift
variable "redshift_secret_name" { type = string } # SecretsManager name containing host/db/user/password
variable "redshift_iam_role_arn" { type = string } # role attached to Redshift for COPY from S3
variable "redshift_db" { type = string }           # e.g. "dev"

# Tags
variable "tags" {
  type = map(string)
  default = {
    project = "nyc-governed-lakehouse"
    env     = "dev"
    owner   = "sumanth"
  }
}
