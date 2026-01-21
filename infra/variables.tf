variable "aws_region" {
  type        = string
  description = "AWS region"
}

variable "account_id" {
  type        = string
  description = "AWS account id"
}

variable "bucket" {
  type        = string
  description = "S3 bucket for lakehouse"
}

variable "scripts_prefix" {
  type        = string
  description = "S3 prefix where Glue scripts are uploaded"
}

variable "sql_prefix" {
  type        = string
  description = "S3 prefix where Athena SQL files are uploaded"
}

variable "glue_role_arn" {
  type        = string
  description = "Glue execution role ARN"
}

variable "athena_workgroup" {
  type        = string
  description = "Athena workgroup name"
}

variable "athena_results_s3" {
  type        = string
  description = "S3 path for Athena query results"
}

variable "athena_db_master" {
  type        = string
  description = "Athena DB for master datasets"
}

variable "athena_db_curated" {
  type        = string
  description = "Athena DB for curated datasets"
}

variable "redshift_secret_name" {
  type        = string
  description = "Secrets Manager secret name for Redshift creds"
}

variable "redshift_iam_role_arn" {
  type        = string
  description = "IAM role ARN Redshift uses for COPY from S3"
}

variable "redshift_db" {
  type        = string
  description = "Redshift database name"
}

# ---- These two fix your current validate error ----
variable "project" {
  type        = string
  description = "Short project prefix for resource names"
  default     = "nyc"
}

variable "owner" {
  type        = string
  description = "Owner tag"
  default     = "sumanth"
}
variable "tags" {
  type        = map(string)
  description = "Common tags applied to all resources"
  default     = {
    Project = "nyc"
    Owner   = "sumanth"
    Env     = "dev"
  }
}
