aws_region = "us-east-1"
account_id = "440376044042"
project = "nyc"
owner   = "sumanth"

bucket         = "nyc-datalake-week2-sumanth"
scripts_prefix = "final-demo/scripts"
sql_prefix     = "final-demo/sql"

glue_role_arn = "arn:aws:iam::440376044042:role/AWSGlueServiceRole-NYCTaxi"

athena_workgroup  = "nyc-governed-wg"
athena_results_s3 = "s3://nyc-datalake-week2-sumanth/athena_results/"
athena_db_master  = "nyc_master_db"
athena_db_curated = "nyc_curated_db"

redshift_secret_name  = "nyc/redshift/admin"
redshift_iam_role_arn = "arn:aws:iam::440376044042:role/RedshiftServerlessS3Role"
redshift_db           = "dev"
