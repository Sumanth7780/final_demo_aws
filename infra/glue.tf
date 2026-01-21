locals {
  glue_common_args = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--datalake-formats"                 = "delta"
    "--TempDir"                          = "s3://${var.bucket}/glue_temp/"
    "--spark-event-logs-path"            = "s3://${var.bucket}/glue_spark_logs/"
    "--conf"                             = "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }
}

resource "aws_glue_job" "day6" {
  name     = "final-day6-raw-to-validated"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = local.script_day6
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--RAW_TRIPS_PATH" = ""
    "--RAW_ZONES_PATH" = ""
    "--DELTA_OUT_PATH" = ""
  })

  tags = var.tags
}

resource "aws_glue_job" "day7" {
  name     = "final-day7-validated-to-curated"
  role_arn = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = local.script_day7
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--VALIDATED_DELTA_PATH" = ""
    "--RAW_ZONES_CSV_PATH"   = ""
    "--CURATED_DELTA_PATH"   = ""
  })

  tags = var.tags
}

resource "aws_glue_job" "day8" {
  name     = "final-day8-dq-gates"
  role_arn = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = local.script_day8
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--INPUT_DELTA_PATH"   = ""
    "--CURATED_DELTA_PATH" = ""
    "--QUARANTINE_PATH"    = ""
    "--DQ_REPORT_PATH"     = ""
  })

  tags = var.tags
}

resource "aws_glue_job" "day9" {
  name     = "final-day9-mdm-zones"
  role_arn = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = local.script_day9
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--RAW_ZONES_PATH"     = ""
    "--MASTER_OUT_PATH"    = ""
    "--STEWARD_QUEUE_PATH" = ""
    "--REJECTS_PATH"       = ""
    "--AUDIT_PATH"         = ""
    "--HIGH_CONF"          = "0.95"
    "--MED_CONF"           = "0.80"
  })

  tags = var.tags
}

resource "aws_glue_job" "day10" {
  name     = "final-day10-lifecycle-orphans"
  role_arn = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = local.script_day10
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--MASTER_ZONES_DELTA_PATH"  = ""
    "--CURATED_TRIPS_DELTA_PATH" = ""
    "--ORPHANS_OUT_PATH"         = ""
    "--LIFECYCLE_SNAPSHOT_PATH"  = ""
    "--AUDIT_HISTORY_OUT_PATH"   = ""
    "--RUN_SUMMARY_PATH"         = ""
  })

  tags = var.tags
}

resource "aws_glue_job" "export_parquet" {
  name     = "final-week4-export-delta-to-parquet"
  role_arn = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = local.script_export
    python_version  = "3"
  }

  default_arguments = merge(local.glue_common_args, {
    "--S3_DELTA_ZONES"  = ""
    "--S3_DELTA_TRIPS"  = ""
    "--S3_RS_DIM_ZONE"  = ""
    "--S3_RS_FACT_TRIP" = ""
  })

  tags = var.tags
}
