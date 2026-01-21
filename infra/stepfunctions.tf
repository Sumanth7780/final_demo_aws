locals {
  sfn_definition = jsonencode({
    Comment = "NYC Taxi Governed Lakehouse - Final Demo"
    StartAt = "Day6RawToValidated"
    States  = {
      Day6RawToValidated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.day6.name
          Arguments = {
            "--RAW_TRIPS_PATH.$" = "$.paths.raw_trips"
            "--RAW_ZONES_PATH.$" = "$.paths.raw_zones"
            "--DELTA_OUT_PATH.$" = "$.paths.validated_delta"
          }
        }
        Next = "Day7ValidatedToCurated"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "FailPipeline"
        }]
      }

      Day7ValidatedToCurated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.day7.name
          Arguments = {
            "--VALIDATED_DELTA_PATH.$" = "$.paths.validated_delta"
            "--RAW_ZONES_CSV_PATH.$"   = "$.paths.zones_csv"
            "--CURATED_DELTA_PATH.$"   = "$.paths.curated_delta"
          }
        }
        Next = "Day8DQGates"
      }

      Day8DQGates = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.day8.name
          Arguments = {
            "--INPUT_DELTA_PATH.$"   = "$.paths.validated_delta"
            "--CURATED_DELTA_PATH.$" = "$.paths.curated_delta"
            "--QUARANTINE_PATH.$"    = "$.paths.quarantine"
            "--DQ_REPORT_PATH.$"     = "$.paths.dq_report"
          }
        }
        Next = "Day9MDMZones"
      }

      Day9MDMZones = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.day9.name
          Arguments = {
            "--RAW_ZONES_PATH.$"     = "$.paths.zones_csv"
            "--MASTER_OUT_PATH.$"    = "$.paths.master_zones_delta"
            "--STEWARD_QUEUE_PATH.$" = "$.paths.steward_queue"
            "--REJECTS_PATH.$"       = "$.paths.mdm_rejects"
            "--AUDIT_PATH.$"         = "$.paths.mdm_audit"
            "--HIGH_CONF"            = "0.95"
            "--MED_CONF"             = "0.80"
          }
        }
        Next = "Day10LifecycleOrphans"
      }

      Day10LifecycleOrphans = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.day10.name
          Arguments = {
            "--MASTER_ZONES_DELTA_PATH.$"  = "$.paths.master_zones_delta"
            "--CURATED_TRIPS_DELTA_PATH.$" = "$.paths.curated_delta"
            "--ORPHANS_OUT_PATH.$"         = "$.paths.orphans"
            "--LIFECYCLE_SNAPSHOT_PATH.$"  = "$.paths.lifecycle"
            "--AUDIT_HISTORY_OUT_PATH.$"   = "$.paths.delta_history"
            "--RUN_SUMMARY_PATH.$"         = "$.paths.run_summary"
          }
        }
        Next = "ExportParquetForServing"
      }

      ExportParquetForServing = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.export_parquet.name
          Arguments = {
            "--S3_DELTA_ZONES.$"  = "$.paths.master_zones_delta"
            "--S3_DELTA_TRIPS.$"  = "$.paths.curated_delta"
            "--S3_RS_DIM_ZONE.$"  = "$.paths.rs_dim_zone"
            "--S3_RS_FACT_TRIP.$" = "$.paths.rs_fact_trip"
          }
        }
        Next = "RunAthenaDDLViews"
      }

      RunAthenaDDLViews = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.athena_submit.arn
          Payload.$    = "$"
        }
        Next = "LoadRedshiftDimsFacts"
      }

      LoadRedshiftDimsFacts = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.redshift_copy.arn
          Payload.$    = "$"
        }
        Next = "WriteAudit"
      }

      WriteAudit = {
        Type = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.pipeline_audit.name
          Item = {
            run_id = { S.$ = "$.run_id" }
            ts     = { S.$ = "$$.State.EnteredTime" }
            status = { S  = "SUCCESS" }
          }
        }
        Next = "Success"
      }

      Success = { Type = "Succeed" }

      FailPipeline = { Type = "Fail", Cause = "Final demo pipeline failed" }
    }
  })
}

resource "aws_sfn_state_machine" "final_demo" {
  name     = local.sfn_name
  role_arn = aws_iam_role.sfn_role.arn
  definition = local.sfn_definition
  tags = var.tags
}
