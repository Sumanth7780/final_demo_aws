# ------------------------------------------------------------
# Step Functions Definition (JSON via jsonencode)
# IAM Role/Policy are defined in iam.tf:
#   - aws_iam_role.sfn_role
#   - aws_iam_policy.sfn_policy + attachment
# ------------------------------------------------------------

locals {
  sfn_definition = jsonencode({
    Comment = "FINAL DEMO: NYC Governed Lakehouse (Week2+3+4)"
    StartAt = "Day6_RawToValidated"

    States = {
      Day6_RawToValidated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          "JobName.$" = "$.jobs.day6"
          Arguments = {
            "--RAW_TRIPS_PATH.$" = "$.paths.raw_trips"
            "--RAW_ZONES_PATH.$" = "$.paths.raw_zones"
            "--DELTA_OUT_PATH.$" = "$.paths.validated_delta"
          }
        }
        Next = "Day7_ValidatedToCurated"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Day7_ValidatedToCurated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          "JobName.$" = "$.jobs.day7"
          Arguments = {
            "--VALIDATED_DELTA_PATH.$" = "$.paths.validated_delta"
            "--RAW_ZONES_CSV_PATH.$"   = "$.paths.zones_csv"
            "--CURATED_DELTA_PATH.$"   = "$.paths.curated_delta"
          }
        }
        Next = "Day8_DQGates"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Day8_DQGates = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          "JobName.$" = "$.jobs.day8"
          Arguments = {
            "--INPUT_DELTA_PATH.$"   = "$.paths.validated_delta"
            "--CURATED_DELTA_PATH.$" = "$.paths.curated_delta"
            "--QUARANTINE_PATH.$"    = "$.paths.quarantine"
            "--DQ_REPORT_PATH.$"     = "$.paths.dq_report"
          }
        }
        Next = "Day9_MDMZones"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Day9_MDMZones = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          "JobName.$" = "$.jobs.day9"
          Arguments = {
            "--RAW_ZONES_PATH.$"     = "$.paths.zones_csv"
            "--MASTER_OUT_PATH.$"    = "$.paths.master_zones_delta"
            "--STEWARD_QUEUE_PATH.$" = "$.paths.steward_queue"
            "--REJECTS_PATH.$"       = "$.paths.mdm_rejects"
            "--AUDIT_PATH.$"         = "$.paths.mdm_audit"
            "--HIGH_CONF.$"          = "$.paths.high_conf"
            "--MED_CONF.$"           = "$.paths.med_conf"
          }
        }
        Next = "Day10_LifecycleOrphans"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Day10_LifecycleOrphans = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          "JobName.$" = "$.jobs.day10"
          Arguments = {
            "--MASTER_ZONES_DELTA_PATH.$"  = "$.paths.master_zones_delta"
            "--CURATED_TRIPS_DELTA_PATH.$" = "$.paths.curated_delta"

            "--ORPHANS_OUT_PATH.$"        = "$.paths.orphans"
            "--LIFECYCLE_SNAPSHOT_PATH.$" = "$.paths.lifecycle"
            "--AUDIT_HISTORY_OUT_PATH.$"  = "$.paths.delta_history"
            "--RUN_SUMMARY_PATH.$"        = "$.paths.run_summary"

            "--ORPHANS_CSV_OUT.$"              = "$.paths.orphans_csv_out"
            "--STEWARD_ACTIVITY_LOG_CSV_OUT.$" = "$.paths.steward_activity_log_csv_out"
          }
        }
        Next = "Export_DeltaToParquet"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Export_DeltaToParquet = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          "JobName.$" = "$.jobs.export"
          Arguments = {
            "--S3_DELTA_ZONES.$"  = "$.paths.master_zones_delta"
            "--S3_DELTA_TRIPS.$"  = "$.paths.curated_delta"
            "--S3_RS_DIM_ZONE.$"  = "$.paths.rs_dim_zone"
            "--S3_RS_FACT_TRIP.$" = "$.paths.rs_fact_trip"
          }
        }
        Next = "Athena_CreateTablesViews"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 15
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Athena_CreateTablesViews = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.athena_submit_sql.arn
          "Payload.$"  = "$"
        }
        Next = "Redshift_CopyLoad"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      Redshift_CopyLoad = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.redshift_copy_load.arn
          "Payload.$"  = "$"
        }
        Next = "AuditSuccess"
        Retry = [{
          ErrorEquals     = ["States.ALL"]
          IntervalSeconds = 10
          MaxAttempts     = 2
          BackoffRate     = 2.0
        }]
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "AuditFailure"
        }]
      }

      AuditSuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.pipeline_audit.name
          Item = {
            run_id = { "S.$" = "$.run_id" }
            ts     = { "S.$" = "$$.State.EnteredTime" }
            status = { S = "SUCCESS" }
          }
        }
        Next = "Success"
      }

      AuditFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = aws_dynamodb_table.pipeline_audit.name
          Item = {
            run_id = { "S.$" = "$.run_id" }
            ts     = { "S.$" = "$$.State.EnteredTime" }
            status = { S = "FAILED" }
            error  = { "S.$" = "$.error.Cause" }
          }
        }
        Next = "FailPipeline"
      }

      Success = { Type = "Succeed" }

      FailPipeline = {
        Type  = "Fail"
        Cause = "Final demo pipeline failed"
      }
    }
  })
}

# ------------------------------------------------------------
# Step Functions State Machine
# Uses IAM role from iam.tf: aws_iam_role.sfn_role
# ------------------------------------------------------------
resource "aws_sfn_state_machine" "final_demo" {
  name     = "final-demo-governed-lakehouse"
  role_arn = aws_iam_role.sfn_role.arn

  definition = local.sfn_definition

  tags = var.tags
}
