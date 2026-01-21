# Step Functions – Final Demo Orchestration

## What this pipeline runs
1. Day6 Glue: Raw → Validated (Delta)
2. Day7 Glue: Enrichment → Curated (Delta)
3. Day8 Glue: Quality Gates → Curated + Quarantine + DQ report
4. Day9 Glue: MDM dedup/match → Master Delta + steward queue
5. Day10 Glue: Orphans + lifecycle + Delta history audit
6. Export Glue: Delta → Parquet (for Athena + Redshift)
7. Athena Lambda: Create DBs + external tables + certified view
8. Redshift Lambda: COPY load dim/fact from S3 Parquet

## Run from CMD
- Update definition:
  stepfunctions\cmd\deploy_state_machine.cmd
- Start run:
  stepfunctions\cmd\start_execution.cmd
- View latest execution:
  stepfunctions\cmd\describe_latest_execution.cmd

## Payloads
- payloads/run_input.dev.json
- payloads/run_input.prod.json
