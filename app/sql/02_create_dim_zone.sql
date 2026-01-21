
-- Purpose: External table over Parquet dim_zone export
-- Data Owner: MDM / Reference Data Steward
-- Dataset: dim_zone (conformed master dimension)
-- Storage: Parquet on S3 (exported from Delta master)

CREATE EXTERNAL TABLE IF NOT EXISTS ${DB_MASTER}.dim_zone (
  location_id              int,
  borough                  string,
  zone                     string,
  service_zone             string,
  record_hash              string,
  effective_start_ts       timestamp,
  effective_end_ts         timestamp,
  is_current               boolean,
  source_system            string
)
STORED AS PARQUET
LOCATION 's3://nyc-datalake-week2-sumanth/redshift_export/dim_zone/';

-- NOTE:
-- If your Parquet has different columns, update ONLY the column list above.
