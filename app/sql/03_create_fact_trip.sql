-- Purpose: External table over Parquet fact_trip export
-- Data Owner: Analytics / Data Platform
-- Dataset: fact_trip (trip-level fact)
-- Storage: Parquet on S3 (exported from curated Delta)

CREATE EXTERNAL TABLE IF NOT EXISTS ${DB_CURATED}.fact_trip (
  vendorid                    int,
  tpep_pickup_datetime        timestamp,
  tpep_dropoff_datetime       timestamp,
  passenger_count             int,
  trip_distance               double,
  pu_location_id              int,
  do_location_id              int,
  payment_type                int,
  fare_amount                 double,
  extra                       double,
  mta_tax                     double,
  tip_amount                  double,
  tolls_amount                double,
  improvement_surcharge       double,
  total_amount                double,
  congestion_surcharge        double
)
STORED AS PARQUET
LOCATION 's3://nyc-datalake-week2-sumanth/redshift_export/fact_trip/';

-- NOTE:
-- If your exported Parquet uses different naming (e.g., PULocationID),
-- update the column names/types here to match.
