-- Purpose: Certified view for self-service analytics (governed join)
-- Governance:
-- - Master zone dim from ${DB_MASTER}.dim_zone (MDM governed)
-- - Facts from ${DB_CURATED}.fact_trip (curated governed)
-- - This view should be treated as "Certified" dataset for consumers

CREATE OR REPLACE VIEW ${DB_CURATED}.vw_certified_trip_zone AS
SELECT
  f.tpep_pickup_datetime,
  f.tpep_dropoff_datetime,
  date_diff('minute', f.tpep_pickup_datetime, f.tpep_dropoff_datetime) AS trip_minutes,

  f.vendorid,
  f.passenger_count,
  f.trip_distance,
  f.payment_type,

  f.fare_amount,
  f.tip_amount,
  f.total_amount,

  f.pu_location_id,
  pu.zone        AS pu_zone,
  pu.borough     AS pu_borough,
  pu.service_zone AS pu_service_zone,

  f.do_location_id,
  dz.zone        AS do_zone,
  dz.borough     AS do_borough,
  dz.service_zone AS do_service_zone

FROM ${DB_CURATED}.fact_trip f
LEFT JOIN ${DB_MASTER}.dim_zone pu
  ON f.pu_location_id = pu.location_id AND pu.is_current = true
LEFT JOIN ${DB_MASTER}.dim_zone dz
  ON f.do_location_id = dz.location_id AND dz.is_current = true;

-- Optional: quick consumer-friendly sample query (for README, not executed)
-- SELECT pu_borough, COUNT(*) trips, AVG(total_amount) avg_total
-- FROM ${DB_CURATED}.vw_certified_trip_zone
-- GROUP BY 1 ORDER BY trips DESC LIMIT 20;
