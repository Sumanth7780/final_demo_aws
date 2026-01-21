from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# INPUT (Delta)
ZONES_DELTA = "s3://nyc-datalake-week2-sumanth/master_data/taxi_zones_delta/"
TRIPS_DELTA = "s3://nyc-datalake-week2-sumanth/curated_data/yellow_tripdata/"

# OUTPUT (Parquet for Redshift COPY)
OUT_DIM_ZONE = "s3://nyc-datalake-week2-sumanth/redshift_export/dim_zone/"
OUT_FACT_TRIP = "s3://nyc-datalake-week2-sumanth/redshift_export/fact_trip/"

def pick(df, *names):
    s = set(df.columns)
    for n in names:
        if n in s:
            return n
    raise Exception(f"None of these columns found: {names}. Available: {df.columns}")

# ---- DIM_ZONE: EXACTLY 4 columns ----
zones = spark.read.format("delta").load(ZONES_DELTA)

c_loc = pick(zones, "LocationID", "location_id", "locationId")
c_bor = pick(zones, "Borough", "borough")
c_zone = pick(zones, "Zone", "zone")
c_srv = pick(zones, "service_zone", "ServiceZone", "serviceZone")

dim_zone_out = zones.select(
    col(c_loc).cast("int").alias("location_id"),
    col(c_bor).cast("string").alias("borough"),
    col(c_zone).cast("string").alias("zone"),
    col(c_srv).cast("string").alias("service_zone")
)

# ---- FACT_TRIP: EXACTLY 11 columns ----
trips = spark.read.format("delta").load(TRIPS_DELTA)

t_pickup = pick(trips, "pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime")
t_drop   = pick(trips, "dropoff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime")
t_pu     = pick(trips, "pickup_location_id", "PULocationID", "pu_location_id")
t_do     = pick(trips, "dropoff_location_id", "DOLocationID", "do_location_id")
t_vendor = pick(trips, "vendor_id", "VendorID")
t_pass   = pick(trips, "passenger_count")
t_dist   = pick(trips, "trip_distance")
t_fare   = pick(trips, "fare_amount")
t_tip    = pick(trips, "tip_amount")
t_total  = pick(trips, "total_amount")
t_pay    = pick(trips, "payment_type")

fact_trip_out = trips.select(
    col(t_pickup).cast("timestamp").alias("pickup_datetime"),
    col(t_drop).cast("timestamp").alias("dropoff_datetime"),
    col(t_pu).cast("int").alias("pickup_location_id"),
    col(t_do).cast("int").alias("dropoff_location_id"),
    col(t_vendor).cast("int").alias("vendor_id"),
    col(t_pass).cast("int").alias("passenger_count"),
    col(t_dist).cast("double").alias("trip_distance"),
    col(t_fare).cast("double").alias("double").alias("fare_amount"),
    col(t_tip).cast("double").alias("tip_amount"),
    col(t_total).cast("double").alias("total_amount"),
    col(t_pay).cast("int").alias("payment_type")
)

print("Writing dim_zone (4 cols) ->", OUT_DIM_ZONE)
print("Writing fact_trip (11 cols) ->", OUT_FACT_TRIP)

dim_zone_out.write.mode("overwrite").parquet(OUT_DIM_ZONE)
fact_trip_out.write.mode("overwrite").parquet(OUT_FACT_TRIP)
