import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# NOTE: Requires Delta support in Glue (Spark conf + delta libs)
# In Glue 4.0, you may need to add delta jars or use a configured runtime.

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INCOMING_PATH",      # s3://.../incoming_zones_delta
    "DIM_SCD2_PATH",      # s3://.../zone_dim_scd2_delta
])

INCOMING_PATH = args["INCOMING_PATH"].rstrip("/")
DIM_SCD2_PATH = args["DIM_SCD2_PATH"].rstrip("/")

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

incoming = spark.read.format("delta").load(INCOMING_PATH) \
    .select(
        F.col("location_id").cast("int").alias("location_id"),
        F.col("borough").cast("string").alias("borough"),
        F.col("zone_name").cast("string").alias("zone_name"),
        F.col("service_zone").cast("string").alias("service_zone"),
    ) \
    .withColumn("hashdiff", F.sha2(F.concat_ws("||", "borough", "zone_name", "service_zone"), 256))

# If dim exists, do merge, else initialize
# This is a template; real merge uses delta.tables.DeltaTable
print("SCD2 Delta template loaded incoming rows:", incoming.count())
print("Implement DeltaTable.merge(...) here if you choose Delta approach.")
