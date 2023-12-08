"""
Get location data from staging table and save to parquet
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, DoubleType
import geocoder

# Parameters
start_id = int(sys.argv[1])
end_id = int(sys.argv[2])

# Constants
API_GOOGLE_KEY = "API_GOOGLE_KEY"

# Connect to Postgres
spark = SparkSession.builder.appName("GetLocation") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Get location data
input_path = "hdfs://namenode:8020/output/location_no_geo.parquet"
location_df = spark.read.parquet(input_path)

# Filter data
location_df = location_df \
    .filter(col("id") >= start_id) \
    .filter(col("id") < end_id)

# Get geolocation data
# Set latitude and longitude to None if geolocation data is not found

def get_geocode(location):
    """
    Get geolocation data from Google Maps API
    """
    g = geocoder.google("{}, Toronto, Ontario, Canada".format(location), key=API_GOOGLE_KEY)
    if g.ok:
        return (g.latlng[0], g.latlng[1])
    else:
        return (None, None)
    
# Define a UDF for get_geocode
get_geocode_udf = spark.udf.register("get_geocode_udf", get_geocode, StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
]))

# Add geolocation column
location_df = location_df.withColumn("geolocation", get_geocode_udf(col("location")))

location_df.show()

location_df = location_df.withColumn("latitude", col("geolocation.latitude"))
location_df = location_df.withColumn("longitude", col("geolocation.longitude"))

# Drop geolocation column
location_df = location_df.drop("geolocation")

location_df.show()

# Write to Parquet
location_df.coalesce(1).write.parquet("hdfs://namenode:8020/output/location_{}_{}.parquet".format(start_id, end_id), mode="overwrite")

# Spark Stop
spark.stop()