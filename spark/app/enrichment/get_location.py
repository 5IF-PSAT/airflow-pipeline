"""
Get location data from staging table and save to parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id
import geocoder

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

# Read data from staging table
database_url = "jdbc:postgresql://postgres:5432/deng_staging"
properties = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver"
}

bus_delay_df = spark.read \
    .jdbc(url=database_url, table=f"public.enrichment_bus_delay", properties=properties)

# Get location data
location_df = bus_delay_df \
    .select("location_slug") \
    .distinct() \
    .orderBy("location_slug")

def unslug(text):
    """
    Convert slug to text
    """
    tmp = text.replace("-", " ")
    return tmp

# Define a UDF for unslug
unslug_udf = spark.udf.register("unslug_udf", unslug, StringType())

# Add id column
location_df = location_df.withColumn("id", monotonically_increasing_id())

# Add location column
location_df = location_df.withColumn("location", unslug_udf(col("location_slug")))

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

# Save to Parquet
output_path = "hdfs://namenode:8020/output/location_tmp.parquet"
location_df.coalesce(1).write.parquet(output_path, mode="overwrite")

location_df = location_df.withColumn("latitude", col("geolocation.latitude"))
location_df = location_df.withColumn("longitude", col("geolocation.longitude"))

# Drop geolocation column
location_df = location_df.drop("geolocation")

location_df.show()

# Write to Parquet
location_df.coalesce(1).write.parquet("hdfs://namenode:8020/output/location.parquet", mode="overwrite")

# Spark Stop
spark.stop()