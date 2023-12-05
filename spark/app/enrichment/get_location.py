"""
Get location data from staging table and save to parquet
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id
import geocoder

# Parameters
year = sys.argv[1]

# Constants
API_GOOGLE_KEY = "API_GOOGLE_KEY"

# Connect to Postgres
spark = SparkSession.builder.appName("GetLocation") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.20.jar") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Read data from staging table
database_url = "jdbc:postgresql://postgres:5432/deng_staging"
properties = {
    "user": "postgres",
    "driver": "org.postgresql.Driver"
}

bus_delay_df = spark.read \
    .jdbc(url=database_url, table=f"public.staging_bus_delay_{year}", properties=properties)

# Get location data
location_df = bus_delay_df \
    .select("location", "location_slug") \
    .distinct() \
    .orderBy("location_slug")

# Unslugify location data
location_df = location_df \
    .withColumn("location", col("location_slug").replace("-", " "))

# Add id column
location_df = location_df.withColumn("id", monotonically_increasing_id())

# Get geolocation data
# Set latitude and longitude to None if geolocation data is not found
location_df = location_df.toPandas()

location_df["latlng"] = location_df.apply(lambda row: geocoder.google(f"{row["location"]}, Toronto, Ontario, Canada", 
                                                                      key=API_GOOGLE_KEY).latlng, axis=1)
location_df["latitude"] = location_df.apply(lambda row: row["latlng"][0] if row["latlng"] else None, axis=1)
location_df["longitude"] = location_df.apply(lambda row: row["latlng"][1] if row["latlng"] else None, axis=1)

# Drop latlng column
location_df = location_df.drop(columns=["latlng"])

# Schema for location data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("location_slug", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

# Convert back to Spark DataFrame
location_df = spark.createDataFrame(location_df, schema=schema)

# Write data to parquet
location_df.coalesce(1).write.parquet(f"/opt/bitnami/spark/resources/data/location_{year}.parquet", mode="overwrite", schema=schema)

# Stop Spark
spark.stop()