"""
Get geocoding of region
"""

import sys
from pyspark.sql import SparkSession
import geocoder
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

API_GOOGLE_KEY = 'API_GOOGLE_KEY'

spark = SparkSession.builder.appName("Get Region Geocoding") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Parameters
# code_country = sys.argv[1]

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

region_df = spark.read.csv("/opt/bitnami/spark/resources/data/region_dim.csv", header=True)

# Get region by code_country

# region_df = regions_df.filter(regions_df.Code == code_country)

# Get geocoding of region
# Set Latitude and Longitude to None if geocoding is not found
region_df = region_df.toPandas()

region_df["LatLng"] = region_df.apply(lambda row: geocoder.google(row["RegionName"] + ", " + row["Country"], key=API_GOOGLE_KEY).latlng, axis=1)
region_df["Latitude"] = region_df.apply(lambda row: row["LatLng"] if row["LatLng"] is None else row["LatLng"][0], axis=1)
region_df["Longitude"] = region_df.apply(lambda row: row["LatLng"] if row["LatLng"] is None else row["LatLng"][1], axis=1)

# Drop column LatLng

region_df = region_df.drop(columns=["LatLng"])
    
# Convert to Spark DataFrame

schema = StructType([
    StructField("RegionID", StringType(), True),
    StructField("RegionName", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
])

region_df = spark.createDataFrame(region_df, schema=schema)

# Write to Parquet

region_df.coalesce(1).write.parquet("/opt/bitnami/spark/resources/data/region_dim.parquet", mode="overwrite")

# Stop Spark Session
spark.stop()
