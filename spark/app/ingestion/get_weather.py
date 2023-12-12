"""
Get weather data by region and vintage
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import requests
import json
import pandas as pd

# Parameters
location_id = sys.argv[1]

spark = SparkSession.builder.appName("Get Weather Data") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read location
region_df = spark.read.parquet("hdfs://namenode:8020/output/regions.parquet")

region_df = region_df.filter(region_df["RegionID"] == int(location_id)).collect()

link = "https://archive-api.open-meteo.com/v1/archive?"
start_date = "1949-01-01"
end_date = "2022-12-31"
params = {
    "latitude": region_df[0]["Latitude"],
    "longitude": region_df[0]["Longitude"],
    "start_date": start_date,
    "end_date": end_date,
    "hourly": "relative_humidity_2m,wind_speed_10m,soil_temperature_28_to_100cm,soil_moisture_28_to_100cm",
    "daily": "temperature_2m_mean,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum",
    "timezone": "Europe/London"
}
response = requests.get(link, params=params)

data = json.loads(response.text)['daily']
        
# Add RegionID and VintageID
data["RegionID"] = [int(location_id) for i in range(len(data["temperature_2m_mean"]))]

# Rename key
data["temperature"] = data.pop("temperature_2m_mean")
data["sunshine_duration"] = data.pop("sunshine_duration")
data["precipitation"] = data.pop("precipitation_sum")
data["rain"] = data.pop("rain_sum")
data["snowfall"] = data.pop("snowfall_sum")
# Pandas DataFrame
pandas_df = pd.DataFrame(data)

# Astype
pandas_df["RegionID"] = pandas_df["RegionID"].astype(int)
pandas_df["temperature"] = pandas_df["temperature"].astype(float)
pandas_df["sunshine_duration"] = pandas_df["sunshine_duration"].astype(float)
pandas_df["precipitation"] = pandas_df["precipitation"].astype(float)
pandas_df["rain"] = pandas_df["rain"].astype(float)
pandas_df["snowfall"] = pandas_df["snowfall"].astype(float)
# Convert to Spark DataFrame
schema = StructType([
    StructField("time", StringType(), True),
    StructField("RegionID", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("sunshine_duration", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("snowfall", DoubleType(), True)
])
weather_df = spark.createDataFrame(pandas_df, schema=schema)
# Write to Parquet
weather_df.coalesce(1).write.parquet("/output/weather_daily.parquet", mode="append")

hourly_data = json.loads(response.text)['hourly']
# Add RegionID and VintageID
hourly_data["RegionID"] = [int(location_id) for i in range(len(hourly_data["relative_humidity_2m"]))]
    
# Rename key
hourly_data["humidity"] = hourly_data.pop("relative_humidity_2m")
hourly_data["wind_speed"] = hourly_data.pop("wind_speed_10m")
hourly_data["soil_temperature"] = hourly_data.pop("soil_temperature_28_to_100cm")
hourly_data["soil_moisture"] = hourly_data.pop("soil_moisture_28_to_100cm")

# Pandas DataFrame
pandas_df = pd.DataFrame(hourly_data)

# Astype
pandas_df["RegionID"] = pandas_df["RegionID"].astype(int)
pandas_df["humidity"] = pandas_df["humidity"].astype(float)
pandas_df["wind_speed"] = pandas_df["wind_speed"].astype(float)
pandas_df["soil_temperature"] = pandas_df["soil_temperature"].astype(float)
pandas_df["soil_moisture"] = pandas_df["soil_moisture"].astype(float)
# Convert to Spark DataFrame
schema = StructType([
    StructField("time", StringType(), True),
    StructField("RegionID", IntegerType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("soil_temperature", DoubleType(), True),
    StructField("soil_moisture", DoubleType(), True)
])
weather_df = spark.createDataFrame(pandas_df, schema=schema)
# Write to Parquet
weather_df.coalesce(1).write.parquet("/output/weather_hourly.parquet", mode="append")

# Stop Spark Session
spark.stop()