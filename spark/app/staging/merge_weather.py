from pyspark.sql import SparkSession
import sys

# Get Year
year = sys.argv[1]

spark = SparkSession.builder.appName("Merge Weather Data") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

weather_df = spark.read.csv(f"/opt/bitnami/spark/resources/daily_weather_{year}.csv", header=True, inferSchema=True)

# Write to Parquet
weather_df.write.parquet(f"/opt/bitnami/spark/resources/weather_daily_{year}.parquet")

# Stop Spark Session
spark.stop()