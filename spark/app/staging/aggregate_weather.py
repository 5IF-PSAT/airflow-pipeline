from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import avg, min, max

spark = SparkSession.builder.appName("Aggregate Weather Data") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read weather data

weather_daily_df = spark.read.parquet("/opt/bitnami/spark/resources/data/weather_daily.parquet")

# Aggregate weather data

weather_daily_df = weather_daily_df.groupBy("RegionID", "VintageID") \
    .agg(avg('temperature').alias('avg_temperature'), min('temperature').alias('min_temperature'), max('temperature').alias('max_temperature'), \
        avg('precipitation').alias('avg_precipitation'), avg('rain').alias('avg_rain'), avg('snowfall').alias('avg_snowfall')) \
    .orderBy("RegionID", "VintageID")

# Read hourly weather data

weather_hourly_df = spark.read.parquet("/opt/bitnami/spark/resources/data/weather_hourly.parquet")

# Aggregate hourly weather data

weather_hourly_df = weather_hourly_df.groupBy("RegionID", "VintageID") \
    .agg(avg('humidity').alias('avg_humidity'), avg('wind_speed').alias('avg_wind_speed'), \
        avg('soil_temperature').alias('avg_soil_temperature'), min('soil_temperature').alias('min_soil_temperature'), max('soil_temperature').alias('max_soil_temperature'), \
        avg('soil_moisture').alias('avg_soil_moisture')) \
    .orderBy("RegionID", "VintageID")

# Join weather_daily_df and weather_hourly_df

weather_df = weather_daily_df.join(weather_hourly_df, on=["RegionID", "VintageID"], how="fullouter") \
    .select(weather_daily_df.RegionID, weather_daily_df.VintageID, weather_daily_df.avg_temperature, weather_daily_df.min_temperature, weather_daily_df.max_temperature, \
        weather_daily_df.avg_precipitation, weather_daily_df.avg_rain, weather_daily_df.avg_snowfall, \
        weather_hourly_df.avg_humidity, weather_hourly_df.avg_wind_speed, \
        weather_hourly_df.avg_soil_temperature, weather_hourly_df.min_soil_temperature, weather_hourly_df.max_soil_temperature, \
        weather_hourly_df.avg_soil_moisture)

# Write to Parquet
weather_df.coalesce(1).write.parquet("/opt/bitnami/spark/resources/data/weather_data.parquet", mode="overwrite")

# Stop Spark Session
spark.stop()