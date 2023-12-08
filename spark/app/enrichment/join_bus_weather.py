from pyspark.sql import SparkSession

# Spark Session
spark = SparkSession.builder.appName("JoinBusWeather") \
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

weather_df = spark.read \
    .jdbc(url=database_url, table=f"public.staging_weather", properties=properties)

# Read bus delay data
bus_delay_df = spark.read.parquet("hdfs://namenode:8020/output/enrichment_bus_delay.parquet")

# Join bus delay and weather data
fact_table_df = bus_delay_df.join(weather_df, on=["year", "month", "day", "day_of_week", "day_type", "hour"], how="inner")

# Show data
fact_table_df.show()

# Write fact table df to parquet
fact_table_df.coalesce(1).write.mode("overwrite").parquet("hdfs://namenode:8020/output/fact_table.parquet")

# Stop spark session
spark.stop()