"""
Get location data from staging table and clean it
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import slugify

# Parameters
year = sys.argv[1]

# Connect to Postgres

spark = SparkSession.builder.appName("CleanLocation") \
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

# Clean data in location column

bus_delay_df = bus_delay_df \
    .withColumn("location_slug", slugify.slugify(col("location"))) \
    .withColumn("location_slug", col("location_slug").cast("string")) \
    .withColumn("location_slug", col("location_slug").replace(" ", "-")) \
    .withColumn("location_slug", col("location_slug").replace(":", "")) \
    .withColumn("location_slug", col("location_slug").replace(";", "")) \
    .withColumn("location_slug", col("location_slug").replace(",", "")) \
    .withColumn("location_slug", col("location_slug").replace(".", "")) \
    .withColumn("location_slug", col("location_slug").replace("(", "")) \
    .withColumn("location_slug", col("location_slug").replace(")", "")) \
    .withColumn("location_slug", col("location_slug").replace("?", "")) \
    .withColumn("location_slug", col("location_slug").replace("!", "")) \
    .withColumn("location_slug", col("location_slug").replace("'", "")) \
    .withColumn("location_slug", col("location_slug").replace('"', "")) \
    .withColumn("location_slug", col("location_slug").replace("/", "")) \
    .withColumn("location_slug", col("location_slug").replace("\\", "")) \
    .withColumn("location_slug", col("location_slug").replace("&", ""))

# Write data to staging table

bus_delay_df.write \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres:5432/deng_staging") \
    .option("dbtable", f"public.staging_bus_delay_{year}") \
    .option("user", "postgres") \
    .option("truncate", "true") \
    .option("batchsize", 10000) \
    .mode("overwrite") \
    .save()

# Stop Spark

spark.stop()