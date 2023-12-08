import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Spark Session
spark = SparkSession.builder.appName("CreateEnrichmentBusDelay") \
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
    .jdbc(url=database_url, table=f"public.staging_bus_delay", properties=properties)

bus_delay_df.show()

def location_slug(text):
    """
    Convert text to slug
    """
    tmp = text.lower()
    tmp = tmp.replace(" ", "-")
    tmp = tmp.replace(":", "")
    tmp = tmp.replace(";", "")
    tmp = tmp.replace(",", "")
    tmp = tmp.replace(".", "")
    tmp = tmp.replace("!", "")
    tmp = tmp.replace("'", "")
    tmp = tmp.replace('"', "")
    tmp = tmp.replace("/", "")
    tmp = tmp.replace("&", "")
    return tmp

# Define a UDF for slugify
location_slug_udf = spark.udf.register("location_slug_udf", location_slug, StringType())

# Add location slug column
bus_delay_df = bus_delay_df.withColumn("location_slug", location_slug_udf(col("location")))

def incident_slug(text):
    """
    Convert text to slug
    """
    tmp = text.replace(" ", "-")
    tmp = tmp.replace(":", "")
    tmp = tmp.replace(";", "")
    tmp = tmp.replace(",", "")
    tmp = tmp.replace(".", "")
    tmp = tmp.replace("!", "")
    tmp = tmp.replace("'", "")
    tmp = tmp.replace('"', "")
    tmp = tmp.replace("/", "")
    tmp = tmp.replace("&", "")
    return tmp

# Define a UDF for slugify
incident_slug_udf = spark.udf.register("incident_slug_udf", incident_slug, StringType())

# Add incident slug column
bus_delay_df = bus_delay_df.withColumn("incident_slug", incident_slug_udf(col("incident")))

# Write to Parquet
output_path = f"hdfs://namenode:8020/output/enrichment_bus_delay.parquet"
bus_delay_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# Stop Spark Session
spark.stop()