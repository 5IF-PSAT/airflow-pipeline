"""
Save location no geocoding to Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id

# Connect to Postgres
spark = SparkSession.builder.appName("SaveLocationNoGeo") \
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

# Show data
location_df.show()

# Save to Parquet
location_df.coalesce(1).write.mode("overwrite").parquet("hdfs://namenode:8020/output/location_no_geo.parquet")

# Stop Spark Session
spark.stop()