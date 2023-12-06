"""
Get incident data from staging table and save to parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StringType

# Connect to Postgres
spark = SparkSession.builder.appName("GetIncident") \
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

# Get incident data
incident_df = bus_delay_df \
    .select("incident_slug") \
    .distinct() \
    .orderBy("incident_slug")

def unslug(text):
    """
    Convert slug to text
    """
    tmp = text.replace("-", " ")
    return tmp

# Define a UDF for unslug
unslug_udf = spark.udf.register("unslug_udf", unslug, StringType())

# Add id column
incident_df = incident_df.withColumn("id", monotonically_increasing_id())

# Add incident column
incident_df = incident_df.withColumn("incident", unslug_udf(col("incident_slug")))

# Show Spark df
incident_df.show()

# Write to Parquet
output_path = "hdfs://namenode:8020/output/incident.parquet"
incident_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# Stop Spark
spark.stop()