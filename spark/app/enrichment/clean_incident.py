from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Connect to Postgres

spark = SparkSession.builder.appName(f"CleanIncident").getOrCreate()

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

def slug(text):
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
slugify_udf = spark.udf.register("slugify_udf", slug, StringType())

# Add incident slug column
bus_delay_df = bus_delay_df.withColumn("incident_slug", slugify_udf(col("incident")))

# Write to Postgres
bus_delay_df.write \
    .jdbc(url=database_url, table=f"public.enrichment_bus_delay", properties=properties, mode="overwrite")

# Stop Spark
spark.stop()