"""
Convert CSV to Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

spark = SparkSession.builder.appName("CSV to Parquet") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read CSV

df = spark.read.csv("/opt/bitnami/spark/resources/data/XWines_Full_100K_wines.csv", header=True)

# Write Parquet

df.write.parquet("/opt/bitnami/spark/resources/data/XWines_Full_100K_wines.parquet", mode="overwrite")

# Stop Spark

spark.stop()