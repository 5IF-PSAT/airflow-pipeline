"""
Get Rating data by WineID and VintageID
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Rating Data") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read Parquet

rating_df = spark.read.parquet("/opt/bitnami/spark/resources/data/ratings.parquet")
vintage_df = spark.read.csv("/opt/bitnami/spark/resources/data/vintage.csv", header=True)

# Join and select necessary columns

rating_df = rating_df.join(vintage_df, rating_df.VintageID == vintage_df.VintageID, "inner") \
    .select(rating_df.WineID, rating_df.VintageID, rating_df.Rating, rating_df.Date, vintage_df.Vintage)

# Write Parquet

rating_df.coalesce(1).write.mode("overwrite").parquet("/opt/bitnami/spark/resources/data/ratings.parquet")

# Stop Spark

spark.stop()
                            