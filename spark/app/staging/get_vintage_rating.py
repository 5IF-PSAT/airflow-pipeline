from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("Get Rating Vintages") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read Parquet

wine_vintage_df = spark.read.parquet("/opt/bitnami/spark/resources/data/join_wine_agg_rating.parquet")

# Change type of Vintage column
wine_vintage_df = wine_vintage_df.withColumn("Vintage", wine_vintage_df["Vintage"].cast(IntegerType()))

# Show data
wine_vintage_df.show()

# Write Parquet
wine_vintage_df.coalesce(1).write.mode("overwrite").parquet("/opt/bitnami/spark/resources/data/join_wine_agg_rating_2.parquet")

# Stop Spark
spark.stop()