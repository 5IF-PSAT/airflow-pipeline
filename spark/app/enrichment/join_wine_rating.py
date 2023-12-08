from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# Spark Session
spark = SparkSession.builder.appName("JoinWineRating") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Read data from HDFS
rating_df = spark.read.parquet("hdfs://namenode:8020/output/rating_vintage_3.parquet")

wine_df = spark.read.parquet("hdfs://namenode:8020/output/wines.parquet")

rating_df = rating_df \
    .join(wine_df, on=["WineID"], how="left") \
    .select(wine_df.WineID, rating_df.Vintage, wine_df.RegionID, wine_df.WineName,
            wine_df.Type, wine_df.Elaborate, wine_df.ABV, wine_df.Body, wine_df.Acidity,
            rating_df.CountRating, rating_df.AverageRating, rating_df.MinRating, rating_df.MaxRating)

# Show dataframe
rating_df.show()

# Write dataframe to HDFS
rating_df.write.mode("overwrite").parquet("hdfs://namenode:8020/output/rating_wine.parquet")

# Stop Spark Session
spark.stop()