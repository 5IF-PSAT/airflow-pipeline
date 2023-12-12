from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, min, max

spark = SparkSession.builder.appName("Get Rating Vintages") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read CSV

wine_vintage_df = spark.read.csv("/opt/bitnami/spark/resources/data/wine_vintage.csv", header=True)

ratings_df = spark.read.parquet("/opt/bitnami/spark/resources/data/ratings.parquet")

# Join wine_vintage with the ratings dataframe

rating_vintage_df = ratings_df.join(wine_vintage_df, on=["WineID", "VintageID"], how="fullouter") \
    .select(ratings_df.WineID, ratings_df.Rating, wine_vintage_df.VintageID)

# Aggregate the ratings by wine and vintage to get the average rating, min rating, max rating, and count of ratings

rating_vintage_df = rating_vintage_df.groupBy("WineID", "VintageID") \
    .agg(count("Rating").alias("CountRating"), avg("Rating").alias("AverageRating"), min("Rating").alias("MinRating"), max("Rating").alias("MaxRating")) \
    .orderBy("WineID", "VintageID")

# Show the rating_vintage dataframe

rating_vintage_df.show()

# Save the rating_vintage to Parquet

rating_vintage_df.coalesce(1).write.mode("overwrite").parquet("/opt/bitnami/spark/resources/data/rating_vintage.parquet")

# Stop Spark

spark.stop()