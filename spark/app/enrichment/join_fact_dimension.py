from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Spark Session
spark = SparkSession.builder.appName("JoinFactTableDimension") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read data from Parquet
fact_table_df = spark.read.parquet("hdfs://namenode:8020/output/fact_table.parquet")

incident_df = spark.read.parquet("hdfs://namenode:8020/output/incident.parquet")

location_df = spark.read.parquet("hdfs://namenode:8020/output/location.parquet")

time_df = spark.read.parquet("hdfs://namenode:8020/output/time.parquet")

# Join fact table with incident table
fact_table_df = fact_table_df.join(incident_df, on=["incident_slug"], how="inner") \
    .select(fact_table_df["*"], incident_df["id"].alias("incident_id"))

# Join fact table with location table
fact_table_df = fact_table_df.join(location_df, on=["location_slug"], how="inner") \
    .select(fact_table_df["*"], location_df["id"].alias("location_id"))

# Join fact table with time table
fact_table_df = fact_table_df.join(time_df, on=["year", "month", "day", "day_of_week", "day_type", "hour"], how="inner") \
    .select(fact_table_df["*"], time_df["id"].alias("time_id"))

# Drop unnecessary columns
fact_table_df = fact_table_df.drop("year", "month", "day", "day_of_week", "day_type", "hour", 
                                   "incident_slug", "location_slug", "location", "incident")

# Show data
fact_table_df.show()

# Write fact table df to parquet
output_path = "hdfs://namenode:8020/output/final_fact_table.parquet"
fact_table_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# rating_wine_df = spark.read.parquet("hdfs://namenode:8020/output/rating_wine.parquet")

# # Cast Vintage column to integer
# rating_wine_df = rating_wine_df.withColumn("Vintage", col("Vintage").cast(IntegerType()))

# rating_wine_df.coalesce(1).write.mode("overwrite").parquet("hdfs://namenode:8020/output/rating_wine_2.parquet")

# Stop spark session
spark.stop()