import functools
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark Session
spark = SparkSession.builder.appName("MergeLocation") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

list_ids = [0, 20, 220, 1220, 2220, 3220, 4220, 5220, 6220, 7220, 8220, 9220, 10220,
            11220, 12220, 13220, 14220, 15220, 16220, 17220, 18220, 19220, 20220,
            21220, 22220, 23220, 24220, 25220, 26220, 27220, 28220, 29220, 30220,
            31220, 32220, 33220, 34220, 35220, 36220, 37220, 38220, 39220, 40220,
            41220, 42220, 43220, 44220, 45220, 46220, 47220, 48220, 49220, 50220,
            51220, 52220, 53220, 54220, 55220, 56220, 57220, 58220, 59220, 60220]

# Read location data
list_location_df = [spark.read.parquet(f"hdfs://namenode:8020/output/location_{list_ids[i]}_{list_ids[i+1]}.parquet")
                    for i in range(len(list_ids) - 1)]

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)


# Merge location data
location_df = unionAll(list_location_df)

# Show Spark df
location_df.show()

# Write to Parquet
output_path = "hdfs://namenode:8020/output/location.parquet"
location_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# Stop Spark
spark.stop()