"""
Get list of days from 1st Jan 2017 to 31st Dec 2022
"""

import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

# Spark Session
spark = SparkSession.builder.appName("GetTime") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Time schema
time_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("day_type", StringType(), True),
    StructField("hour", IntegerType(), True)
])

# Get list of days from 1st Jan 2017 to 31st Dec 2022
start_time = datetime.datetime.strptime("2017-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
end_time = datetime.datetime.strptime("2022-12-31 23:00:00", "%Y-%m-%d %H:%M:%S")
time_list = []
dic_dat_of_week = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday"
}
while start_time <= end_time:
    time_list.append({
        "id": 0, # This will be updated later in the code
        "year": start_time.year,
        "month": start_time.month,
        "day": start_time.day,
        "day_of_week": dic_dat_of_week[start_time.weekday()],
        "day_type": "weekday" if start_time.weekday() < 5 else "weekend",
        "hour": start_time.hour
    })
    start_time += datetime.timedelta(hours=1)

# Create Spark df
time_df = spark.createDataFrame(time_list, schema=time_schema)

# Add id column
time_df = time_df.withColumn("id", monotonically_increasing_id())

# Show Spark df
time_df.show()

# Write to Parquet
output_path = "hdfs://namenode:8020/output/time.parquet"
time_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# Stop Spark
spark.stop()