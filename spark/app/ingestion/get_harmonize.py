"""
Get the harmonize from the wine dataframe
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import col, split, explode, monotonically_increasing_id, regexp_replace

spark = SparkSession.builder.appName("Get Harmonize") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Define schema

schema = StructType([
    StructField("WineID", IntegerType(), True),
    StructField("WineName", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Elaborate", StringType(), True),
    StructField("Grapes", StringType(), True),
    StructField("Harmonize", StringType(), True),
    StructField("ABV", DoubleType(), True),
    StructField("Body", StringType(), True),
    StructField("Acidity", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("RegionID", IntegerType(), True),
    StructField("RegionName", StringType(), True),
    StructField("WineryID", IntegerType(), True),
    StructField("WineryName", StringType(), True),
    StructField("Website", StringType(), True),
    StructField("Vintages", StringType(), True)
])

# Read Parquet

df = spark.read.csv("/opt/bitnami/spark/resources/data/XWines_Full_100K_wines.csv", schema=schema, header=True)

# Convert "Harmonize" column to list

df = df.withColumn("Harmonize", split(df['Harmonize'], ", ")) \
    .withColumn("Harmonize", col("Harmonize").cast(ArrayType(StringType())))

df = df.withColumn("Harmonize", explode("Harmonize")) \
    .withColumn("Harmonize", regexp_replace(col("Harmonize"), "[\\[\\]\"']", ""))

# Get the harmonize

harmonize_df = df.select("Harmonize").distinct().orderBy("Harmonize", ascending=True)

# Show the harmonize

harmonize_df.show()

# Define id for harmonize

harmonize_df = harmonize_df.withColumn("HarmonizeID", monotonically_increasing_id())

# Join the harmonize with the wine dataframe

wine_harmonize_df = df.join(harmonize_df, df.Harmonize == harmonize_df.Harmonize, "inner") \
    .select(df.WineID, harmonize_df.HarmonizeID)

# Save the harmonize to CSV

harmonize_df.write.csv("/opt/bitnami/spark/resources/data/harmonize.csv", header=True, mode="overwrite")

# Save the wine_vintage to CSV

wine_harmonize_df.write.csv("/opt/bitnami/spark/resources/data/wine_harmonize.csv", header=True, mode="overwrite")

# Stop Spark

spark.stop()
