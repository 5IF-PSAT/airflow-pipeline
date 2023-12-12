"""
Get the grapes from the wine dataframe
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import col, split, explode, monotonically_increasing_id, regexp_replace

spark = SparkSession.builder.appName("Get Grapes") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Define schema
# WineID,WineName,Type,Elaborate,Grapes,
# Harmonize,ABV,Body,Acidity,
# Code,Country,RegionID,RegionName,
# WineryID,WineryName,Website,Vintages
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

# Convert "Grapes" column to list

df = df.withColumn("Grapes", split(df['Grapes'], ", ")) \
    .withColumn("Grapes", col("Grapes").cast(ArrayType(StringType())))

df = df.withColumn("Grapes", explode("Grapes")) \
    .withColumn("Grapes", regexp_replace(col("Grapes"), "[\\[\\]\"']", ""))

# Get the grapes

grapes_df = df.select("Grapes").distinct().orderBy("Grapes", ascending=True)

# Show the grapes

grapes_df.show()

# Define id for grapes

grapes_df = grapes_df.withColumn("GrapeID", monotonically_increasing_id())

# Join the grapes with the wine dataframe

wine_grape_df = df.join(grapes_df, df.Grapes == grapes_df.Grapes, "inner") \
    .select(df.WineID, grapes_df.GrapeID)

# Save the grapes to CSV

grapes_df.write.csv("/opt/bitnami/spark/resources/data/grapes.csv", header=True, mode="overwrite")

# Save the wine_vintage to CSV

wine_grape_df.write.csv("/opt/bitnami/spark/resources/data/wine_grape.csv", header=True, mode="overwrite")

# Stop Spark

spark.stop()
