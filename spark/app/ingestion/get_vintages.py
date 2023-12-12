"""
Get the vintages from the wine dataframe
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import col, split, explode, monotonically_increasing_id

spark = SparkSession.builder.appName("Get Vintages") \
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

# Convert "Vintages" column to list

df = df.withColumn("Vintages", split(df['Vintages'], ", ")) \
    .withColumn("Vintages", col("Vintages").cast(ArrayType(IntegerType())))

df = df.withColumn("Vintages", explode("Vintages"))

# Get the vintages

vintages_df = df.select("Vintages").distinct().orderBy("Vintages", ascending=True)

# Show the vintages

vintages_df.show()

# Define id for vintages

vintages_df = vintages_df.withColumn("VintageID", monotonically_increasing_id())

# Join the vintages with the wine dataframe

wine_vintage_df = df.join(vintages_df, df.Vintages == vintages_df.Vintages, "inner") \
    .select(df.WineID, vintages_df.VintageID)

# Save the vintages to CSV

vintages_df.write.csv("/opt/bitnami/spark/resources/data/vintage.csv", header=True, mode="overwrite")

# Save the wine_vintage to CSV

wine_vintage_df.write.csv("/opt/bitnami/spark/resources/data/wine_vintage.csv", header=True, mode="overwrite")

# Stop Spark

spark.stop()
