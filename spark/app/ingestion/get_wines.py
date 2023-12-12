from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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

# Read CSV

df = spark.read.csv("/opt/bitnami/spark/resources/data/XWines_Full_100K_wines.csv", schema=schema, header=True)

# Drop unused columns

df = df.drop("Grapes", "Harmonize", "Code", "Country", "RegionName", "WineryID", "WineryName", "Website", "Vintages")

# Export to Parquet

df.coalesce(1).write.mode("overwrite").parquet("/opt/bitnami/spark/resources/data/wines.parquet")

# Stop Spark Session

spark.stop()