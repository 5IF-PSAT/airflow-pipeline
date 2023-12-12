"""
Get country, region, and winery from the wine dataframe
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Create Spark Session
spark = SparkSession.builder.appName("Get Regions") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Define wine schema

wine_schema = StructType([
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

df = spark.read.csv("/opt/bitnami/spark/resources/data/XWines_Full_100K_wines.csv", header=True, schema=wine_schema)

# Get the country, region, and winery

country_df = df.select("Country", "Code").distinct().orderBy("Country", ascending=True)

region_df = df.select("RegionID", "RegionName", "Country").distinct().orderBy("RegionID", ascending=True)

winery_df = df.select("WineryID", "WineryName", "Website").distinct().orderBy("WineryID", ascending=True)

winery_region_df = df.select("WineryID", "RegionID").distinct().orderBy("WineryID", ascending=True)

# Define the country id

country_df = country_df.withColumn("CountryID", monotonically_increasing_id())

# Join the region dataframe with the country dataframe

region_df = region_df.join(country_df, region_df.Country == country_df.Country) \
    .select(region_df.RegionID, region_df.RegionName, country_df.CountryID)

# Write the country dataframe to CSV

country_df.write.csv("/opt/bitnami/spark/resources/data/country.csv", header=True, mode="overwrite")

# Write the region dataframe to CSV

region_df.write.csv("/opt/bitnami/spark/resources/data/region.csv", header=True, mode="overwrite")

# Write the winery dataframe to CSV

winery_df.write.csv("/opt/bitnami/spark/resources/data/winery.csv", header=True, mode="overwrite")

# Write the winery region dataframe to CSV

winery_region_df.write.csv("/opt/bitnami/spark/resources/data/winery_region.csv", header=True, mode="overwrite")

# Stop Spark Session

spark.stop()