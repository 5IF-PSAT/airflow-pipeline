"""
Create fact table from the wine dataframe
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("Get Fact Table") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Define schema
# WineID,WineName,Type,Elaborate,ABV,Body,Acidity,WineryID
wine_schema = StructType([
    StructField("WineID", IntegerType(), True),
    StructField("WineName", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("Elaborate", StringType(), True),
    StructField("ABV", DoubleType(), True),
    StructField("Body", StringType(), True),
    StructField("Acidity", StringType(), True),
    StructField("WineryID", IntegerType(), True)
])

wine_vintage_schema = StructType([
    StructField("WineID", IntegerType(), True),
    StructField("VintageID", IntegerType(), True)
])

wine_grape_schema = StructType([
    StructField("WineID", IntegerType(), True),
    StructField("GrapeID", IntegerType(), True)
])

wine_harmonize_schema = StructType([
    StructField("WineID", IntegerType(), True),
    StructField("HarmonizeID", IntegerType(), True)
])

# Read CSV

wines_df = spark.read.csv("/opt/bitnami/spark/resources/data/XWines_Full_100K_wines_dropped.csv", schema=wine_schema, header=True)

wine_vintage_df = spark.read.csv("/opt/bitnami/spark/resources/data/wine_vintage.csv", schema=wine_vintage_schema, header=True)

fact_table_df = wines_df.join(wine_vintage_df, wines_df.WineID == wine_vintage_df.WineID, "inner") \
    .select(wines_df.WineID, wines_df.WineName, wines_df.Type, wines_df.Elaborate, wines_df.ABV, wines_df.Body, wines_df.Acidity, wines_df.WineryID, wine_vintage_df.VintageID)

wine_grape_df = spark.read.csv("/opt/bitnami/spark/resources/data/wine_grape.csv", schema=wine_grape_schema, header=True)

fact_table_df = fact_table_df.join(wine_grape_df, fact_table_df.WineID == wine_grape_df.WineID, "inner") \
    .select(fact_table_df.WineID, fact_table_df.WineName, fact_table_df.Type, fact_table_df.Elaborate, fact_table_df.ABV, fact_table_df.Body, fact_table_df.Acidity, fact_table_df.WineryID, fact_table_df.VintageID, wine_grape_df.GrapeID)

wine_harmonize_df = spark.read.csv("/opt/bitnami/spark/resources/data/wine_harmonize.csv", schema=wine_harmonize_schema, header=True)

fact_table_df = fact_table_df.join(wine_harmonize_df, fact_table_df.WineID == wine_harmonize_df.WineID, "inner") \
    .select(fact_table_df.WineID, fact_table_df.WineName, fact_table_df.Type, fact_table_df.Elaborate, fact_table_df.ABV, 
            fact_table_df.Body, fact_table_df.Acidity, fact_table_df.WineryID, fact_table_df.VintageID, fact_table_df.GrapeID, 
            wine_harmonize_df.HarmonizeID)

winery_schema = StructType([
    StructField("WineryID", IntegerType(), True),
    StructField("WineryName", StringType(), True),
    StructField("Website", StringType(), True)
])

winery_region_schema = StructType([
    StructField("WineryID", IntegerType(), True),
    StructField("RegionID", IntegerType(), True)
])

winery_df = spark.read.csv("/opt/bitnami/spark/resources/data/winery.csv", header=True, schema=winery_schema)

winery_region_df = spark.read.csv("/opt/bitnami/spark/resources/data/winery_region.csv", header=True, schema=winery_region_schema)

winery_dim_df = winery_df.join(winery_region_df, winery_df.WineryID == winery_region_df.WineryID, "inner") \
    .select(winery_df.WineryID, winery_df.WineryName, winery_df.Website, winery_region_df.RegionID)

fact_table_df = fact_table_df.join(winery_dim_df, fact_table_df.WineryID == winery_dim_df.WineryID, "inner") \
    .select(fact_table_df.WineID, fact_table_df.WineName, fact_table_df.Type, fact_table_df.Elaborate, fact_table_df.ABV,
            fact_table_df.Body, fact_table_df.Acidity, fact_table_df.VintageID, fact_table_df.GrapeID, fact_table_df.HarmonizeID,
            winery_dim_df.WineryID, winery_dim_df.WineryName, winery_dim_df.Website, winery_dim_df.RegionID)

# Join Ratings

ratings_df = spark.read.parquet("/opt/bitnami/spark/resources/data/XWines_Full_21M_ratings_aggregated.parquet")

fact_table_df = fact_table_df.join(ratings_df, (fact_table_df.WineID == ratings_df.WineID) & (fact_table_df.VintageID == ratings_df.VintageID), "left_outer") \
    .select(fact_table_df.WineID, fact_table_df.WineName, fact_table_df.Type, fact_table_df.Elaborate, fact_table_df.ABV,
            fact_table_df.Body, fact_table_df.Acidity, fact_table_df.VintageID, fact_table_df.GrapeID, fact_table_df.HarmonizeID,
            fact_table_df.WineryID, fact_table_df.WineryName, fact_table_df.Website, fact_table_df.RegionID, ratings_df.AverageRating, 
            ratings_df.RatingCount, ratings_df.MinRating, ratings_df.MaxRating)

# Winery Schema

winery_dim_df = fact_table_df.select(fact_table_df.WineryID, fact_table_df.WineryName, fact_table_df.Website) \
    .distinct() \
    .orderBy(fact_table_df.WineryID, ascending=True)

# Region Schema

region_df = spark.read.csv("/opt/bitnami/spark/resources/data/region.csv", header=True)

country_df = spark.read.csv("/opt/bitnami/spark/resources/data/country.csv", header=True)

region_dim_df = region_df.join(country_df, region_df.CountryID == country_df.CountryID, "inner") \
    .select(region_df.RegionID, region_df.RegionName, country_df.Country, country_df.Code)

# Drop columns

fact_table_df = fact_table_df.drop("WineryName", "Website")

# export to CSV

fact_table_df.coalesce(1).write.parquet("/opt/bitnami/spark/resources/data/fact_table.parquet", mode="overwrite")

region_dim_df.coalesce(1).write.csv("/opt/bitnami/spark/resources/data/region_dim.csv", header=True, mode="overwrite")

winery_dim_df.coalesce(1).write.csv("/opt/bitnami/spark/resources/data/winery_dim.csv", header=True, mode="overwrite")

# Stop Spark Session

spark.stop()