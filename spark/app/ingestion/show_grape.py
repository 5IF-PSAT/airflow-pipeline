"""
Show grape of given wine
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Show Grape") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Get data frames
grapes_schema = StructType([
    StructField("GrapeId", IntegerType(), True),
    StructField("Grapes", StringType(), True)
])

grapes_df = spark.read.csv("/opt/bitnami/spark/resources/data/grapes.csv", header=True, schema=grapes_schema)

wine_grape_schema = StructType([
    StructField("WineId", IntegerType(), True),
    StructField("GrapeId", IntegerType(), True)
])

wine_grapes_df = spark.read.csv("/opt/bitnami/spark/resources/data/wine_grape.csv", header=True, schema=wine_grape_schema)

print("There are {} relations".format(wine_grapes_df.count()))

# Show the result
grapes_df.join(wine_grapes_df, "GrapeId").show()