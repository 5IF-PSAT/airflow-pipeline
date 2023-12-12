from py2neo import Graph, Node, Relationship
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Insert Wine Node") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

graph = Graph("bolt://neo:7687")  

# Read Parquet

wine_df = spark.read.parquet("/opt/bitnami/spark/resources/data/wines.parquet")

# Iterate through the wine dataframe and create a wine node for each row

for row in wine_df.collect():
    graph.create(Node("Wine", WineID=row.WineID, WineName=row.WineName, Type=row.Type, Elaborate=row.Elaborate, 
                      ABV=row.ABV, Body=row.Body, Acidity=row.Acidity))
    winery_node = graph.nodes.match("Winery", WineryID=row.WineryID).first()
    graph.create(Relationship(winery_node, "PRODUCES", graph.nodes.match("Wine", WineID=row.WineID).first()))

# Stop Spark

spark.stop()