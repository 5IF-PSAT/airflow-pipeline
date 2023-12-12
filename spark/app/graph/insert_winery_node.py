from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Insert Winery Node") \
    .master("local[*]") \
    .config("job.local.dir", "/tmp/spark-temp") \
    .config("spark.jars", "/opt/bitnami/spark/jars/neo4j-connector-apache-spark_2.12-4.0.0.jar") \
    .getOrCreate()

# Spark Context

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Read CSV file

country_df = spark.read.csv("/opt/bitnami/spark/resources/data/country.csv", header=True)

# Create Winery Node

country_df.write \
    .format("org.neo4j.spark.DataSource") \
    .mode("Overwrite") \
    .option("url", "bolt://neo4j:7687") \
    .option("authentication.type", "basic") \
    .option("labels", ":Country") \
    .option("node.keys", "CountryID") \
    .save()

# Stop Spark Session

spark.stop()