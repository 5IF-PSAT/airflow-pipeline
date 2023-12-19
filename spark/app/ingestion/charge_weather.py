from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
import os
import sys
import pandas as pd

def process_json_file(file_path):
    """
    Process a JSON file and returns a DataFrame
    Returns:
    --------
    Pandas DataFrame
    """
    region_id = int(file_path.split("/")[-1].split(".")[0].split("_")[1])
    print(f"Processing file with region id {region_id}")
    # Read JSON file
    df = pd.read_json(file_path)
    # Add the region id to the DataFrame
    df['RegionID'] = region_id
    # Return the DataFrame
    return df

# Parameters
start_year = int(sys.argv[1])

# Initialize SparkSession
spark = SparkSession.builder.appName("Parallel JSON Reading").getOrCreate()

list_dir = os.listdir(f"/opt/bitnami/spark/resources/daily_data_{start_year}")
json_files = [f"/opt/bitnami/spark/resources/daily_data_{start_year}/{file}" for file in list_dir]

# Parallelize the reading of JSON files
json_dataframes = spark.sparkContext.parallelize(json_files).map(process_json_file)

# Convert the RDD to a DataFrame
df = json_dataframes.toDF()

# Print the schema
df.printSchema()

# Show the DataFrame
df.show()

# Stop SparkSession
spark.stop()
