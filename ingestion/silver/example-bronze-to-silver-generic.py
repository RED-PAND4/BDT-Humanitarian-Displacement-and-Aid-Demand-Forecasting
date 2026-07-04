from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver")

# 1. Read Bronze as a Stream
bronze_df = (spark.readStream
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/test_data"))

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["location_code", "location_name"])

# Drop duplicates based on a unique ID or code
deduplicated_df = cleaned_df.dropDuplicates(["location_code"])

# 3. Create Database and Table inside Hive-Metastore
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.test_data
    USING delta
    LOCATION 's3a://lakehouse/silver/test_data'
""")

# 4. Write to Silver using AvailableNow
query = (deduplicated_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver_processing")
    .trigger(availableNow=True) 
    .start("s3a://lakehouse/silver/test_data"))

query.awaitTermination()

#print("Taking out the trash in the Bronze layer...")

# Keep only the last 24 hours of deleted/old data
spark.sql("VACUUM delta.`s3a://lakehouse/bronze/test_data_v3` RETAIN 24 HOURS")