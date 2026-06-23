from pyspark.sql import SparkSession
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

# Starting the Spark session for National Risk
spark = get_spark_session("BronzeToSilver-NationalRisk")

# 1. reading the raw data from Bronze layer
bronze_df = spark.read.format("delta").load("s3a://lakehouse/bronze/national_risk")

# 2. cleaning and removing duplicates
# We use the country code (location_code) and the start of the reference year as primary keys
keys = ["location_code", "reference_period_start"]
cleaned_df = bronze_df.dropna(subset=keys)
deduplicated_df = cleaned_df.dropDuplicates(keys)

# 3. Create database and table in silver (if they don't exist)
spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")
spark.sql("CREATE TABLE IF NOT EXISTS silver.national_risk USING delta LOCATION 's3a://lakehouse/silver/national_risk'")
#  Write to Silver using AvailableNow
# query = (deduplicated_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver/currency")
#     .trigger(availableNow=True) # 👈 THE MAGIC TRICK: Process new data and shut down
#     .start("s3a://lakehouse/silver/currency"))

# query.awaitTermination()

# 4. Final batch write with overwrite to remove historical duplicates
query = deduplicated_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/national_risk")

print("National Risk Silver layer updated successfully.")