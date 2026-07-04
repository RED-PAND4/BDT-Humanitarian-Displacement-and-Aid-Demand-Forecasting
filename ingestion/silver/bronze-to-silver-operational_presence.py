from pyspark.sql import SparkSession
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

# Starting the Spark session for Operational Presence
spark = get_spark_session("BronzeToSilver-OperationalPresence")

# 1. reading the raw data from Bronze layer
bronze_df = spark.read.format("delta").load("s3a://lakehouse/bronze/operational_presence")

# 2. cleaning and removing duplicates
keys = ["location_code", "org_acronym", "sector_code", "reference_period_start"]
cleaned_df = bronze_df.dropna(subset=keys)
deduplicated_df = cleaned_df.dropDuplicates(keys)

# 3. Create database and table in silver (if they don't exist)
spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")
spark.sql("CREATE TABLE IF NOT EXISTS silver.operational_presence USING delta LOCATION 's3a://lakehouse/silver/operational_presence'")

# 4. Final batch write with overwrite to remove historical duplicates
query = deduplicated_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/operational_presence")

print("Operational Presence Silver layer updated successfully.")