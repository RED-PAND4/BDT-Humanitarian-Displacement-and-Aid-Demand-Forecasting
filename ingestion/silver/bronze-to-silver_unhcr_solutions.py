from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver_UNHCR_Solutions")

# 1. Read Bronze as a Stream
bronze_df = (spark.readStream
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/solutions"))

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["year", "coo_iso"])

# Drop duplicates based on a unique ID or code
# Note: Streaming deduplication requires a watermark, or you can use standard batch read/writes if you prefer
deduplicated_df = cleaned_df.dropDuplicates(["year", "coo_iso", "coa_iso"])

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.solutions
    USING delta
    LOCATION 's3a://lakehouse/silver/solutions'
""")

# 3. Write to Silver using AvailableNow
query = (deduplicated_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver_solutions")
    .trigger(availableNow=True) # 👈 THE MAGIC TRICK: Process new data and shut down
    .start("s3a://lakehouse/silver/solutions"))

query.awaitTermination()

print("Taking out the trash in the Bronze layer...")

# Example A: Keep only the last 24 hours of deleted/old data
spark.sql("VACUUM delta.`s3a://lakehouse/bronze/solutions` RETAIN 24 HOURS")