from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver-WFPMarket")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/wfpmarket"))

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["code", "location_code"])

# Drop duplicates based on a unique ID or code
# Note: Streaming deduplication requires a watermark, or you can use standard batch read/writes if you prefer
deduplicated_df = cleaned_df.dropDuplicates(subset=["code", "location_code"])

spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.wfpmarket
    USING delta
    LOCATION 's3a://lakehouse/silver/wfpmarket'
""")

# 3. Write to Silver using AvailableNow
# query = (deduplicated_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver/wfpmarket")
#     .trigger(availableNow=True) # 👈 THE MAGIC TRICK: Process new data and shut down
#     .start("s3a://lakehouse/silver/wfpmarket"))

# query.awaitTermination()

# Step 2: Write data
query= (deduplicated_df.write 
    .format("delta") 
    #.option("<option_name>", "<option_value>") \
    .mode("append") 
    .saveAsTable("s3a://lakehouse/silver/wfpmarket")
)

print("Taking out the trash in the Bronze layer...")

# Example A: Keep only the last 1 hours of deleted/old data
#spark.sql("VACUUM delta.`s3a://lakehouse/bronze/currency` RETAIN 1 HOURS")