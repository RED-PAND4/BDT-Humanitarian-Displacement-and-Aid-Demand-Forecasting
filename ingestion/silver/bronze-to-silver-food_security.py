from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver-FS")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/foodsecurity"))

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["location_code", "location_name"])

# Drop duplicates based on a unique ID or code
# Note: Streaming deduplication requires a watermark, or you can use standard batch read/writes if you prefer
deduplicated_df = cleaned_df.dropDuplicates(["location_code"])

spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.foodsecurity
    USING delta
    LOCATION 's3a://lakehouse/silver/foodsecurity'
""")

# 3. Write to Silver using AvailableNow
# query = (deduplicated_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver/foodsecurity")
#     .trigger(availableNow=True) # 👈 THE MAGIC TRICK: Process new data and shut down
#     .start("s3a://lakehouse/silver/foodsecurity"))

# query.awaitTermination()

# Step 2: Write data
query= (deduplicated_df.write 
    .format("delta") 
    #.option("<option_name>", "<option_value>") \
    .mode("append") 
    .save("s3a://lakehouse/silver/foodsecurity")
)

print("Taking out the trash in the Bronze layer...")

# Example A: Keep only the last 1 hours of deleted/old data
#spark.sql("VACUUM delta.`s3a://lakehouse/bronze/currency` RETAIN 1 HOURS")