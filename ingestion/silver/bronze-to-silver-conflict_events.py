from pyspark.sql import SparkSession
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session
#starting the spark session for Conflict Events
spark = get_spark_session('BronzeToSilver-ConflictEvents')

# 1. reading the raw data from Bronze layer
bronze_df = spark.read.format('delta').load('s3a://lakehouse/bronze/conflict_events')

# 2. cleaning and removing duplicates
keys = ['location_code', 'reference_period_start']
cleaned_df = bronze_df.dropna(subset=keys)
deduplicated_df = cleaned_df.dropDuplicates(keys)

# 3. Create database and table in silver
spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")

# 3. Write to Silver using AvailableNow
# query = (deduplicated_df.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", "s3a://lakehouse/checkpoints/silver/currency")
#     .trigger(availableNow=True) # 👈 THE MAGIC TRICK: Process new data and shut down
#     .start("s3a://lakehouse/silver/currency"))

# query.awaitTermination()

query = deduplicated_df.write.format('delta').mode('overwrite').save('s3a://lakehouse/silver/conflict_events')

print('Conflict Events Silver layer updated successfully.')