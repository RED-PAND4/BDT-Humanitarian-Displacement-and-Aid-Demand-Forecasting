from pyspark.sql import SparkSession
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session

spark = get_spark_session('BronzeToSilver-ConflictEvents')

# 1. Read from Bronze
bronze_df = spark.read.format('delta').load('s3a://lakehouse/bronze/conflict_events')

# 2. Clean and deduplicate
keys = ['location_code', 'reference_period_start']
cleaned_df = bronze_df.dropna(subset=keys)
deduplicated_df = cleaned_df.dropDuplicates(keys)

# 3. Create Silver database and table
spark.sql("CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://lakehouse/silver'")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.conflict_events (
        location_code STRING,
        location_name STRING,
        admin1_name STRING,
        admin2_name STRING,
        event_type STRING,
        events INT,
        fatalities INT,
        reference_period_start STRING,
        reference_period_end STRING
    )
    USING delta
    LOCATION 's3a://lakehouse/silver/conflict_events'
""")

# 4. Write to Silver
deduplicated_df.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save('s3a://lakehouse/silver/conflict_events')

print('Conflict Events Silver layer updated successfully.')
spark.stop()