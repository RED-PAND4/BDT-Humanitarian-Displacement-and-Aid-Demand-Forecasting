import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from typing import Dict, List, Optional

import time
import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "baseline_population"


if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-baselinepopulation")
    #print("Clearing stale catalog metadata...")
    #spark.sql("DROP TABLE IF EXISTS default.test1")
    
    # Define schema of expected JSON message matching the API response
    schema = StructType([
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("admin1_code", StringType(), True),
        StructField("admin1_name", StringType(), True),
        StructField("admin2_code", StringType(), True),
        StructField("admin2_name", StringType(), True),
        StructField("admin_level", IntegerType(), True),
        StructField("resource_hdx_id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age_range", StringType(), True),
        StructField("min_age", IntegerType(), True),
        StructField("max_age", IntegerType(), True),
        StructField("population", IntegerType(), True),
        StructField("reference_period_start", TimestampType(), True),
        StructField("reference_period_end", TimestampType(), True)
    ])

    # Maintaining exact variable names as keys and values from the API
    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "gender": "gender",
        "age_range": "age_range",
        "min_age": "min_age",
        "max_age": "max_age",
        "population": "population",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://lakehouse/bronze'")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.baselinepopulation
        USING delta
        LOCATION 's3a://lakehouse/bronze/baselinepopulation'
    """)
    print("Starting Kafka Read Stream...")

    # Read stream from Kafka topic
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    print("Parse Kafka Read Stream...")
    # Transform: Parse and Clean
    parsed_df = parse_kafka_message(
        df=raw_df,
        schema=schema,
        fields_mapping=my_fields_to_keep
    )
    target_columns = list(my_fields_to_keep.values())

    print("Starting Write Streams...")

    #Sink 1: Write to Console (for debugging/testing)
    # console_query = (
    #     parsed_df.writeStream
    #     .format("console")
    #     .outputMode("append")
    #     .option("truncate", "false")
    #     .start()
    # )



    # Define a path for Spark to track streaming progress
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze/baselinepopulation"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        #.trigger(processingTime="10 seconds")  # Adjust trigger interval as needed
        .trigger(availableNow=True)
        #.option("maxOffsetsPerTrigger", "50")
        .start("s3a://lakehouse/bronze/baselinepopulation")
    )


    
    print("Waiting for streams to finish...")
    #spark.stop()
    # Wait for the streams to process data indefinitely
    delta_query.awaitTermination()
    
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)

    
