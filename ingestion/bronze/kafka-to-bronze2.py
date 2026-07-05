import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "test_data_v3"


if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze")
    #print("Clearing stale catalog metadata...")
    #spark.sql("DROP TABLE IF EXISTS default.test1")
    
    # Define schema of expected JSON message
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", StringType(), True),
        StructField("region", StringType(), True)
    ])

    my_fields_to_keep = {
        "code": "location_code",
        "name": "location_name",
        "region": "world_region"  # You can easily add more fields like this
    }


    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.test_data_v3
        USING delta
        LOCATION 's3a://lakehouse/bronze/test_data_v3'
    """)
    print("Starting Kafka Read Stream...")

    # Read stream from Kafka topic
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    print("Parse Kafka Read Stream...")
    # Transform: Parse and Clean
    parsed_df = parse_kafka_message(
        df=raw_df,
        schema=schema,
        fields_mapping=my_fields_to_keep
    )
    # target_columns = list(my_fields_to_keep.values())
    parsed_df = parsed_df.withColumn("ingested_at", current_timestamp())
    
    
    # 3. Updated target columns list to include the new column
    target_columns = list(my_fields_to_keep.values()) + ["ingested_at"]

    print("Starting Write Streams...")

    #Sink 1: Write to Console (for debugging/testing)
    console_query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )



    # Define a path for Spark to track streaming progress
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/test_data_v3"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")  # Adjust trigger interval as needed
        #.option("maxOffsetsPerTrigger", "50")
        .start("s3a://lakehouse/bronze/test_data_v3")
        #.start()
    )


    
    print("Waiting for streams to finish...")
    #spark.stop()
    # Wait for the streams to process data indefinitely
    delta_query.awaitTermination()
