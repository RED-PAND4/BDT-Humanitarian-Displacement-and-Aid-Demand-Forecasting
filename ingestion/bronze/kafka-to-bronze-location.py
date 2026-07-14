import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from typing import Dict, List, Optional

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message, initialize_delta_table


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "location"


if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-Location")
    #print("Clearing stale catalog metadata...")
    #spark.sql("DROP TABLE IF EXISTS default.test1")
    
    # Define schema of expected JSON message
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", StringType(), True),
        StructField("has_hrp", BooleanType(), True),
        StructField("in_gho", BooleanType(), True),
        StructField("from_cods", BooleanType(), True),
        StructField("reference_period_start", TimestampType(), True),
        StructField("reference_period_end", TimestampType(), True)
    ])

    # Maintaining exact variable names as keys and values from the API
    my_fields_to_keep = {
        "id": "id",
        "code": "code",
        "name": "name",
        "has_hrp": "has_hrp",
        "in_gho": "in_gho",
        "from_cods": "from_cods",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
        # Easily add or remove other fields from the API above as needed
    }

    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="location"
    )
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
    # target_columns = list(my_fields_to_keep.values())
    parsed_df = parsed_df.withColumn("ingested_at", current_timestamp())
    
    
    # 3. Updated target columns list to include the new column
    target_columns = list(my_fields_to_keep.values()) + ["ingested_at"]

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
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze/location"

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
        .start("s3a://lakehouse/bronze/location")
        #.start()
    )


    
    print("Waiting for streams to finish...")
    #spark.stop()
    # Wait for the streams to process data indefinitely
    delta_query.awaitTermination()
    
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)
