import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "humanitarian_needs"


if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze_HDX_Needs")
    #print("Clearing stale catalog metadata...")
    #spark.sql("DROP TABLE IF EXISTS default.test1")
    
    # Define schema of expected JSON message
    schema = StructType([
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("admin1_code", StringType(), True),
        StructField("admin1_name", StringType(), True),
        StructField("admin2_code", StringType(), True),
        StructField("admin2_name", StringType(), True),
        StructField("admin_level", IntegerType(), True),
        StructField("resource_hdx_id", StringType(), True),
        StructField("sector_code", StringType(), True),
        StructField("category", StringType(), True),
        StructField("population_status", StringType(), True),
        StructField("population", IntegerType(), True),
        StructField("reference_period_start", StringType(), True),
        StructField("reference_period_end", StringType(), True),
        StructField("sector_name", StringType(), True)
    ])

    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "admin1_code": "admin1_code",
        "admin1_name": "admin1_name",
        "admin2_code": "admin2_code",
        "admin2_name": "admin2_name",
        "admin_level": "admin_level",
        "sector_code": "sector_code",
        "sector_name": "sector_name",
        "category": "category",
        "population_status": "population_status",
        "population": "population",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
    }


    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.humanitarian_needs  
        USING delta
        LOCATION 's3a://lakehouse/bronze/humanitarian_needs'
    """)
    print("Starting Kafka Read Stream...")

    # Read stream from Kafka topic
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        #.option("startingOffsets", "latest")
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
    console_query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )


    # Define a path for Spark to track streaming progress
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze_needs"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        #.trigger(processingTime="10 seconds")  # Adjust trigger interval as needed
        #.option("maxOffsetsPerTrigger", "50")
        .start("s3a://lakehouse/bronze/humanitarian_needs")
        #.start()
    )

    
    print("Waiting for streams to finish...")
    #spark.stop()
    # Wait for the streams to process data indefinitely
    delta_query.awaitTermination()
