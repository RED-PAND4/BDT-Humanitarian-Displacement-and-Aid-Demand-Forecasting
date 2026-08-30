import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message, initialize_delta_table

KAFKA_BROKER = "kafka:9092"
# This must match the topic used in the main_hdx_idps.py file
KAFKA_TOPIC = "idps"

if __name__ == "__main__":

    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("KafkaToBronze_HDX_IDPs")
    #print("Clearing stale catalog metadata...")
    #spark.sql("DROP TABLE IF EXISTS default.test1")
    
    # ==========================================
    # PHASE 2: Define JSON Schema
    # ==========================================
    # Explicit schema mapping the raw payload fields from the Kafka topic
    schema = StructType([
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("admin1_code", StringType(), True),
        StructField("admin1_name", StringType(), True),
        StructField("admin2_code", StringType(), True),
        StructField("admin2_name", StringType(), True),
        StructField("admin_level", IntegerType(), True),
        StructField("resource_hdx_id", StringType(), True),
        StructField("reporting_round", IntegerType(), True),
        StructField("assessment_type", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("population", IntegerType(), True),
        StructField("reference_period_start", StringType(), True),
        StructField("reference_period_end", StringType(), True)
    ])

    # Dictionary mapping fields to keep
    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "admin1_code": "admin1_code",
        "admin1_name": "admin1_name",
        "admin2_code": "admin2_code",
        "admin2_name": "admin2_name",
        "admin_level": "admin_level",
        "resource_hdx_id": "resource_hdx_id",
        "reporting_round": "reporting_round",
        "assessment_type": "assessment_type",
        "operation": "operation",
        "population": "population",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
    }

    # ==========================================
    # PHASE 3: Initialize Target Delta Table
    # ==========================================
    # Ensures the database and the empty Delta table exist on MinIO storage
    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="idps"
    )

    # ==========================================
    # PHASE 4: Read Streaming Data from Kafka topic
    # ==========================================
    print("Starting Kafka Read Stream...")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest") # earliest to start from scratch, latest to process only new data
        #.option("startingOffsets", "latest")
        .load()
    )

    # ==========================================
    # PHASE 5: Parse and Enrich Data
    # ==========================================
    print("Parse Kafka Read Stream...")
    # Parse JSON using the defined schema and select mapped fields via utilities
    parsed_df = parse_kafka_message(
        df=raw_df,
        schema=schema,
        fields_mapping=my_fields_to_keep
    )
    
    # Append ingestion metadata timestamp for lineage and tracking
    parsed_df = parsed_df.withColumn("ingested_at", current_timestamp())
    
    # Updated target columns list to include the new column
    target_columns = list(my_fields_to_keep.values()) + ["ingested_at"]
    
    # ==========================================
    # PHASE 6: Write Stream to Delta Lake (Bronze)
    # ==========================================
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
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze_idps"

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
        .start("s3a://lakehouse/bronze/idps")
        #.start()
    )

    print("Waiting for streams to finish...")
    #spark.stop()
    # Wait for the streams to process data indefinitely
    delta_query.awaitTermination()

    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)
