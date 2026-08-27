import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message, initialize_delta_table

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "worldbank_extreme_poverty"

if __name__ == "__main__":

    # ==========================================
    # PHASE 1: Initialize Spark Session
    # ==========================================
    spark = get_spark_session("KafkaToBronze-WorldBank-Extreme-Poverty")

    # ==========================================
    # PHASE 2: Define JSON Schema
    # ==========================================
    # Explicit schema mapping the raw payload fields from the Kafka topic
    schema = StructType([
        StructField("indicator", StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ]), True),
        StructField("country", StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ]), True),
        StructField("countryiso3code", StringType(), True),
        StructField("date", StringType(), True),
        StructField("value", DoubleType(), True), 
        StructField("unit", StringType(), True),
        StructField("obs_status", StringType(), True),
        StructField("decimal", IntegerType(), True)
    ])

    # Dictionary mapping fields to keep
    # Strategic Mapping: We Rename Fields for Standardization
    # Let's turn "countryiso3code" into HDX/UNHCR "location_code"
    my_fields_to_keep = {
        "countryiso3code": "location_code",
        "country.value": "location_name", 
        "date": "year",                 
        "value": "extreme_poverty_value"     
    } 

    # ==========================================
    # PHASE 3: Initialize Target Delta Table
    # ==========================================
    # Ensures the database and the empty Delta table exist on MinIO storage
    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="worldbank_extreme_poverty"
    )

    # =============================================
    # PHASE 4: Read Streaming Data from Kafka topic
    # =============================================
    print("Starting Kafka Read Stream...")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
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

    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze/worldbank_extreme_poverty"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/worldbank_extreme_poverty")
    )

    print("Waiting for streams to finish...")
    delta_query.awaitTermination()

    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)