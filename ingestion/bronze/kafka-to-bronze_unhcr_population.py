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
# Questo topic deve corrispondere esattamente a quello nel tuo main_unhcr_population.py
KAFKA_TOPIC = "population" 

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze_UNHCR_Population")
    #print("Clearing stale catalog metadata...")
    #spark.sql("DROP TABLE IF EXISTS default.test1")
    
    # Schema basato sul JSON dell'API UNHCR Population
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("coo_id", StringType(), True),
        StructField("coo_name", StringType(), True),
        StructField("coo", StringType(), True),
        StructField("coo_iso", StringType(), True),
        StructField("coa_id", StringType(), True),
        StructField("coa_name", StringType(), True),
        StructField("coa", StringType(), True),
        StructField("coa_iso", StringType(), True),
        StructField("refugees", IntegerType(), True),
        StructField("asylum_seekers", IntegerType(), True),
        StructField("returned_refugees", IntegerType(), True),
        StructField("idps", IntegerType(), True),
        StructField("returned_idps", IntegerType(), True),
        StructField("stateless", StringType(), True), # Nel JSON è "0" o testo
        StructField("ooc", IntegerType(), True),
        StructField("oip", StringType(), True),       # Nel JSON c'è spesso "-"
        StructField("hst", StringType(), True)        # Nel JSON è "0" o testo
    ])

    # Dizionario di mappatura: manteniamo tutti i campi
    my_fields_to_keep = {
        "year": "year",
        "coo_id": "coo_id",
        "coo_name": "coo_name",
        "coo": "coo",
        "coo_iso": "coo_iso",
        "coa_id": "coa_id",
        "coa_name": "coa_name",
        "coa": "coa",
        "coa_iso": "coa_iso",
        "refugees": "refugees",
        "asylum_seekers": "asylum_seekers",
        "returned_refugees": "returned_refugees",
        "idps": "idps",
        "returned_idps": "returned_idps",
        "stateless": "stateless",
        "ooc": "ooc",
        "oip": "oip",
        "hst": "hst"
    }


    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.population
        USING delta
        LOCATION 's3a://lakehouse/bronze/population'
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
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze_population"

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
        .start("s3a://lakehouse/bronze/population")
        #.start()
    )


    
    print("Waiting for streams to finish...")
    #spark.stop()
    # Wait for the streams to process data indefinitely
    delta_query.awaitTermination()
