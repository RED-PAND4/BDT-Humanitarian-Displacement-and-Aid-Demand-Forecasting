import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import Dict, List, Optional

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "funding"

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-Funding")

    schema = StructType([
        StructField("resource_hdx_id", StringType(), True),
        StructField("appeal_code", StringType(), True),
        StructField("appeal_name", StringType(), True),
        StructField("appeal_type", StringType(), True),
        StructField("requirements_usd", DoubleType(), True),
        StructField("funding_usd", DoubleType(), True),
        StructField("funding_pct", DoubleType(), True),
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("reference_period_start", StringType(), True),
        StructField("reference_period_end", StringType(), True)
    ])

    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "appeal_code": "appeal_code",
        "appeal_name": "appeal_name",
        "appeal_type": "appeal_type",
        "requirements_usd": "requirements_usd",
        "funding_usd": "funding_usd",
        "funding_pct": "funding_pct",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.funding
        USING delta
        LOCATION 's3a://lakehouse/bronze/funding'
    """)
    print("Starting Kafka Read Stream...")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    print("Parse Kafka Read Stream...")

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

    console_query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/funding"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        #.trigger(processingTime="10 seconds")
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/funding")
    )

    print("Waiting for streams to finish...")
    delta_query.awaitTermination()