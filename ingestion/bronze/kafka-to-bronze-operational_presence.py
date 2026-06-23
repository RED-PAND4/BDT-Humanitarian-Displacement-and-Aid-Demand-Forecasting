import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "operational_presence"

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-OperationalPresence")

    schema = StructType([
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("admin1_code", StringType(), True),
        StructField("admin1_name", StringType(), True),
        StructField("admin2_code", StringType(), True),
        StructField("admin2_name", StringType(), True),
        StructField("admin_level", IntegerType(), True),
        StructField("resource_hdx_id", StringType(), True),
        StructField("org_acronym", StringType(), True),
        StructField("org_name", StringType(), True),
        StructField("sector_code", StringType(), True),
        StructField("sector_name", StringType(), True),
        StructField("reference_period_start", StringType(), True),
        StructField("reference_period_end", StringType(), True),
        StructField("org_type_code", StringType(), True),
        StructField("org_type_description", StringType(), True)
    ])

    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "org_acronym": "org_acronym",
        "org_name": "org_name",
        "sector_code": "sector_code",
        "sector_name": "sector_name",
        "org_type_description": "org_type_description",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
    }

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.operational_presence
        USING delta
        LOCATION 's3a://lakehouse/bronze/operational_presence'
    """)
    print("Starting Kafka Read Stream...")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    print("Parse Kafka Read Stream...")

    parsed_df = parse_kafka_message(
        df=raw_df,
        schema=schema,
        fields_mapping=my_fields_to_keep
    )
    target_columns = list(my_fields_to_keep.values())

    print("Starting Write Streams...")

    console_query = (
        parsed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/operational_presence"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        # .trigger(processingTime="10 seconds")
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/operational_presence")
    )

    print("Waiting for streams to finish...")
    delta_query.awaitTermination()