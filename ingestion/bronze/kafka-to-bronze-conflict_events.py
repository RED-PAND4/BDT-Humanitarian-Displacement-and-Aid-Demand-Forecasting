import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "conflict_events"

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-ConflictEvents")

    schema = StructType([
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("admin1_code", StringType(), True),
        StructField("admin1_name", StringType(), True),
        StructField("admin2_code", StringType(), True),
        StructField("admin2_name", StringType(), True),
        StructField("admin_level", IntegerType(), True),
        StructField("resource_hdx_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("events", IntegerType(), True),
        StructField("fatalities", IntegerType(), True),
        StructField("reference_period_start", StringType(), True),
        StructField("reference_period_end", StringType(), True)
    ])

    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "admin1_code": "admin1_code",
        "admin1_name": "admin1_name",
        "admin2_code": "admin2_code",
        "admin2_name": "admin2_name",
        "admin_level": "admin_level",
        "resource_hdx_id": "resource_hdx_id",
        "event_type": "event_type",
        "events": "events",
        "fatalities": "fatalities",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end"
    }

    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="conflict_events"
    )

    print("Starting Kafka Read Stream...")

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "earliest")
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

    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/conflict_events"

    print("Writing stream to Delta Lake Bronze...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/conflict_events")
    )

    print("Waiting for streams to finish...")
    delta_query.awaitTermination()
    spark.stop()