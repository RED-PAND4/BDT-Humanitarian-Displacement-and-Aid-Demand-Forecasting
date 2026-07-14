import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import Dict, List, Optional

import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message, initialize_delta_table

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "national_risk"

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-NationalRisk")

    schema = StructType([
        StructField("location_code", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("risk_class", IntegerType(), True),
        StructField("global_rank", IntegerType(), True),
        StructField("overall_risk", DoubleType(), True),
        StructField("hazard_exposure_risk", DoubleType(), True),
        StructField("vulnerability_risk", DoubleType(), True),
        StructField("coping_capacity_risk", DoubleType(), True),
        StructField("meta_missing_indicators_pct", DoubleType(), True),
        StructField("meta_avg_recentness_years", DoubleType(), True),
        StructField("reference_period_start", StringType(), True),
        StructField("reference_period_end", StringType(), True),
        StructField("resource_hdx_id", StringType(), True)
    ])

    my_fields_to_keep = {
        "location_code": "location_code",
        "location_name": "location_name",
        "risk_class": "risk_class",
        "global_rank": "global_rank",
        "overall_risk": "overall_risk",
        "hazard_exposure_risk": "hazard_exposure_risk",
        "vulnerability_risk": "vulnerability_risk",
        "coping_capacity_risk": "coping_capacity_risk",
        "meta_missing_indicators_pct": "meta_missing_indicators_pct",
        "meta_avg_recentness_years": "meta_avg_recentness_years",
        "reference_period_start": "reference_period_start",
        "reference_period_end": "reference_period_end",
        "resource_hdx_id": "resource_hdx_id"
    }

    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="national_risk"
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



    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/national_risk"

    print("Writing stream to Delta Lake...")
    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        # .trigger(processingTime="10 seconds")
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/national_risk")
    )

    print("Waiting for streams to finish...")
    delta_query.awaitTermination()