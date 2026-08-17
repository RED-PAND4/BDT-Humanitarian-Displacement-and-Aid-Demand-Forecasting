import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message, initialize_delta_table

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "worldbank_mpi" 

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-WorldBank-MPM")
    
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

    my_fields_to_keep = {
        "countryiso3code": "location_code",
        "country.value": "location_name", 
        "date": "year",                 
        "value": "mpm_value"     
    }

    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="worldbank_mpm"
    )

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    
    parsed_df = parse_kafka_message(
        df=raw_df,
        schema=schema,
        fields_mapping=my_fields_to_keep
    )
    
    parsed_df = parsed_df.filter(col("location_code").isNotNull() & (col("location_code") != ""))
    parsed_df = parsed_df.withColumn("ingested_at", current_timestamp())

    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze/worldbank_mpm"

    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/worldbank_mpm")
    )

    delta_query.awaitTermination()
    spark.stop()
    sys.exit(0)