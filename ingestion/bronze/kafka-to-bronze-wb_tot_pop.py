import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import get_spark_session, parse_kafka_message, initialize_delta_table

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "worldbank_population"

if __name__ == "__main__":
    spark = get_spark_session("KafkaToBronze-WorldBank")
    
    # 1. Definiamo lo schema esatto che ci restituisce la World Bank
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
        StructField("date", IntegerType(), True),
        StructField("value", DoubleType(), True), # Uso Double per la popolazione totale per evitare limiti numerici
        StructField("unit", StringType(), True),
        StructField("obs_status", StringType(), True),
        StructField("decimal", IntegerType(), True)
    ])

    # 2. Mappatura strategica: rinominiamo i campi per la standardizzazione
    # Trasformiamo la "countryiso3code" nella "location_code" di HDX/UNHCR
    my_fields_to_keep = {
        "countryiso3code": "location_code",
        "country.value": "location_name", 
        "date": "year",                 
        "value": "total_population"     
    }

    initialize_delta_table(
        spark=spark,
        db_name="bronze",
        table_name="worldbank_population"
    )
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
    
    # Rimuoviamo i record aggregati della World Bank (che non hanno ISO3) o senza popolazione
    parsed_df = parsed_df.filter(col("location_code").isNotNull() & (col("location_code") != ""))
    parsed_df = parsed_df.withColumn("ingested_at", current_timestamp())

    print("Starting Write Streams...")
    CHECKPOINT_PATH = "s3a://lakehouse/checkpoints/bronze/worldbank_population"

    delta_query = (
        parsed_df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start("s3a://lakehouse/bronze/worldbank_population")
    )

    print("Waiting for streams to finish...")
    delta_query.awaitTermination()
    
    print("Execution complete. Explicitly shutting down Spark to release locks...")
    spark.stop()
    sys.exit(0)