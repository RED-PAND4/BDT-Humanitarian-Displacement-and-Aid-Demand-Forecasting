import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional

def get_spark_session(name: StringType) -> SparkSession:
    """Initializes Spark Session with Delta Lake extensions."""
    spark = (SparkSession.builder
        .appName(name)
        #Limit resources config
        .config("spark.driver.memory", "1g")       # Limit driver RAM
        .config("spark.executor.memory", "1g")     # Limit worker RAM
        .config("spark.executor.cores", "1")       # Only use 1 core per executor
        .config("spark.cores.max", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.warehouse.dir", "s3a://lakehouse")
        # --- Hive Metastore Integration Configuration ---
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        # --- MINIO CONFIGURATIONS ---
        # Credentials
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        # Required for local S3 alternatives like MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") # Replace with your MinIO IP/Port
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        # Unlocking disable the 7-day minimum retention check
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")   
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_kafka_message(df: DataFrame, schema: StructType, fields_mapping: Dict[str, str]) -> DataFrame:
    """
    Parses incoming Kafka JSON messages and extracts desired fields dynamically.

    Args:
        df: The raw Kafka streaming DataFrame.
        schema: The expected schema of the incoming JSON.
        fields_mapping: A dictionary where Keys are the original JSON fields
                        and Values are the desired DataFrame column names.
                        Example: {"name": "location_name", "id": "location_id"}
    """
    # 1. Clean and parse the raw Kafka value
    parsed_df = (
        df
        .withColumn("raw_string", col("value").cast("string"))
        .withColumn("valid_json_string", regexp_replace(col("raw_string"), "'", '"'))
        .select(from_json(col("valid_json_string"), schema).alias("data"))
    )

    # 2. Dynamically build the select expressions based on the mapping dictionary
    # This translates to: [col("data.code").alias("location_code"), col("data.name").alias("location_name"), ...]
    select_exprs = [
        col(f"data.{json_field}").alias(new_col_name)
        for json_field, new_col_name in fields_mapping.items()
    ]

    # 3. Unpack the list of expressions into the select statement
    return parsed_df.select(*select_exprs)