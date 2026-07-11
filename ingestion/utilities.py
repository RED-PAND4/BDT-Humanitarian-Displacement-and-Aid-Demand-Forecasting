import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, regexp_replace, trim, current_timestamp, row_number, to_date, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Dict, List, Optional
from pyspark.sql.window import Window 
#from pyspark.sql import functions as F

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
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
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
        .withColumn("valid_json_string", regexp_replace(col("raw_string"), "'", '*'))
        .withColumn("valid_json_string", regexp_replace(col("valid_json_string"), r'"(\d+)"', r'$1'))
        .select(from_json(col("valid_json_string"), schema).alias("data"))
    )

    # 2. Dynamically build the select expressions based on the mapping dictionary
    # This is sames as: [col("data.code").alias("location_code"), col("data.name").alias("location_name"), ...]
    select_exprs = [
        col(f"data.{json_field}").alias(new_col_name)
        for json_field, new_col_name in fields_mapping.items()
    ]

    # 3. Unpack the list of expressions into the select statement
    return parsed_df.select(*select_exprs)

def clean_and_deduplicate_data(df: DataFrame, subset_cols: list) -> DataFrame:
    """
    Drops rows with nulls in critical fields and deduplicates the DataFrame
    by keeping only the most recent record based on 'ingested_at'.
    
    :param df: Input PySpark DataFrame (Bronze layer)
    :param subset_cols: List of column names to check for nulls and use as deduplication keys
    :return: Cleaned and deduplicated PySpark DataFrame
    """
    # # 1. Clean the Data: Drop rows where critical fields are null
    # cleaned_df = df.dropna(subset=subset_cols)
    # print(f"Number of records after dropping nulls: {cleaned_df.count()}")
    
    # 2. Define a Window partitioned by the passed keys and ordered by timestamp descending
    # The *subset_cols unpacks the list into separate arguments for partitionBy
    window_spec = Window.partitionBy(*subset_cols).orderBy(col("ingested_at").desc())
    
    # 3. Filter to keep only the top row (rank 1 = most recent)
    deduplicated_df = (
        df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    print(f"Number of records after deduplication: {deduplicated_df.count()}")
    
    return deduplicated_df


def upsert_to_silver_layer(spark: SparkSession, deduplicated_df: DataFrame, table_name: str, base_path: str = "s3a://lakehouse/silver/") -> None:
    """
    Safely writes data to the Silver layer using a selective overwrite by partition (year) 
    if the table exists, or initializes it if it doesn't. Finally, runs a VACUUM.
    
    :param spark: Active SparkSession instance
    :param deduplicated_df: Input deduplicated PySpark DataFrame
    :param table_name: Name of the target table (e.g., 'population')
    :param base_path: Base S3/storage path for the silver layer
    """
    # Construct the full storage path
    # Ensures no double slashes if base_path ends with a slash
    target_path = f"{base_path.rstrip('/')}/{table_name}"
    
    # 1. Check if the table is already initialized 
    silver_df = spark.read.format("delta").load(target_path)
    is_year_present = "year" in silver_df.columns
    
    # 3. Write to Silver safely
    if is_year_present:
        # Scenario A: Table has a schema -> Perform selective overwrite
        unique_years_rows = deduplicated_df.select("year").distinct().collect()
        unique_years = [row['year'] for row in unique_years_rows]
        years_predicate = ", ".join([f"'{y}'" if isinstance(y, str) else str(y) for y in unique_years])
        replace_condition = f"year IN ({years_predicate})"
        
        print(f"Applying selective overwrite to {target_path} for years: {unique_years}")
        (deduplicated_df.write 
            .format("delta") 
            .mode("overwrite") 
            .option("replaceWhere", replace_condition)
            .save(target_path)
        )
    else:
        # Scenario B: Table is brand new/empty -> Append to initialize the schema
        print(f"Silver table '{table_name}' has no schema yet or is not temporal. Initializing/Replacing table at {target_path}...")
        (deduplicated_df.write 
            .format("delta") 
            .mode("overwrite") 
            .save(target_path)
        )
    

    # 4. Maintenance: Vacuum old files
    # Using the exact identifier format specified (silver.table_name)
    #print(f"Taking out the old files in the silver layer for {table_name}...")
    #spark.sql(f"VACUUM silver.{table_name}")

def extract_date_components(df: DataFrame, date_col: str) -> DataFrame:
    """
    Extracts year, month, and day from a timestamp string column formatted as 'YYYY-MM-DDTHH:mm:ss'
    and adds them as integer columns to the DataFrame.

    Args:
        df: The input PySpark DataFrame.
        date_col: The name of the string column containing the date.

    Returns:
        DataFrame with three new columns: 'year', 'month', and 'day'.
    """
    return (
        df
        # Parse the ISO timestamp format safely using the literal 'T' mask
        .withColumn("_temp_parsed_date", to_date(col(date_col), "yyyy-MM-dd'T'HH:mm:ss"))
        
        # Extract components
        .withColumn("year", year(col("_temp_parsed_date")))
        .withColumn("month", month(col("_temp_parsed_date")))
        .withColumn("day", dayofmonth(col("_temp_parsed_date")))
        
        # Clean up the intermediate column
        .drop("_temp_parsed_date")
    )

def initialize_delta_table(spark: SparkSession, db_name: str, table_name: str) -> None:
    """
    Creates the target database and an empty Delta table if they do not already exist.
    The S3 location is automatically inferred as 's3a://lakehouse/db_name/table_name'.

    Args:
        spark: The active SparkSession object.
        db_name: Name of the database (e.g., 'silver').
        table_name: Name of the table (e.g., 'idps').
    """
    # Automatically build the location string
    location = f"s3a://lakehouse/{db_name}/{table_name}"
    
    print(f"Ensuring database '{db_name}' exists...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    
    print(f"Ensuring Delta table '{db_name}.{table_name}' is initialized at {location}...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
        USING delta
        LOCATION '{location}'
    """)