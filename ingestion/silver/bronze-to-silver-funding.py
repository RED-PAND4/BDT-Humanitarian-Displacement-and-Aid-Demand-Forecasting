from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

# Starting the Spark session forFunding
spark = get_spark_session("BronzeToSilver-Funding")

# 1. reading the raw data from Bronze layer 
bronze_df = (spark.read
    .format("delta")
    .load("s3a://lakehouse/bronze/funding")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
)

# 2. cleaning and removing duplicates

cleaned_df = bronze_df.dropna(subset=["location_code", "appeal_code"])

subset_cols = [
    "resource_hdx_id",
    "location_code",
    "location_name",
    "appeal_code",
    "appeal_name",
    "appeal_type",
    "requirements_usd",
    "funding_usd",
    "funding_pct",
    "reference_period_start",
    "reference_period_end"
]

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)
deduplicated_with_column_df = extract_date_components(deduplicated_df, "reference_period_start")

# 3. Create database and table in silver
initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="funding"
)

# 3. Write to Silver 
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_with_column_df, 
    table_name="funding"
)

