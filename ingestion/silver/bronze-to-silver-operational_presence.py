from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

# Starting the Spark session for Operational Presence
spark = get_spark_session("BronzeToSilver-OperationalPresence")

# 1. reading the raw data from Bronze layer
bronze_df = (spark.read
    .format("delta")
    .load("s3a://lakehouse/bronze/operational_presence")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today      
)    

# 2. cleaning and removing duplicates
keys = ["location_code", "org_acronym", "sector_code"]
cleaned_df = bronze_df.dropna(subset=keys)
subset_cols = [
    "location_code",
    "location_name",
    "admin1_code",
    "admin1_name",
    "admin2_code",
    "admin2_name",
    "admin_level",
    "resource_hdx_id",
    "org_acronym",
    "org_name",
    "sector_code",
    "sector_name",
    "org_type_code",
    "org_type_description",
    "reference_period_start",
    "reference_period_end"
]

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)
deduplicated_with_column_df = extract_date_components(deduplicated_df, "reference_period_start")

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="operational_presence"
)

# 3. Write to Silver 
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_with_column_df, 
    table_name="operational_presence"
)

# # 4. Final batch write with overwrite to remove historical duplicates
# query = deduplicated_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/operational_presence")

# print("Operational Presence Silver layer updated successfully.")