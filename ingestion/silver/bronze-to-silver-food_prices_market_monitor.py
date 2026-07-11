from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import *

spark = get_spark_session("BronzeToSilver-FPMM")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/foodpricesmarketmonitor")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
)

# 2. Clean the Data
# Drop rows where critical fields are null (using exact API names)
cleaned_df = bronze_df.dropna(subset=["location_code","market_code", "commodity_code", "currency_code"])

# Drop duplicates based on the unique location code from the API
subset_cols = [
    "location_code",
    "location_name",
    "admin1_code",
    "admin1_name",
    "admin2_code",
    "admin2_name",
    "admin_level",
    "resource_hdx_id",
    "market_code",
    "market_name",
    "commodity_code",
    "commodity_name",
    "commodity_category",
    "currency_code",
    "unit",
    "price_flag",
    "price_type",
    "price",
    "lat",
    "lon",
    "reference_period_start",
    "reference_period_end"
]
deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)
deduplicated_with_column_df = extract_date_components(deduplicated_df, "reference_period_start")

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="foodpricesmarketmonitor"
)


# 3. Write to Silver 
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_with_column_df, 
    table_name="foodpricesmarketmonitor"
)
# query= (deduplicated_df.write 
#     .format("delta") 
#     .mode("append") 
#     .save("s3a://lakehouse/silver/foodpricesmarketmonitor")
# )

#print("Taking out the old files in the silver layer...")

#Keep only the last 1 hours of deleted/old data
#spark.sql("VACUUM silver.currency` RETAIN 1 HOURS")