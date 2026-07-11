from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

# Starting the Spark session for National Risk
spark = get_spark_session("BronzeToSilver-NationalRisk")

# 1. reading the raw data from Bronze layer
bronze_df = (spark.read
    .format("delta")
    .load("s3a://lakehouse/bronze/national_risk")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
)

# 2. cleaning and removing duplicates
# We use the country code (location_code) and the start of the reference year as primary keys
keys = ["location_code", "risk_class"]
cleaned_df = bronze_df.dropna(subset=keys)

subset_cols = [
    "location_code",
    "location_name",
    "risk_class",
    "global_rank",
    "overall_risk",
    "hazard_exposure_risk",
    "vulnerability_risk",
    "coping_capacity_risk",
    "meta_missing_indicators_pct",
    "meta_avg_recentness_years",
    "reference_period_start",
    "reference_period_end",
    "resource_hdx_id"
] 
deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)
deduplicated_with_column_df = extract_date_components(deduplicated_df, "reference_period_start")


# 3. Create database and table in silver (if they don't exist)
initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="national_risk"
)

# 3. Write to Silver 
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_with_column_df, 
    table_name="national_risk"
)

# # 4. Final batch write with overwrite to remove historical duplicates
# query = deduplicated_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/silver/national_risk")

# print("National Risk Silver layer updated successfully.")