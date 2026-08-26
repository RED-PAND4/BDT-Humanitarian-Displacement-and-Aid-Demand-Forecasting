from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from utilities import *

spark = get_spark_session("BronzeToSilver_HDX_IDPs")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/idps")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
)

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["location_code", "location_name"])

# Drop duplicates based on a unique ID or code
subset_cols=[
    "location_code",
    "location_name",
    "admin1_code",
    "admin1_name",
    "admin2_code",
    "admin2_name",
    "admin_level",
    "reporting_round",
    "assessment_type",
    "operation",
    "population",
    "reference_period_start",
    "reference_period_end"
]

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)
deduplicated_with_column_df = extract_date_components(deduplicated_df, "reference_period_start")


initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="idps"
)

# 3. Write to Silver 
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_with_column_df, 
    table_name="idps"
)

# # 3. Write to Silver 
# query= (deduplicated_df.write 
#     .format("delta") 
#     .mode("overwrite") 
#     .save("s3a://lakehouse/silver/idps")

# )
