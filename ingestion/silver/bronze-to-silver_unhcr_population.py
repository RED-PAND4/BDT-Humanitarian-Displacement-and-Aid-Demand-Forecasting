from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import *

spark = get_spark_session("BronzeToSilver_UNHCR_Population")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/population")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
) 

subset_cols = ["year", "coo_iso", "coa_iso"]

cleaned_df = bronze_df.dropna(subset=subset_cols)

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=subset_cols)

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="population"
)


# # Check if the Silver table is already initialized with a schema
# try:
#     silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/population")
#     is_table_initialized = "year" in silver_df.columns
# except Exception:
#     is_table_initialized = False

# # 3. Write to Silver safely
# unique_years_rows = deduplicated_df.select("year").distinct().collect()
# unique_years = [row['year'] for row in unique_years_rows]

# if unique_years:
#     if is_table_initialized:
#         # Scenario A: Table has a schema -> Perform selective overwrite
#         years_predicate = ", ".join([f"'{y}'" if isinstance(y, str) else str(y) for y in unique_years])
#         replace_condition = f"year IN ({years_predicate})"
        
#         print(f"Applying selective overwrite for years: {unique_years}")
#         (deduplicated_df.write 
#             .format("delta") 
#             .mode("overwrite") 
#             .option("replaceWhere", replace_condition)
#             .save("s3a://lakehouse/silver/population")
#         )
#     else:
#         # Scenario B: Table is brand new/empty -> Append to initialize the schema
#         print("Silver table has no schema yet. Initializing table structure...")
#         (deduplicated_df.write 
#             .format("delta") 
#             .mode("append") 
#             .save("s3a://lakehouse/silver/population")
#         )
# else:
#     print("No records found in Bronze to write to Silver today.") 

upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_df, 
    table_name="population"
)

# #  3. Write to Silver using AvailableNow
# query= (deduplicated_df.write 
#     .format("delta") 
#     .mode("append") 
#     .save("s3a://lakehouse/silver/population")
# )

#print("Taking out the old files in the silver layer...")
#spark.sql("VACUUM silver.population")
