from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session, clean_and_deduplicate_data, upsert_to_silver_layer

spark = get_spark_session("BronzeToSilver_UNHCR_Solutions")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/solutions"))

# 2. Clean the Data
# Drop rows where critical fields are null
# cleaned_df = bronze_df.dropna(subset=["year", "coo_iso", "coa_iso"])

# # Drop duplicates based on a unique ID or code
# deduplicated_df = cleaned_df.dropDuplicates(["year", "coo_iso", "coa_iso"])

deduplicated_df = clean_and_deduplicate_data(df=bronze_df, subset_cols=["year", "coo_iso", "coa_iso"])

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.solutions
    USING delta
    LOCATION 's3a://lakehouse/silver/solutions'
""")

upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_df, 
    table_name="solutions"
)

# # 3. Write to Silver 
# query= (deduplicated_df.write 
#     .format("delta") 
#     .mode("overwrite") 
#     .save("s3a://lakehouse/silver/solutions")

# )

# print("Taking out the old files in the silver layer...")
# spark.sql("VACUUM silver.solutions")