from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql import functions as F
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver_UNHCR_Population")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/population")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date())
) 
print(f"Number of records in bronze_df: {bronze_df.count()}")
# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["year", "coo_iso", "coa_iso"])
print(f"Number of records in cleaned_df: {cleaned_df.count()}")
# Drop duplicates based on a unique ID or code
# deduplicated_df = cleaned_df.dropDuplicates(["year", "coo_iso", "coa_iso"])
# print(f"Number of records in deduplicated_df: {deduplicated_df.count()}")

# 2. Define a Window partitioned by your unique keys and ordered by timestamp descending
window_spec = Window.partitionBy("year", "coo_iso", "coa_iso").orderBy(col("ingested_at").desc())

# 3. Filter to keep only the top row (rank 1 = most recent)
deduplicated_df = (
    cleaned_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.population
    USING delta
    LOCATION 's3a://lakehouse/silver/population'
""")

# Check if the Silver table is already initialized with a schema
try:
    silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/population")
    is_table_initialized = "year" in silver_df.columns
except Exception:
    is_table_initialized = False

# 3. Write to Silver safely
unique_years_rows = deduplicated_df.select("year").distinct().collect()
unique_years = [row['year'] for row in unique_years_rows]

if unique_years:
    if is_table_initialized:
        # Scenario A: Table has a schema -> Perform selective overwrite
        years_predicate = ", ".join([f"'{y}'" if isinstance(y, str) else str(y) for y in unique_years])
        replace_condition = f"year IN ({years_predicate})"
        
        print(f"Applying selective overwrite for years: {unique_years}")
        (deduplicated_df.write 
            .format("delta") 
            .mode("overwrite") 
            .option("replaceWhere", replace_condition)
            .save("s3a://lakehouse/silver/population")
        )
    else:
        # Scenario B: Table is brand new/empty -> Append to initialize the schema
        print("Silver table has no schema yet. Initializing table structure...")
        (deduplicated_df.write 
            .format("delta") 
            .mode("append") 
            .save("s3a://lakehouse/silver/population")
        )
else:
    print("No records found in Bronze to write to Silver today.") 

# #  3. Write to Silver using AvailableNow
# query= (deduplicated_df.write 
#     .format("delta") 
#     .mode("append") 
#     .save("s3a://lakehouse/silver/population")
# )

#print("Taking out the old files in the silver layer...")
spark.sql("VACUUM silver.population")
