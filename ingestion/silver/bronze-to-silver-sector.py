from pyspark.sql import SparkSession
import sys
import os
from pyspark.sql.functions import col, to_date, year, month, dayofmonth
from pyspark.sql import functions as F

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import *

spark = get_spark_session("BronzeToSilver-Sector")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/sector")
    .filter(F.to_date(F.col("ingested_at")) >= F.current_date()) # process only the data from kafka done today
)

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["code", "name"])

# Drop duplicates based on a unique ID or code

deduplicated_df = clean_and_deduplicate_data(df=cleaned_df, subset_cols=["code", "name"])

initialize_delta_table(
    spark=spark,
    db_name="silver",
    table_name="sector"
)

# 3. Write to Silver 
upsert_to_silver_layer(
    spark=spark, 
    deduplicated_df=deduplicated_df, 
    table_name="sector"
)



# 3. Write to Silver 
# query= (deduplicated_df.write 
#     .format("delta") 
#     .mode("overwrite") 
#     .save("s3a://lakehouse/silver/sector")
# )

#print("Taking out the old files in the silver layer...")

#Keep only the last 1 hours of deleted/old data
#spark.sql("VACUUM silver.currency` RETAIN 1 HOURS")