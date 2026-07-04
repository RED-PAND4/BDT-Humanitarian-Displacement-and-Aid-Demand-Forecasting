from pyspark.sql import SparkSession
import sys
import os


parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


from utilities import get_spark_session

spark = get_spark_session("BronzeToSilver_HDX_IDPs")

# 1. Read Bronze as a Stream
bronze_df = (spark.read
    .format("delta")
    #.option("inferSchema", "true")
    .load("s3a://lakehouse/bronze/idps"))

# 2. Clean the Data
# Drop rows where critical fields are null
cleaned_df = bronze_df.dropna(subset=["location_code", "location_name"])

# Drop duplicates based on a unique ID or code

deduplicated_df = cleaned_df.dropDuplicates([
    "location_code", 
    "assessment_type", 
    "reporting_round", 
    "operation", 
    "reference_period_start",
    "population"
])

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.idps
    USING delta
    LOCATION 's3a://lakehouse/silver/idps'
""")

# 3. Write to Silver 
query= (deduplicated_df.write 
    .format("delta") 
    .mode("overwrite") 
    .save("s3a://lakehouse/silver/idps")

)

#print("Taking out the trash in the Bronze layer...")
# spark.sql("VACUUM delta.`s3a://lakehouse/bronze/idps` RETAIN 24 HOURS")